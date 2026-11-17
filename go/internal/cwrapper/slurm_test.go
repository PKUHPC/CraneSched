// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cwrapper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/cacctmgr"
	"github.com/PKUHPC/CraneSched/go/internal/cbatch"
	"google.golang.org/grpc"
)

func TestNormalizeSacctmgrArgsAcceptsNameFilters(t *testing.T) {
	tests := []struct {
		args       []string
		wantArgs   []string
		wantFilter map[string]string
	}{
		{
			args:       []string{"show", "account", "Name=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "account", "name=acct1"},
			wantFilter: map[string]string{"name": "acct1"},
		},
		{
			args:       []string{"list", "user", "Accounts=acct1", "Names=user1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "account=acct1", "name=user1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
		{
			args:       []string{"show", "qos", "Name=normal"},
			wantArgs:   []string{"cacctmgr", "show", "qos", "name=normal"},
			wantFilter: map[string]string{"name": "normal"},
		},
		{
			args:       []string{"show", "user", "where", "Name=user1", "Account=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "where", "name=user1", "account=acct1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
	}

	for _, tt := range tests {
		got := normalizeSacctmgrArgs(tt.args)
		if !slices.Equal(got, tt.wantArgs) {
			t.Errorf("normalizeSacctmgrArgs(%v) = %v, want %v", tt.args, got, tt.wantArgs)
			continue
		}

		command, err := cacctmgr.ParseCAcctMgrCommand(strings.Join(got[1:], " "))
		if err != nil {
			t.Errorf("ParseCAcctMgrCommand(%v) returned error: %v", got[1:], err)
			continue
		}
		for key, want := range tt.wantFilter {
			if value := command.GetFilterValue(key); value != want {
				t.Errorf("%v filter %s = %q, want %q", tt.args, key, value, want)
			}
		}
	}
}

func TestConvertSqueueFormats(t *testing.T) {
	got, err := convertSqueueFormat("%a %C %D %.8i %20j %k %l %M %m %N %n %o %P %p %q %r %S %T %t %u %U %V %State %JobType")
	if err != nil {
		t.Fatalf("convertSqueueFormat() error = %v", err)
	}
	const want = "%a %C %N %.8j %20n %k %l %e %M %L %r %o %P %p %q %R %S %state %statecompact %u %U %s %State %JobType"
	if got != want {
		t.Fatalf("convertSqueueFormat() = %q, want %q", got, want)
	}

	if _, err := convertSqueueFormat("%K"); err == nil || !strings.Contains(err.Error(), "%K") {
		t.Fatalf("convertSqueueFormat(%%K) error = %v, want unsupported format error", err)
	}
}

func TestSqueueHelpMatchesSlurmFormatSemantics(t *testing.T) {
	cmd := squeue()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)

	cmd.Run(cmd, []string{"--help"})
	help := output.String()
	for _, expected := range []string{
		"Supported Slurm format identifiers:",
		"%D/%NodeNum",
		"%i/%JobID",
		"%j/%Name",
		"%M/%ElapsedTime",
		"%m/%ReqMemPerNode",
		"%N/%NodeList",
		"%n/%ReqNodes",
		"%r/%Reason",
		"%t/%StateCompact",
		"%T/%State",
		"%V/%SubmitTime",
		"Supported Crane extension identifiers (long form only):",
		"%StepId",
		"%Wckey",
		"%JobType",
	} {
		if !strings.Contains(help, expected) {
			t.Errorf("squeue --help does not contain %q", expected)
		}
	}
	for _, unexpected := range []string{
		"%c/%AllocCpus",
		"%e/%ElapsedTime",
		"%h/%Held",
		"%i/%StepId",
		"%j/%JobID",
		"%K/%Wckey",
		"%L/%NodeList",
		"%M/%ReqMemPerNode",
		"%m/%AllocMemPerNode",
		"%N/%NodeNum",
		"%n/%Name",
		"%Q/%ReqCpuPerNode",
		"%R/%Reason",
		"%r/%ReqNodes",
		"%s/%SubmitTime",
		"%T/%JobType",
		"%X/%Exclusive",
		"%x/%ExcludeNodes",
	} {
		if strings.Contains(help, unexpected) {
			t.Errorf("squeue --help contains unsupported or incorrect alias %q", unexpected)
		}
	}
}

func TestBatchWorkingDirectorySubmission(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recorder := &batchSubmissionRecorder{jobs: make(chan *protos.JobToCtld, 1)}
	server := grpc.NewServer()
	protos.RegisterCraneCtldServer(server, recorder)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve submission recorder: %v", err)
		}
	})
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := fmt.Sprintf("ControlMachine: %q\nCraneCtldListenPort: %q\nCraneCtldForInternalListenPort: %q\n", host, port, port)
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	submitDir := t.TempDir()
	scriptPath := filepath.Join(t.TempDir(), "job.sh")
	for _, commandName := range []string{"cbatch", "sbatch"} {
		for _, tt := range []struct {
			name      string
			flags     []string
			directive string
			want      string
		}{
			{name: "CLI long dot", flags: []string{"--chdir=."}, directive: "#SBATCH --chdir=script-results", want: submitDir},
			{name: "CLI short relative", flags: []string{"-D", "cli-results"}, want: filepath.Join(submitDir, "cli-results")},
			{name: "SBATCH long relative", directive: "#SBATCH --chdir=script-results", want: filepath.Join(submitDir, "script-results")},
			{name: "SBATCH short relative", directive: "#SBATCH -D script-results", want: filepath.Join(submitDir, "script-results")},
			{name: "CBATCH long relative", directive: "#CBATCH --chdir=script-results", want: filepath.Join(submitDir, "script-results")},
			{name: "CBATCH short relative", directive: "#CBATCH -D script-results", want: filepath.Join(submitDir, "script-results")},
			{name: "CLI overrides CBATCH", flags: []string{"--chdir=cli-results"}, directive: "#CBATCH --chdir=script-results", want: filepath.Join(submitDir, "cli-results")},
		} {
			t.Run(commandName+"/"+tt.name, func(t *testing.T) {
				if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\n"+tt.directive+"\npwd\n"), 0600); err != nil {
					t.Fatal(err)
				}
				// Each invocation needs fresh global Cobra flags, as in a real CLI process.
				args := []string{"-test.run=^TestBatchWorkingDirectoryCommandProcess$", "--", commandName, "--config", configPath}
				args = append(args, tt.flags...)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, executable, append(args, scriptPath)...)
				command.Dir = submitDir
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("%s failed: %v\n%s", commandName, err, output)
				}
				select {
				case job := <-recorder.jobs:
					if job.Cwd != tt.want || job.SubmitDir != submitDir {
						t.Errorf("%s submitted Cwd=%q SubmitDir=%q, want %q and %q", commandName, job.Cwd, job.SubmitDir, tt.want, submitDir)
					}
				default:
					t.Fatalf("%s did not submit a job\n%s", commandName, output)
				}
			})
		}
	}
}

func TestBatchWorkingDirectoryCommandProcess(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		return
	}
	args := os.Args[separator+1:]
	if len(args) == 0 {
		t.Fatal("missing batch command")
	}
	switch args[0] {
	case "cbatch":
		os.Args = args
		cbatch.ParseCmdArgs()
	case "sbatch":
		os.Args = append([]string{"cwrapper"}, args...)
		ParseCmdArgs()
	default:
		t.Fatalf("unexpected command %q", args[0])
	}
}

type batchSubmissionRecorder struct {
	protos.UnimplementedCraneCtldServer
	jobs chan *protos.JobToCtld
}

func (s *batchSubmissionRecorder) SubmitBatchJob(_ context.Context, req *protos.SubmitBatchJobRequest) (*protos.SubmitBatchJobReply, error) {
	s.jobs <- req.Job
	return &protos.SubmitBatchJobReply{
		Ok:      true,
		Payload: &protos.SubmitBatchJobReply_JobId{JobId: 1},
	}, nil
}
