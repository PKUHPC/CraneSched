package cwrapper

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/cbatch"
	"google.golang.org/grpc"
)

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
