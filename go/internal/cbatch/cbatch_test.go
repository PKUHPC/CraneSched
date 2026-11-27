package cbatch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/spf13/cobra"
)

func TestBuildCbatchJobSignalOptions(t *testing.T) {
	oldSignal := FlagSignal
	t.Cleanup(func() { FlagSignal = oldSignal })

	for _, tt := range []struct {
		name      string
		directive string
		flags     []string
		wantTimes []uint32
		wantError string
	}{
		{name: "omitted"},
		{name: "empty flag", flags: []string{"--signal="}, wantError: "--signal: empty signal"},
		{name: "empty CBATCH option", directive: "#CBATCH --signal=", wantError: "--signal in script: empty signal"},
		{name: "missing SBATCH value", directive: "#SBATCH --signal", wantError: "--signal in script: empty signal"},
		{name: "invalid CBATCH short option", directive: "#CBATCH -s 999@3", wantError: "-s in script: invalid signal number"},
		{name: "invalid SBATCH time", directive: "#SBATCH --signal=R:USR1@65536", wantError: "--signal in script: invalid signal time"},
		{name: "invalid script with valid flag", directive: "#CBATCH --signal=", flags: []string{"--signal=USR1"}, wantError: "--signal in script: empty signal"},
		{name: "script and flag combine", directive: "#SBATCH --signal=B:USR1@0", flags: []string{"--signal=R:64@65535"}, wantTimes: []uint32{0, 65535}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().StringVarP(&FlagSignal, "signal", "s", "", "")
			if err := cmd.ParseFlags(tt.flags); err != nil {
				t.Fatal(err)
			}
			script := filepath.Join(t.TempDir(), "job.sh")
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+tt.directive+"\ntrue\n"), 0600); err != nil {
				t.Fatal(err)
			}
			job, err := BuildCbatchJob(cmd, []string{script}, &util.Config{})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("got error %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(job.Signals) != len(tt.wantTimes) {
				t.Fatalf("got %d signals, want %d", len(job.Signals), len(tt.wantTimes))
			}
			for i, seconds := range tt.wantTimes {
				if job.Signals[i].SignalTime != seconds {
					t.Errorf("signal %d time = %d, want %d", i, job.Signals[i].SignalTime, seconds)
				}
			}
		})
	}
}

func TestBuildCbatchJobResolvesWorkingDirectory(t *testing.T) {
	oldCwd, oldWrap := FlagCwd, FlagWrappedScript
	t.Cleanup(func() {
		FlagCwd, FlagWrappedScript = oldCwd, oldWrap
	})
	submitDir := t.TempDir()
	t.Chdir(submitDir)
	script := filepath.Join(t.TempDir(), "job.sh")
	absoluteDir := t.TempDir()

	for _, tt := range []struct {
		name      string
		directive string
		args      []string
		wrap      string
		want      string
	}{
		{name: "default", want: submitDir},
		{name: "SBATCH dot", directive: "#SBATCH --chdir=.", want: submitDir},
		{name: "SBATCH relative", directive: "#SBATCH --chdir=results", want: filepath.Join(submitDir, "results")},
		{name: "SBATCH parent", directive: "#SBATCH --chdir=../results", want: filepath.Join(filepath.Dir(submitDir), "results")},
		{name: "SBATCH absolute", directive: "#SBATCH --chdir=" + absoluteDir, want: absoluteDir},
		{name: "CBATCH dot", directive: "#CBATCH --chdir=.", want: submitDir},
		{name: "CLI dot", args: []string{"--chdir=."}, want: submitDir},
		{name: "CLI relative overrides script", directive: "#SBATCH --chdir=" + absoluteDir, args: []string{"-D", "results"}, want: filepath.Join(submitDir, "results")},
		{name: "CLI absolute overrides script", directive: "#SBATCH --chdir=.", args: []string{"--chdir=" + absoluteDir}, want: absoluteDir},
		{name: "wrapped script", args: []string{"-D", "."}, wrap: "pwd", want: submitDir},
	} {
		t.Run(tt.name, func(t *testing.T) {
			FlagWrappedScript = tt.wrap
			cmd := &cobra.Command{}
			cmd.Flags().StringVarP(&FlagCwd, "chdir", "D", "", "")
			if err := cmd.ParseFlags(tt.args); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+tt.directive+"\npwd\n"), 0600); err != nil {
				t.Fatal(err)
			}
			job, err := BuildCbatchJob(cmd, []string{script}, &util.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if job.Cwd != tt.want {
				t.Errorf("working directory = %q, want %q", job.Cwd, tt.want)
			}
		})
	}
}

func TestBuildCbatchJobRequeuePolicy(t *testing.T) {
	oldRequeue, oldNoRequeue := FlagRequeue, FlagNoRequeue
	t.Cleanup(func() { FlagRequeue, FlagNoRequeue = oldRequeue, oldNoRequeue })
	for _, tc := range []struct {
		name, directive, flag, policy string
		onFailure, disabled           bool
	}{
		{name: "default", policy: "REQUEUE_DEFAULT"},
		{name: "explicit false", flag: "--requeue=false", policy: "REQUEUE_ON_FAILURE_FALSE"},
		{name: "on failure", flag: "--requeue", policy: "REQUEUE_ON_FAILURE", onFailure: true},
		{name: "disabled", flag: "--no-requeue", policy: "REQUEUE_DISABLED", disabled: true},
		{name: "directive", directive: "#CBATCH --requeue=false", policy: "REQUEUE_ON_FAILURE_FALSE"},
		{name: "override", directive: "#CBATCH --no-requeue", flag: "--requeue", policy: "REQUEUE_ON_FAILURE", onFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().BoolVar(&FlagRequeue, "requeue", false, "")
			cmd.Flags().BoolVar(&FlagNoRequeue, "no-requeue", false, "")
			if tc.flag != "" {
				if err := cmd.ParseFlags([]string{tc.flag}); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), "job.sh")
			if err := os.WriteFile(path, []byte("#!/bin/sh\n"+tc.directive+"\ntrue\n"), 0600); err != nil {
				t.Fatal(err)
			}
			job, err := BuildCbatchJob(cmd, []string{path}, &util.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if job.GetRequeuePolicy().String() != tc.policy || job.GetRequeueIfFailed() != tc.onFailure || job.GetNoRequeue() != tc.disabled {
				t.Fatalf("policy=%s requeue=%t no-requeue=%t", job.GetRequeuePolicy(), job.GetRequeueIfFailed(), job.GetNoRequeue())
			}
		})
	}
}
