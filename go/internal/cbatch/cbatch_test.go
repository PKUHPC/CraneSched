package cbatch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/spf13/cobra"
)

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
