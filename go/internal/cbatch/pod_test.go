package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestBuildPodMetaAppliesContainerIdentityPolicy(t *testing.T) {
	tests := []struct {
		name    string
		uid     uint32
		gids    []uint32
		user    string
		userns  bool
		wantUID uint32
		wantGID uint32
		wantErr string
	}{
		{"userns defaults to root", 1000, []uint32{1000}, "", true, 0, 0, ""},
		{"userns explicit root", 1000, []uint32{1000}, "0:0", true, 0, 0, ""},
		{"userns root UID only", 1000, []uint32{1000}, "0", true, 0, 0, ""},
		{"userns effective group differs from UID", 1000, []uint32{2000}, "", true, 0, 0, ""},
		{"userns duplicate effective group", 1000, []uint32{1000, 1000}, "", true, 0, 0, ""},
		{"userns supplementary groups reduced", 1000, []uint32{1000, 2000, 2001}, "", true, 0, 0, ""},
		{"userns explicit root reduces supplementary groups", 1000, []uint32{1000, 2000}, "0:0", true, 0, 0, ""},
		{"userns nonzero UID", 1000, []uint32{1000}, "42:0", true, 42, 0, ""},
		{"userns nonzero GID", 1000, []uint32{1000}, "0:42", true, 0, 42, ""},
		{"userns UID only keeps zero GID", 1000, []uint32{2000}, "123", true, 123, 0, ""},
		{"userns explicit identity", 1000, []uint32{2000}, "123:456", true, 123, 456, ""},
		{"userns IDs independent of host NSS", 1000, []uint32{2000}, "42420:42421", true, 42420, 42421, ""},
		{"root may select a userns identity", 0, []uint32{0}, "123:456", true, 123, 456, ""},
		{"userns nonroot reduces supplementary groups", 1000, []uint32{2000, 2001}, "123:456", true, 123, 456, ""},
		{"host defaults preserve groups", 1000, []uint32{1000, 2000, 2001}, "", false, 1000, 1000, ""},
		{"host effective group differs from UID", 1000, []uint32{2000, 2001}, "", false, 1000, 2000, ""},
		{"host UID only keeps effective group", 1000, []uint32{2000, 2001}, "1000", false, 1000, 2000, ""},
		{"host explicit identity", 1000, []uint32{1000, 2000}, "1000:1000", false, 1000, 1000, ""},
		{"host other UID rejected", 1000, []uint32{1000}, "0:1000", false, 0, 0, "submitter"},
		{"host supplementary GID cannot replace effective group", 1000, []uint32{1000, 2000}, "1000:2000", false, 0, 0, "submitter"},
		{"root must use own host identity", 0, []uint32{0}, "1000:1000", false, 0, 0, "submitter"},
		{"empty userns groups", 1000, nil, "", true, 0, 0, "empty"},
		{"empty host groups", 1000, nil, "", false, 0, 0, "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &protos.JobToCtld{Uid: tt.uid, Gids: append([]uint32(nil), tt.gids...)}
			meta, err := buildPodMeta(job, &podOptions{user: tt.user, userns: tt.userns})
			wantGids := tt.gids
			if tt.userns && len(wantGids) > 1 {
				wantGids = wantGids[:1]
			}
			if job.Uid != tt.uid || !reflect.DeepEqual(job.Gids, wantGids) {
				t.Fatalf("execution identity = %d:%v, want %d:%v", job.Uid, job.Gids, tt.uid, wantGids)
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if meta.RunAsUser != tt.wantUID || meta.RunAsGroup != tt.wantGID {
				t.Fatalf("container identity = %d:%d, want %d:%d", meta.RunAsUser, meta.RunAsGroup, tt.wantUID, tt.wantGID)
			}
		})
	}
}

func TestBuildCbatchJobSetsHostIdentityBeforePodValidation(t *testing.T) {
	oldPod, oldUserns, oldWrap, oldName := FlagPod, FlagPodUserns, FlagWrappedScript, FlagJob
	t.Cleanup(func() {
		FlagPod, FlagPodUserns, FlagWrappedScript, FlagJob = oldPod, oldUserns, oldWrap, oldName
	})
	FlagPod, FlagPodUserns = true, false
	FlagWrappedScript, FlagJob = "true", "identity-test"
	cmd := &cobra.Command{}
	cmd.Flags().Bool("pod-userns", true, "")
	cmd.Flags().String("job-name", "", "")
	if err := cmd.ParseFlags([]string{"--pod-userns=false", "--job-name=identity-test"}); err != nil {
		t.Fatal(err)
	}
	config := &util.Config{}
	config.Container.UserNsEnabledByDefault = true
	job, err := BuildCbatchJob(cmd, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	wantGroups, err := util.CollectEffectiveGroups()
	if err != nil {
		t.Fatal(err)
	}
	if job.Uid != uint32(os.Getuid()) || !reflect.DeepEqual(job.Gids, wantGroups) {
		t.Fatalf("host identity = %d:%v, want %d:%v", job.Uid, job.Gids, os.Getuid(), wantGroups)
	}
	if job.PodMeta.RunAsUser != job.Uid || job.PodMeta.RunAsGroup != wantGroups[0] {
		t.Fatalf("Pod identity differs from submitter: %v", job.PodMeta)
	}
	if job.PodMeta.Name != FlagJob {
		t.Fatalf("Pod name = %q, want %q", job.PodMeta.Name, FlagJob)
	}
}

func TestBuildCbatchJobUsernsGroupPolicy(t *testing.T) {
	oldPod, oldUserns, oldWrap := FlagPod, FlagPodUserns, FlagWrappedScript
	t.Cleanup(func() {
		FlagPod, FlagPodUserns, FlagWrappedScript = oldPod, oldUserns, oldWrap
	})
	FlagPod, FlagPodUserns, FlagWrappedScript = true, true, "true"
	cmd := &cobra.Command{}
	cmd.Flags().Bool("pod-userns", false, "")
	if err := cmd.ParseFlags([]string{"--pod-userns=true"}); err != nil {
		t.Fatal(err)
	}
	groups, err := util.CollectEffectiveGroups()
	if err != nil {
		t.Fatal(err)
	}
	job, err := BuildCbatchJob(cmd, nil, &util.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if job.PodMeta.RunAsUser != 0 || job.PodMeta.RunAsGroup != 0 ||
		len(job.Gids) != 1 || job.Gids[0] != groups[0] {
		t.Fatalf("incorrect fakeroot identity: %v", job)
	}
}

func TestBuildCbatchJobUsernsRunAsOptions(t *testing.T) {
	oldPod, oldUserns, oldUser, oldWrap := FlagPod, FlagPodUserns, FlagPodUser, FlagWrappedScript
	t.Cleanup(func() {
		FlagPod, FlagPodUserns, FlagPodUser, FlagWrappedScript = oldPod, oldUserns, oldUser, oldWrap
	})
	groups, err := util.CollectEffectiveGroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		script  string
		args    []string
		wantUID uint32
		wantGID uint32
	}{
		{"script identity", "#CBATCH --pod-userns=true\n#CBATCH --pod-user=123:456\n", nil, 123, 456},
		{"CLI UID overrides script pair", "#CBATCH --pod-userns=false\n#CBATCH --pod-user=0:456\n", []string{"--pod-userns=true", "--pod-user=123"}, 123, 0},
		{"CLI pair overrides script", "#CBATCH --pod-userns=false\n#CBATCH --pod-user=0:0\n", []string{"--pod-userns=true", "--pod-user=42420:42421"}, 42420, 42421},
	} {
		t.Run(tt.name, func(t *testing.T) {
			FlagPod, FlagWrappedScript = false, ""
			cmd := &cobra.Command{}
			cmd.Flags().BoolVar(&FlagPodUserns, "pod-userns", false, "")
			cmd.Flags().StringVar(&FlagPodUser, "pod-user", "", "")
			if err := cmd.ParseFlags(tt.args); err != nil {
				t.Fatal(err)
			}
			script := filepath.Join(t.TempDir(), "job.sh")
			if err := os.WriteFile(script, []byte("#CBATCH --pod\n"+tt.script+"true\n"), 0600); err != nil {
				t.Fatal(err)
			}
			job, err := BuildCbatchJob(cmd, []string{script}, &util.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if job.Uid != uint32(os.Getuid()) || len(job.Gids) != 1 ||
				job.Gids[0] != groups[0] {
				t.Fatalf("execution identity changed: %d:%v", job.Uid, job.Gids)
			}
			if !job.PodMeta.Userns || job.PodMeta.RunAsUser != tt.wantUID || job.PodMeta.RunAsGroup != tt.wantGID {
				t.Fatalf("Pod identity = %v, want userns %d:%d", job.PodMeta, tt.wantUID, tt.wantGID)
			}
		})
	}
}
