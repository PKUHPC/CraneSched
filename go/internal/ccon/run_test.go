package ccon

import (
	"CraneFrontEnd/generated/protos"
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
			meta, err := buildPodMeta(nil, &Flags{Run: RunFlags{Network: "default", User: tt.user, UserNS: tt.userns}}, job)
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

func TestContainerStepCannotOverrideParentIdentity(t *testing.T) {
	for _, args := range [][]string{
		{"--user=123:456"},
		{"--userns=true"},
		{"--userns=false"},
		{"--userns=true", "--user=123:456"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			flags := &Flags{}
			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&flags.Run.User, "user", "", "")
			cmd.Flags().BoolVar(&flags.Run.UserNS, "userns", false, "")
			if err := cmd.ParseFlags(args); err != nil {
				t.Fatal(err)
			}
			_, err := buildContainerStep(cmd, flags, 1, "example.invalid/test:1", nil)
			if err == nil || !strings.Contains(err.Error(), "pod configuration is inherited from the job") {
				t.Fatalf("error = %v, want inherited Pod configuration", err)
			}
		})
	}
}
