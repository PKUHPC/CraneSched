package util

import (
	"os/user"
	"strconv"
	"testing"
)

func TestNormalizeExecutionGroups(t *testing.T) {
	got, err := normalizeExecutionGroups(100, []int{200, 100, 300, 200})
	if err != nil {
		t.Fatalf("normalizeExecutionGroups() error = %v", err)
	}
	want := []uint32{100, 200, 300}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestNormalizeExecutionGroupsRejectsInvalidInput(t *testing.T) {
	if _, err := normalizeExecutionGroups(100, []int{-1}); err == nil {
		t.Fatal("expected a negative supplementary GID to be rejected")
	}
	tooMany := make([]int, maxExecutionGroups)
	if _, err := normalizeExecutionGroups(100, tooMany); err == nil {
		t.Fatal("expected an oversized group list to be rejected")
	}
}

func TestCollectGroupsForUserUsesRequestedPrimary(t *testing.T) {
	current, err := user.Current()
	if err != nil {
		t.Skipf("current user is unavailable: %v", err)
	}
	uid, err := strconv.ParseUint(current.Uid, 10, 32)
	if err != nil {
		t.Fatalf("invalid current UID %q: %v", current.Uid, err)
	}
	gid, err := strconv.ParseUint(current.Gid, 10, 32)
	if err != nil {
		t.Fatalf("invalid current GID %q: %v", current.Gid, err)
	}

	groups, err := CollectGroupsForUser(uint32(uid), uint32(gid))
	if err != nil {
		t.Fatalf("CollectGroupsForUser() error = %v", err)
	}
	if len(groups) == 0 || groups[0] != uint32(gid) {
		t.Fatalf("CollectGroupsForUser() = %v, primary GID %d is not first", groups, gid)
	}
}

func TestCollectGroupsForUserRejectsUnknownUser(t *testing.T) {
	if _, err := CollectGroupsForUser(^uint32(0), 0); err == nil {
		t.Fatal("expected an unknown target UID to be rejected")
	}
}
