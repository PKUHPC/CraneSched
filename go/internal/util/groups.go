package util

import (
	"fmt"
	"os/user"
	"strconv"
	"syscall"
)

const maxExecutionGroups = 256

func normalizeExecutionGroups(egid int, groups []int) ([]uint32, error) {
	if egid < 0 {
		return nil, fmt.Errorf("failed to get effective group: %d", egid)
	}
	if len(groups) >= maxExecutionGroups {
		return nil, fmt.Errorf("group list exceeds maximum size of %d", maxExecutionGroups)
	}

	result := make([]uint32, 0, len(groups)+1)
	seen := make(map[uint32]struct{}, len(groups)+1)
	appendGroup := func(group int) {
		gid := uint32(group)
		if _, exists := seen[gid]; exists {
			return
		}
		seen[gid] = struct{}{}
		result = append(result, gid)
	}

	appendGroup(egid)
	for _, group := range groups {
		if group < 0 {
			return nil, fmt.Errorf("invalid supplementary group: %d", group)
		}
		appendGroup(group)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("effective group list is empty")
	}
	return result, nil
}

// CollectEffectiveGroups returns the caller's effective group followed by its
// supplementary groups.  The effective group is authoritative for the first
// protocol element; supplementary groups are deduplicated while preserving
// the kernel-provided order.
func CollectEffectiveGroups() ([]uint32, error) {
	egid := syscall.Getegid()
	groups, err := syscall.Getgroups()
	if err != nil {
		return nil, fmt.Errorf("failed to get supplementary groups: %w", err)
	}
	return normalizeExecutionGroups(egid, groups)
}

// CollectGroupsForUser resolves the requested user's supplementary groups
// through NSS. The requested primary GID is always emitted first; the
// backend performs the final node-local authorization and intersection.
func CollectGroupsForUser(uid, primary uint32) ([]uint32, error) {
	target, err := lookupUser(uid)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve target user %d: %w", uid, err)
	}
	return collectGroupsForLookup(target, uid, primary)
}

// CollectDefaultGroupsForUser resolves a user's passwd primary group and
// returns it before the supplementary groups. It is used when a container
// selects a target UID without an explicit GID.
func CollectDefaultGroupsForUser(uid uint32) ([]uint32, error) {
	target, err := lookupUser(uid)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve target user %d: %w", uid, err)
	}
	primary, err := strconv.ParseUint(target.Gid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid primary NSS group %q for user %d: %w", target.Gid, uid, err)
	}
	return collectGroupsForLookup(target, uid, uint32(primary))
}

func lookupUser(uid uint32) (*user.User, error) {
	return user.LookupId(strconv.FormatUint(uint64(uid), 10))
}

func collectGroupsForLookup(target *user.User, uid, primary uint32) ([]uint32, error) {
	groupIDs, err := target.GroupIds()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve groups for user %d: %w", uid, err)
	}
	groups := make([]int, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		gid, err := strconv.ParseUint(groupID, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid NSS group %q for user %d: %w", groupID, uid, err)
		}
		groups = append(groups, int(gid))
	}
	return normalizeExecutionGroups(int(primary), groups)
}
