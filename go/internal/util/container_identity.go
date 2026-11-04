package util

import (
	"CraneFrontEnd/generated/protos"
	"fmt"
)

// ValidateContainerIdentity checks container IDs against the submitting process's
// host identity. gids must contain the effective GID first.
func ValidateContainerIdentity(uid uint32, gids []uint32, pod *protos.PodJobAdditionalMeta) error {
	if len(gids) == 0 {
		return fmt.Errorf("effective group list is empty")
	}
	if pod.Userns {
		for _, gid := range gids[1:] {
			if gid != gids[0] {
				return fmt.Errorf("userns containers do not support supplementary groups; submit without extra groups or disable userns")
			}
		}
		// The execution node checks container IDs against its SubID mappings.
	} else if pod.RunAsUser != uid || pod.RunAsGroup != gids[0] {
		return fmt.Errorf("without userns, container UID/GID must match the submitter's UID/effective GID")
	}
	return nil
}
