package cgroup

import (
	"fmt"

	"CraneFrontEnd/plugin/monitor/pkg/types"
)

type CgroupReader interface {
	GetCgroupStats() (types.CgroupStats, error)
}

type Version int

const (
	V1 Version = iota + 1
	V2
)

func NewCgroupReader(version Version, cgroupName string) (CgroupReader, error) {
	switch version {
	case V1:
		reader := NewV1Reader(cgroupName)
		if reader == nil {
			return nil, fmt.Errorf("failed to create v1 reader for cgroup: %s", cgroupName)
		}
		return reader, nil
	case V2:
		return nil, fmt.Errorf("cgroup v2 not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported cgroup version: %d", version)
	}
}
