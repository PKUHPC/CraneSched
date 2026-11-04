package db

import (
	"github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/plugin/monitor/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveJobEnergy(*types.JobData) error
	SaveNodeEvents([]*protos.CranedEventInfo) error
	SaveLicenseUsage([]*protos.LicenseInfo) error
	Close() error
}
