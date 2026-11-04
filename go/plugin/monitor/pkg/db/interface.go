package db

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveJobEnergy(*types.JobData) error
	SaveNodeEvents([]*protos.CranedEventInfo) error
	SaveLicenseUsage([]*protos.LicenseInfo) error
	Close() error
}
