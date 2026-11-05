package db

import (
	"sync"

	"github.com/PKUHPC/CraneSched/go/plugin/monitor/pkg/config"
)

var (
	instance DBInterface
	once     sync.Once
)

func GetInstance() DBInterface {
	return instance
}

func InitDB(config *config.Config) error {
	var err error
	once.Do(func() {
		switch config.DB.Type {
		case "influxdb":
			instance, err = NewInfluxDB(config)
		default:
			instance, err = NewInfluxDB(config)
		}
	})
	return err
}
