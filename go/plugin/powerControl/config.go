package main

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	PowerControl struct {
		PredictorScript               string  `yaml:"PredictorScript"`
		EnableSleep                   bool    `yaml:"EnableSleep"`
		SleepTimeThresholdSeconds     int     `yaml:"SleepTimeThresholdSeconds"`
		IdleReserveRatio              float64 `yaml:"IdleReserveRatio"`
		CheckIntervalSeconds          int     `yaml:"CheckIntervalSeconds"`
		NodeStateCheckIntervalSeconds int     `yaml:"NodeStateCheckIntervalSeconds"`
		PowerControlLogFile           string  `yaml:"PowerControlLogFile"`
		NodeStateChangeFile           string  `yaml:"NodeStateChangeFile"`
		ClusterStateFile              string  `yaml:"ClusterStateFile"`
	} `yaml:"PowerControl"`

	Predictor struct {
		Debug            bool   `yaml:"Debug"`
		URL              string `yaml:"URL"`
		CheckpointFile   string `yaml:"CheckpointFile"`
		ScalersFile      string `yaml:"ScalersFile"`
		PredictorLogFile string `yaml:"PredictorLogFile"`
		ForecastMinutes  int    `yaml:"ForecastMinutes"`
		LookbackMinutes  int    `yaml:"LookbackMinutes"`
	} `yaml:"Predictor"`

	InfluxDB struct {
		URL    string `yaml:"URL"`
		Token  string `yaml:"Token"`
		Org    string `yaml:"Org"`
		Bucket string `yaml:"Bucket"`
	} `yaml:"InfluxDB"`

	IPMI struct {
		User                 string            `yaml:"User"`
		Password             string            `yaml:"Password"`
		MaxNodesPerBatch     int               `yaml:"MaxNodesPerBatch"`
		BatchIntervalSeconds int               `yaml:"BatchIntervalSeconds"`
		ExcludeNodes         []string          `yaml:"ExcludeNodes"`
		NodeBMCMapping       map[string]string `yaml:"NodeBMCMapping"`
	} `yaml:"IPMI"`

	SSH struct {
		User     string `yaml:"User"`
		Password string `yaml:"Password"`
	} `yaml:"SSH"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	var expandedNodes []string
	for _, nodeExpr := range config.IPMI.ExcludeNodes {
		nodes, ok := util.ParseHostList(nodeExpr)
		if !ok {
			return nil, fmt.Errorf("invalid node expression in ExcludeNodes: %s", nodeExpr)
		}
		expandedNodes = append(expandedNodes, nodes...)
	}
	config.IPMI.ExcludeNodes = expandedNodes

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %v", err)
	}

	PrintConfig(config)

	return config, nil
}

func PrintConfig(cfg *Config) {
	log.Info("Power Control Plugin Configuration:")
	log.Info("----------------------------------------")
	log.Info("PowerControl:")
	log.Infof("  PredictorScript: %s", cfg.PowerControl.PredictorScript)
	log.Infof("  EnableSleep: %t", cfg.PowerControl.EnableSleep)
	log.Infof("  SleepTimeThresholdSeconds: %d", cfg.PowerControl.SleepTimeThresholdSeconds)
	log.Infof("  IdleReserveRatio: %.2f", cfg.PowerControl.IdleReserveRatio)
	log.Infof("  CheckIntervalSeconds: %d", cfg.PowerControl.CheckIntervalSeconds)
	log.Infof("  NodeStateCheckIntervalSeconds: %d", cfg.PowerControl.NodeStateCheckIntervalSeconds)
	log.Infof("  PowerControlLogFile: %s", cfg.PowerControl.PowerControlLogFile)
	log.Infof("  NodeStateChangeFile: %s", cfg.PowerControl.NodeStateChangeFile)
	log.Infof("  ClusterStateFile: %s", cfg.PowerControl.ClusterStateFile)

	log.Info("Predictor:")
	log.Infof("  Debug: %t", cfg.Predictor.Debug)
	log.Infof("  URL: %s", cfg.Predictor.URL)
	log.Infof("  CheckpointFile: %s", cfg.Predictor.CheckpointFile)
	log.Infof("  ScalersFile: %s", cfg.Predictor.ScalersFile)
	log.Infof("  PredictorLogFile: %s", cfg.Predictor.PredictorLogFile)
	log.Infof("  ForecastMinutes: %d", cfg.Predictor.ForecastMinutes)
	log.Infof("  LookbackMinutes: %d", cfg.Predictor.LookbackMinutes)

	log.Info("IPMI:")
	log.Infof("  User: %s", cfg.IPMI.User)
	log.Info("  Password: ********")
	log.Infof("  ExcludeNodes: %v", cfg.IPMI.ExcludeNodes)
	log.Infof("  NodeBMCMapping: %v", cfg.IPMI.NodeBMCMapping)
	log.Info("SSH:")
	log.Infof("  User: %s", cfg.SSH.User)
	log.Info("  Password: ********")
}

func validateConfig(config *Config) error {
	if config.PowerControl.PredictorScript == "" {
		return fmt.Errorf("PowerControl.PredictorScript cannot be empty")
	}
	if config.PowerControl.EnableSleep {
		if config.PowerControl.SleepTimeThresholdSeconds <= 0 {
			return fmt.Errorf("Predictor.SleepTimeThresholdSeconds must be positive")
		}
	}
	if config.PowerControl.IdleReserveRatio < 0 || config.PowerControl.IdleReserveRatio > 1 {
		return fmt.Errorf("Predictor.IdleReserveRatio must be between 0 and 1")
	}
	if config.PowerControl.CheckIntervalSeconds <= 0 {
		return fmt.Errorf("Predictor.CheckIntervalSeconds must be positive")
	}
	if config.PowerControl.NodeStateCheckIntervalSeconds <= 0 {
		return fmt.Errorf("Predictor.NodeStateCheckIntervalSeconds must be positive")
	}
	if config.PowerControl.PowerControlLogFile == "" {
		return fmt.Errorf("PowerControl.PowerControlLogFile cannot be empty")
	}
	if config.PowerControl.ClusterStateFile == "" {
		return fmt.Errorf("PowerControl.ClusterStateFile cannot be empty")
	}

	if config.Predictor.URL == "" {
		return fmt.Errorf("Predictor.URL cannot be empty")
	}
	if config.Predictor.CheckpointFile == "" {
		return fmt.Errorf("Predictor.CheckpointFile cannot be empty")
	}
	if config.Predictor.ScalersFile == "" {
		return fmt.Errorf("Predictor.ScalersFile cannot be empty")
	}
	if config.Predictor.PredictorLogFile == "" {
		return fmt.Errorf("Predictor.PredictorLogFile cannot be empty")
	}
	if config.Predictor.ForecastMinutes <= 0 {
		return fmt.Errorf("Predictor.ForecastMinutes must be positive")
	}
	if config.Predictor.LookbackMinutes <= 0 {
		return fmt.Errorf("Predictor.LookbackMinutes must be positive")
	}

	if config.InfluxDB.URL == "" {
		return fmt.Errorf("InfluxDB.URL cannot be empty")
	}
	if config.InfluxDB.Token == "" {
		return fmt.Errorf("InfluxDB.Token cannot be empty")
	}
	if config.InfluxDB.Org == "" {
		return fmt.Errorf("InfluxDB.Org cannot be empty")
	}
	if config.InfluxDB.Bucket == "" {
		return fmt.Errorf("InfluxDB.Bucket cannot be empty")
	}

	if config.IPMI.User == "" {
		return fmt.Errorf("IPMI.User cannot be empty")
	}
	if config.IPMI.Password == "" {
		return fmt.Errorf("IPMI.Password cannot be empty")
	}
	if config.IPMI.MaxNodesPerBatch <= 0 {
		return fmt.Errorf("IPMI.MaxNodesPerBatch must be positive")
	}
	if config.IPMI.BatchIntervalSeconds <= 0 {
		return fmt.Errorf("IPMI.BatchIntervalSeconds must be positive")
	}
	if len(config.IPMI.NodeBMCMapping) == 0 {
		return fmt.Errorf("IPMI.NodeBMCMapping cannot be empty")
	}

	if config.SSH.User == "" {
		return fmt.Errorf("SSH.User cannot be empty")
	}
	if config.SSH.Password == "" {
		return fmt.Errorf("SSH.Password cannot be empty")
	}

	return nil
}
