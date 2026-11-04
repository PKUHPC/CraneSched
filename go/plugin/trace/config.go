package main

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	defaultTraceLogPath        = "/var/log/crane/trace.log"
	defaultTraceBucket         = "crane_trace"
	defaultTraceWriterShards   = 4
	defaultTraceBatchSpans     = 1024
	defaultTraceQueueBatches   = 8192
	defaultTraceFlushMs        = 50
	defaultTraceRetryBackoffMs = 200
	defaultTraceMaxBackoffMs   = 5000
)

type Config struct {
	Tracing TracingConfig `mapstructure:"Tracing"`
	DB      DBConfig      `mapstructure:"Database"`
}

type TracingConfig struct {
	LogPath string `mapstructure:"LogPath"`
}

type DBConfig struct {
	Type        string            `mapstructure:"Type"`
	InfluxDB    *InfluxDBConfig   `mapstructure:"Influxdb"`
	TraceWriter TraceWriterConfig `mapstructure:"TraceWriter"`
}

type InfluxDBConfig struct {
	URL               string   `mapstructure:"Url"`
	Token             string   `mapstructure:"Token"`
	Org               string   `mapstructure:"Org"`
	TraceBucket       string   `mapstructure:"TraceBucket"`
	TraceCoreBucket   string   `mapstructure:"TraceCoreBucket"`
	TraceDetailBucket string   `mapstructure:"TraceDetailBucket"`
	TraceErrorBucket  string   `mapstructure:"TraceErrorBucket"`
	TraceShardBuckets []string `mapstructure:"TraceShardBuckets"`
}

type TraceWriterConfig struct {
	Shards            int `mapstructure:"Shards"`
	BatchSpans        int `mapstructure:"BatchSpans"`
	QueueBatches      int `mapstructure:"QueueBatches"`
	FlushIntervalMs   int `mapstructure:"FlushIntervalMs"`
	RetryBackoffMs    int `mapstructure:"RetryBackoffMs"`
	MaxRetryBackoffMs int `mapstructure:"MaxRetryBackoffMs"`
}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg.Tracing.LogPath == "" {
		cfg.Tracing.LogPath = defaultTraceLogPath
	}

	if cfg.DB.Type == "" {
		cfg.DB.Type = "influxdb"
	}

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB == nil {
			return fmt.Errorf("influxdb configuration is required when type is influxdb")
		}
		if cfg.DB.InfluxDB.URL == "" || cfg.DB.InfluxDB.Token == "" || cfg.DB.InfluxDB.Org == "" {
			return fmt.Errorf("incomplete influxdb configuration")
		}
		if cfg.DB.InfluxDB.TraceBucket == "" {
			cfg.DB.InfluxDB.TraceBucket = defaultTraceBucket
		}
		if cfg.DB.InfluxDB.TraceCoreBucket == "" {
			cfg.DB.InfluxDB.TraceCoreBucket = cfg.DB.InfluxDB.TraceBucket
		}
		if cfg.DB.InfluxDB.TraceDetailBucket == "" {
			cfg.DB.InfluxDB.TraceDetailBucket = cfg.DB.InfluxDB.TraceBucket
		}
		if cfg.DB.InfluxDB.TraceErrorBucket == "" {
			cfg.DB.InfluxDB.TraceErrorBucket = cfg.DB.InfluxDB.TraceBucket
		}
	default:
		return fmt.Errorf("unsupported database type: %s", cfg.DB.Type)
	}

	normalizeTraceWriterConfig(&cfg.DB.TraceWriter)
	return nil
}

func normalizeTraceWriterConfig(cfg *TraceWriterConfig) {
	if cfg.Shards <= 0 {
		cfg.Shards = defaultTraceWriterShards
	}
	if cfg.BatchSpans <= 0 {
		cfg.BatchSpans = defaultTraceBatchSpans
	}
	if cfg.QueueBatches <= 0 {
		cfg.QueueBatches = defaultTraceQueueBatches
	}
	if cfg.FlushIntervalMs <= 0 {
		cfg.FlushIntervalMs = defaultTraceFlushMs
	}
	if cfg.RetryBackoffMs <= 0 {
		cfg.RetryBackoffMs = defaultTraceRetryBackoffMs
	}
	if cfg.MaxRetryBackoffMs <= 0 {
		cfg.MaxRetryBackoffMs = defaultTraceMaxBackoffMs
	}
	if cfg.MaxRetryBackoffMs < cfg.RetryBackoffMs {
		cfg.MaxRetryBackoffMs = cfg.RetryBackoffMs
	}
}

func PrintConfig(cfg *Config) {
	log.Infof("=== Current Trace Configuration Start ===")
	log.Infof("Tracing Configuration:")
	log.Infof("  Log Path: %v", cfg.Tracing.LogPath)
	log.Infof("Database Configuration:")
	log.Infof("  Type: %s", cfg.DB.Type)
	if cfg.DB.InfluxDB != nil {
		log.Infof("  InfluxDB Settings:")
		log.Infof("    URL: %s", cfg.DB.InfluxDB.URL)
		log.Infof("    Organization: %s", cfg.DB.InfluxDB.Org)
		log.Infof("    Trace Bucket: %s", cfg.DB.InfluxDB.TraceBucket)
		log.Infof("    Trace Core Bucket: %s", cfg.DB.InfluxDB.TraceCoreBucket)
		log.Infof("    Trace Detail Bucket: %s", cfg.DB.InfluxDB.TraceDetailBucket)
		log.Infof("    Trace Error Bucket: %s", cfg.DB.InfluxDB.TraceErrorBucket)
		log.Infof("    Trace Shard Buckets: %v", cfg.DB.InfluxDB.TraceShardBuckets)
		if cfg.DB.InfluxDB.Token != "" {
			tokenPreview := cfg.DB.InfluxDB.Token
			if len(tokenPreview) > 10 {
				tokenPreview = tokenPreview[:10] + "..."
			}
			log.Infof("    Token: %s", tokenPreview)
		}
	}
	log.Infof("  TraceWriter: shards=%d batch_spans=%d queue_batches=%d flush_interval_ms=%d retry_backoff_ms=%d max_retry_backoff_ms=%d",
		cfg.DB.TraceWriter.Shards, cfg.DB.TraceWriter.BatchSpans,
		cfg.DB.TraceWriter.QueueBatches, cfg.DB.TraceWriter.FlushIntervalMs,
		cfg.DB.TraceWriter.RetryBackoffMs, cfg.DB.TraceWriter.MaxRetryBackoffMs)
	log.Infof("=== Current Trace Configuration End ===")
}
