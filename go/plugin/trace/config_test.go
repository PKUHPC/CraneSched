package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTraceConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "trace.yaml")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfigRequiresInfluxDBConnectionFields(t *testing.T) {
	path := writeTraceConfig(t, `
Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://localhost:8086"
    Token: ""
    Org: "org"
`)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("LoadConfig succeeded, want missing token error")
	}
	if !strings.Contains(err.Error(), "incomplete influxdb configuration") {
		t.Fatalf("error = %v, want incomplete influxdb configuration", err)
	}
}

func TestLoadConfigDefaultsTraceBucketsAndWriter(t *testing.T) {
	path := writeTraceConfig(t, `
Database:
  Type: "influxdb"
  Influxdb:
    Url: "http://localhost:8086"
    Token: "token"
    Org: "org"
  TraceWriter:
    Shards: 0
    BatchSpans: -1
    QueueBatches: 0
    FlushIntervalMs: 0
    RetryBackoffMs: 0
    MaxRetryBackoffMs: 0
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Tracing.LogPath != defaultTraceLogPath {
		t.Fatalf("LogPath = %q, want %q", cfg.Tracing.LogPath, defaultTraceLogPath)
	}
	if cfg.DB.InfluxDB.TraceBucket != defaultTraceBucket {
		t.Fatalf("TraceBucket = %q, want %q", cfg.DB.InfluxDB.TraceBucket, defaultTraceBucket)
	}
	if cfg.DB.InfluxDB.TraceCoreBucket != defaultTraceBucket ||
		cfg.DB.InfluxDB.TraceDetailBucket != defaultTraceBucket ||
		cfg.DB.InfluxDB.TraceErrorBucket != defaultTraceBucket {
		t.Fatalf("trace derived buckets did not default to %q: %+v", defaultTraceBucket, cfg.DB.InfluxDB)
	}
	if cfg.DB.TraceWriter.Shards != defaultTraceWriterShards ||
		cfg.DB.TraceWriter.BatchSpans != defaultTraceBatchSpans ||
		cfg.DB.TraceWriter.QueueBatches != defaultTraceQueueBatches ||
		cfg.DB.TraceWriter.FlushIntervalMs != defaultTraceFlushMs ||
		cfg.DB.TraceWriter.RetryBackoffMs != defaultTraceRetryBackoffMs ||
		cfg.DB.TraceWriter.MaxRetryBackoffMs != defaultTraceMaxBackoffMs {
		t.Fatalf("writer defaults = %+v", cfg.DB.TraceWriter)
	}
}
