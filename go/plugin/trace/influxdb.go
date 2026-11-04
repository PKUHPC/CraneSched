package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	"CraneFrontEnd/generated/protos"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

const (
	maxRetries    = 3
	retryInterval = 5 * time.Second
)

type TraceStore interface {
	SaveSpansToBucket(string, []*protos.SpanInfo) error
	TraceBucketsForSpan(*protos.SpanInfo) []string
	Close() error
}

type InfluxTraceStore struct {
	client            influxdb2.Client
	org               string
	traceBucket       string
	traceCoreBucket   string
	traceDetailBucket string
	traceErrorBucket  string
	traceShardBuckets []string
}

var coreTraceSpanNames = map[string]struct{}{
	"job/pending":   {},
	"job/lifecycle": {},
	"step/execute":  {},
	"job/end":       {},
}

func NewTraceStore(cfg *Config) (TraceStore, error) {
	switch cfg.DB.Type {
	case "influxdb":
		return NewInfluxTraceStore(cfg)
	default:
		return nil, fmt.Errorf("unsupported trace database type: %s", cfg.DB.Type)
	}
}

func NewInfluxTraceStore(cfg *Config) (*InfluxTraceStore, error) {
	var client influxdb2.Client
	var err error

	for i := 0; i < maxRetries; i++ {
		client = influxdb2.NewClient(cfg.DB.InfluxDB.URL, cfg.DB.InfluxDB.Token)
		_, err = client.Ping(context.Background())

		if err == nil {
			break
		}

		log.Warnf("Failed to connect to InfluxDB (attempt %d/%d): %v", i+1, maxRetries, err)
		client.Close()

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB after %d attempts: %v", maxRetries, err)
	}

	store := &InfluxTraceStore{
		client:            client,
		org:               cfg.DB.InfluxDB.Org,
		traceBucket:       cfg.DB.InfluxDB.TraceBucket,
		traceCoreBucket:   cfg.DB.InfluxDB.TraceCoreBucket,
		traceDetailBucket: cfg.DB.InfluxDB.TraceDetailBucket,
		traceErrorBucket:  cfg.DB.InfluxDB.TraceErrorBucket,
		traceShardBuckets: append([]string(nil), cfg.DB.InfluxDB.TraceShardBuckets...),
	}

	for _, bucket := range store.traceBuckets() {
		if err := store.createBucketIfNotExists(bucket); err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create trace bucket %s: %v", bucket, err)
		}
	}

	return store, nil
}

func (s *InfluxTraceStore) SaveSpansToBucket(bucket string, spans []*protos.SpanInfo) error {
	if len(spans) == 0 {
		return nil
	}
	if bucket == "" {
		bucket = s.traceBucket
	}

	start := time.Now()
	writeAPI := s.client.WriteAPIBlocking(s.org, bucket)
	ctx := context.Background()
	points := make([]*write.Point, 0, len(spans))

	for _, span := range spans {
		tags := map[string]string{
			"name": span.Name,
		}
		if span.ServiceName != "" {
			tags["service"] = span.ServiceName
		}

		startTime := span.StartTime.AsTime()
		endTime := span.EndTime.AsTime()
		duration := endTime.Sub(startTime).Microseconds()

		fields := map[string]interface{}{
			"trace_id":       span.TraceId,
			"span_id":        span.SpanId,
			"parent_span_id": span.ParentSpanId,
			"duration_us":    duration,
		}

		for k, v := range span.Attributes {
			fields[k] = v
		}

		point := influxdb2.NewPoint("spans", tags, fields, endTime)
		points = append(points, point)
	}

	if err := writeAPI.WritePoint(ctx, points...); err != nil {
		log.Errorf("Failed to write %d spans to InfluxDB bucket=%s: %v", len(points), bucket, err)
		return fmt.Errorf("failed to write spans to bucket %s: %v", bucket, err)
	}

	elapsed := time.Since(start)
	log.Debugf("Saved %d trace spans to InfluxDB bucket=%s in %s", len(points), bucket, elapsed)
	if elapsed > time.Second {
		log.Warnf("Slow trace span write: saved %d spans to InfluxDB bucket=%s in %s",
			len(points), bucket, elapsed)
	}
	return nil
}

func (s *InfluxTraceStore) TraceBucketsForSpan(span *protos.SpanInfo) []string {
	if span == nil {
		return []string{s.traceBucket}
	}

	primary := s.traceBucket
	if _, ok := coreTraceSpanNames[span.Name]; ok {
		if len(s.traceShardBuckets) > 0 {
			primary = s.traceShardBuckets[stableTraceShardKey(span)%uint32(len(s.traceShardBuckets))]
		} else if s.traceCoreBucket != "" {
			primary = s.traceCoreBucket
		}

		buckets := []string{primary}
		if spanShouldWriteErrorBucket(span) {
			errorBucket := s.traceErrorBucket
			if errorBucket == "" {
				errorBucket = s.traceBucket
			}
			if errorBucket != "" && errorBucket != primary {
				buckets = append(buckets, errorBucket)
			}
		}
		return buckets
	}

	if spanShouldWriteErrorBucket(span) {
		if s.traceErrorBucket != "" {
			primary = s.traceErrorBucket
		}
	} else if s.traceDetailBucket != "" {
		primary = s.traceDetailBucket
	}
	return []string{primary}
}

func spanShouldWriteErrorBucket(span *protos.SpanInfo) bool {
	if span == nil {
		return false
	}
	if span.Status == protos.SpanStatus_SPAN_STATUS_ERROR {
		return true
	}
	if v, ok := span.Attributes["final_status"]; ok && v != "" &&
		v != "2" && v != "Completed" && v != "completed" {
		return true
	}
	return false
}

func stableTraceShardKey(span *protos.SpanInfo) uint32 {
	key := ""
	if span != nil {
		if jobID := span.Attributes["job_id"]; jobID != "" {
			key = jobID
		} else if span.TraceId != "" {
			key = span.TraceId
		} else {
			key = span.SpanId
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (s *InfluxTraceStore) traceBuckets() []string {
	seen := make(map[string]struct{})
	add := func(bucket string) {
		if bucket == "" {
			return
		}
		seen[bucket] = struct{}{}
	}
	add(s.traceBucket)
	add(s.traceCoreBucket)
	add(s.traceDetailBucket)
	add(s.traceErrorBucket)
	for _, bucket := range s.traceShardBuckets {
		add(bucket)
	}
	buckets := make([]string, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets
}

func (s *InfluxTraceStore) Close() error {
	s.client.Close()
	return nil
}

func (s *InfluxTraceStore) createBucketIfNotExists(bucketName string) error {
	ctx := context.Background()

	if err := s.createOrgIfNotExists(); err != nil {
		return fmt.Errorf("failed to ensure organization exists: %v", err)
	}

	bucketsAPI := s.client.BucketsAPI()
	bucket, _ := bucketsAPI.FindBucketByName(ctx, bucketName)

	if bucket != nil {
		log.Infof("Bucket already exists: %s", bucketName)
		return nil
	}

	log.Infof("Creating bucket: %s", bucketName)
	orgAPI := s.client.OrganizationsAPI()
	org, err := orgAPI.FindOrganizationByName(ctx, s.org)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	_, err = bucketsAPI.CreateBucketWithName(ctx, org, bucketName)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	log.Infof("Successfully created bucket: %s", bucketName)
	return nil
}

func (s *InfluxTraceStore) createOrgIfNotExists() error {
	ctx := context.Background()
	orgAPI := s.client.OrganizationsAPI()

	org, _ := orgAPI.FindOrganizationByName(ctx, s.org)

	if org != nil {
		log.Infof("Organization already exists: %s", s.org)
		return nil
	}

	log.Infof("Creating organization: %s", s.org)
	_, err := orgAPI.CreateOrganizationWithName(ctx, s.org)
	if err != nil {
		return fmt.Errorf("failed to create organization: %v", err)
	}

	log.Infof("Successfully created organization: %s", s.org)
	return nil
}
