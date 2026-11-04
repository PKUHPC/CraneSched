package main

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"
)

const (
	traceWriterStatsInterval = 5 * time.Second
	traceWriterCloseTimeout  = 10 * time.Second
)

type TraceWriter struct {
	db     TraceStore
	cfg    TraceWriterConfig
	shards []*traceWriterShard

	stop    chan struct{}
	done    chan struct{}
	stopped atomic.Bool
}

type traceWriterShard struct {
	id    int
	db    TraceStore
	cfg   TraceWriterConfig
	queue chan []*protos.SpanInfo

	enqueuedBatches atomic.Uint64
	enqueuedSpans   atomic.Uint64
	failedEnqueues  atomic.Uint64
}

type traceSpanWrite struct {
	bucket string
	span   *protos.SpanInfo
}

func NewTraceWriter(database TraceStore, writerConfig TraceWriterConfig) *TraceWriter {
	normalizeTraceWriterConfig(&writerConfig)
	writer := &TraceWriter{
		db:     database,
		cfg:    writerConfig,
		shards: make([]*traceWriterShard, writerConfig.Shards),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	var wg sync.WaitGroup
	wg.Add(writerConfig.Shards)
	for i := range writer.shards {
		shard := &traceWriterShard{
			id:    i,
			db:    database,
			cfg:   writerConfig,
			queue: make(chan []*protos.SpanInfo, writerConfig.QueueBatches),
		}
		writer.shards[i] = shard
		go shard.run(writer.stop, &wg)
	}
	go func() {
		wg.Wait()
		close(writer.done)
	}()
	return writer
}

func (w *TraceWriter) Enqueue(ctx context.Context, spans []*protos.SpanInfo) error {
	if len(spans) == 0 || w == nil || w.stopped.Load() {
		return nil
	}

	byShard := make(map[int][]*protos.SpanInfo)
	for _, span := range spans {
		shardID := traceShardID(span, len(w.shards))
		byShard[shardID] = append(byShard[shardID], span)
	}

	for shardID, shardSpans := range byShard {
		batch := append([]*protos.SpanInfo(nil), shardSpans...)
		shard := w.shards[shardID]
		select {
		case shard.queue <- batch:
			shard.enqueuedBatches.Add(1)
			shard.enqueuedSpans.Add(uint64(len(batch)))
		case <-ctx.Done():
			failed := shard.failedEnqueues.Add(1)
			if failed == 1 || failed%128 == 0 {
				log.Warnf("Trace writer enqueue canceled shard_id=%d count=%d err=%v",
					shardID, failed, ctx.Err())
			}
			return ctx.Err()
		case <-w.stop:
			return errors.New("trace writer is stopping")
		}
	}
	return nil
}

func (w *TraceWriter) Close() {
	if w == nil || w.stopped.Swap(true) {
		return
	}

	close(w.stop)

	timer := time.NewTimer(traceWriterCloseTimeout)
	defer timer.Stop()

	select {
	case <-w.done:
	case <-timer.C:
		log.Warnf("Trace writer did not finish draining within %s", traceWriterCloseTimeout)
	}
}

func (s *traceWriterShard) run(stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Duration(s.cfg.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	statsTicker := time.NewTicker(traceWriterStatsInterval)
	defer statsTicker.Stop()

	pending := make([]traceSpanWrite, 0, s.cfg.BatchSpans)
	stats := traceWriterStats{}
	var lastEnqueuedBatches uint64
	var lastEnqueuedSpans uint64
	var lastFailedEnqueues uint64
	var retryBackoff time.Duration
	var nextRetry time.Time
	var oldestPending time.Time

	logStats := func(final bool) {
		enqueuedBatches := s.enqueuedBatches.Load()
		enqueuedSpans := s.enqueuedSpans.Load()
		failedEnqueues := s.failedEnqueues.Load()
		snapshot := stats.snapshot()
		stats.reset()

		enqueueBatchesDelta := enqueuedBatches - lastEnqueuedBatches
		enqueueSpansDelta := enqueuedSpans - lastEnqueuedSpans
		failedEnqueuesDelta := failedEnqueues - lastFailedEnqueues
		lastEnqueuedBatches = enqueuedBatches
		lastEnqueuedSpans = enqueuedSpans
		lastFailedEnqueues = failedEnqueues

		queueLen := len(s.queue)
		oldestPendingMs := int64(0)
		if !oldestPending.IsZero() && len(pending) > 0 {
			oldestPendingMs = time.Since(oldestPending).Milliseconds()
		}
		shouldLog := final || enqueueBatchesDelta > 0 || snapshot.flushCount > 0 ||
			queueLen > 0 || len(pending) > 0 || snapshot.writeErrors > 0 ||
			failedEnqueuesDelta > 0 || snapshot.retryCount > 0
		if !shouldLog {
			return
		}

		msg := "TraceWriterStats final=%t shard_id=%d queue_len_batches=%d queue_cap_batches=%d " +
			"pending_spans=%d oldest_pending_ms=%d enqueue_batches=%d enqueue_spans=%d " +
			"flush_count=%d flush_spans=%d flush_batch_spans_p50=%d flush_batch_spans_p95=%d " +
			"flush_batch_spans_p99=%d flush_batch_spans_max=%d flush_elapsed_ms_p50=%d " +
			"flush_elapsed_ms_p95=%d flush_elapsed_ms_p99=%d flush_elapsed_ms_max=%d " +
			"write_errors=%d retry_count=%d retry_spans=%d dropped_spans=%d enqueue_canceled=%d"
		args := []any{
			final, s.id, queueLen, cap(s.queue), len(pending), oldestPendingMs,
			enqueueBatchesDelta, enqueueSpansDelta, snapshot.flushCount,
			snapshot.flushSpans, snapshot.batchP50, snapshot.batchP95,
			snapshot.batchP99, snapshot.batchMax, snapshot.elapsedP50Ms,
			snapshot.elapsedP95Ms, snapshot.elapsedP99Ms, snapshot.elapsedMaxMs,
			snapshot.writeErrors, snapshot.retryCount, snapshot.retrySpans,
			snapshot.droppedSpans, failedEnqueuesDelta,
		}
		if queueLen > cap(s.queue)*3/4 || snapshot.elapsedP95Ms >= 500 ||
			snapshot.writeErrors > 0 || failedEnqueuesDelta > 0 ||
			snapshot.retryCount > 0 || oldestPendingMs >= 5000 {
			log.Warnf(msg, args...)
		} else {
			log.Infof(msg, args...)
		}
	}

	flush := func(force bool) {
		if len(pending) == 0 {
			return
		}
		if !force && !nextRetry.IsZero() && time.Now().Before(nextRetry) {
			return
		}

		limit := s.cfg.BatchSpans
		if limit > len(pending) {
			limit = len(pending)
		}
		batch := pending[:limit]
		byBucket := make(map[string][]*protos.SpanInfo)
		for _, item := range batch {
			byBucket[item.bucket] = append(byBucket[item.bucket], item.span)
		}
		buckets := make([]string, 0, len(byBucket))
		for bucket := range byBucket {
			buckets = append(buckets, bucket)
		}
		sort.Strings(buckets)

		begin := time.Now()
		failed := make([]traceSpanWrite, 0)
		for _, bucket := range buckets {
			if err := s.db.SaveSpansToBucket(bucket, byBucket[bucket]); err != nil {
				stats.writeErrors++
				stats.retryCount++
				stats.retrySpans += uint64(len(byBucket[bucket]))
				if retryBackoff == 0 {
					retryBackoff = time.Duration(s.cfg.RetryBackoffMs) * time.Millisecond
				} else {
					retryBackoff *= 2
					maxBackoff := time.Duration(s.cfg.MaxRetryBackoffMs) * time.Millisecond
					if retryBackoff > maxBackoff {
						retryBackoff = maxBackoff
					}
				}
				nextRetry = time.Now().Add(retryBackoff)
				log.Errorf("Failed to save async trace spans shard_id=%d bucket=%s batch_spans=%d retry_backoff_ms=%d: %v",
					s.id, bucket, len(byBucket[bucket]), retryBackoff.Milliseconds(), err)
				for _, span := range byBucket[bucket] {
					failed = append(failed, traceSpanWrite{bucket: bucket, span: span})
				}
			}
		}

		stats.record(len(batch), time.Since(begin))
		if len(failed) > 0 {
			nextPending := make([]traceSpanWrite, 0, len(failed)+len(pending)-limit)
			nextPending = append(nextPending, failed...)
			nextPending = append(nextPending, pending[limit:]...)
			pending = nextPending
			return
		}

		pending = pending[limit:]
		if len(pending) == 0 {
			oldestPending = time.Time{}
		}
		retryBackoff = 0
		nextRetry = time.Time{}
	}

	appendPending := func(spans []*protos.SpanInfo) {
		if len(spans) == 0 {
			return
		}
		if len(pending) == 0 {
			oldestPending = time.Now()
		}
		for _, span := range spans {
			for _, bucket := range s.db.TraceBucketsForSpan(span) {
				pending = append(pending, traceSpanWrite{bucket: bucket, span: span})
			}
		}
	}

	for {
		select {
		case spans := <-s.queue:
			appendPending(spans)
			for len(pending) >= s.cfg.BatchSpans {
				before := len(pending)
				flush(false)
				if len(pending) == before {
					break
				}
			}
		case <-ticker.C:
			flush(false)
		case <-statsTicker.C:
			logStats(false)
		case <-stop:
			for {
				select {
				case spans := <-s.queue:
					appendPending(spans)
					for len(pending) >= s.cfg.BatchSpans {
						before := len(pending)
						flush(true)
						if len(pending) == before {
							break
						}
					}
				default:
					for len(pending) > 0 {
						before := len(pending)
						flush(true)
						if len(pending) == before {
							stats.droppedSpans += uint64(len(pending))
							log.Warnf("Trace writer shard_id=%d stopped with %d undrained spans", s.id, len(pending))
							pending = pending[:0]
						}
					}
					logStats(true)
					return
				}
			}
		}
	}
}

type traceWriterStats struct {
	flushCount   uint64
	flushSpans   uint64
	writeErrors  uint64
	retryCount   uint64
	retrySpans   uint64
	droppedSpans uint64
	batchSizes   []int
	elapsedMs    []int64
}

type traceWriterStatsSnapshot struct {
	flushCount   uint64
	flushSpans   uint64
	writeErrors  uint64
	retryCount   uint64
	retrySpans   uint64
	droppedSpans uint64
	batchP50     int
	batchP95     int
	batchP99     int
	batchMax     int
	elapsedP50Ms int64
	elapsedP95Ms int64
	elapsedP99Ms int64
	elapsedMaxMs int64
}

func (s *traceWriterStats) record(batchSize int, elapsed time.Duration) {
	if batchSize == 0 {
		return
	}
	s.flushCount++
	s.flushSpans += uint64(batchSize)
	s.batchSizes = append(s.batchSizes, batchSize)
	s.elapsedMs = append(s.elapsedMs, elapsed.Milliseconds())
}

func (s *traceWriterStats) reset() {
	s.flushCount = 0
	s.flushSpans = 0
	s.writeErrors = 0
	s.retryCount = 0
	s.retrySpans = 0
	s.droppedSpans = 0
	s.batchSizes = s.batchSizes[:0]
	s.elapsedMs = s.elapsedMs[:0]
}

func (s *traceWriterStats) snapshot() traceWriterStatsSnapshot {
	return traceWriterStatsSnapshot{
		flushCount:   s.flushCount,
		flushSpans:   s.flushSpans,
		writeErrors:  s.writeErrors,
		retryCount:   s.retryCount,
		retrySpans:   s.retrySpans,
		droppedSpans: s.droppedSpans,
		batchP50:     percentileInt(s.batchSizes, 50),
		batchP95:     percentileInt(s.batchSizes, 95),
		batchP99:     percentileInt(s.batchSizes, 99),
		batchMax:     percentileInt(s.batchSizes, 100),
		elapsedP50Ms: percentileInt64(s.elapsedMs, 50),
		elapsedP95Ms: percentileInt64(s.elapsedMs, 95),
		elapsedP99Ms: percentileInt64(s.elapsedMs, 99),
		elapsedMaxMs: percentileInt64(s.elapsedMs, 100),
	}
}

func traceShardID(span *protos.SpanInfo, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
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
	return int(h.Sum32() % uint32(shardCount))
}

func percentileInt(values []int, percentile int) int {
	if len(values) == 0 {
		return 0
	}
	copied := append([]int(nil), values...)
	sort.Ints(copied)
	idx := percentileIndex(len(copied), percentile)
	return copied[idx]
}

func percentileInt64(values []int64, percentile int) int64 {
	if len(values) == 0 {
		return 0
	}
	copied := append([]int64(nil), values...)
	sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
	idx := percentileIndex(len(copied), percentile)
	return copied[idx]
}

func percentileIndex(length int, percentile int) int {
	if length <= 1 {
		return 0
	}
	if percentile <= 0 {
		return 0
	}
	if percentile >= 100 {
		return length - 1
	}
	idx := (length*percentile + 99) / 100
	if idx <= 0 {
		return 0
	}
	if idx > length {
		return length - 1
	}
	return idx - 1
}
