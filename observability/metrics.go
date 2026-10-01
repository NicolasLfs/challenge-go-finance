package observability

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

var processingBuckets = [...]time.Duration{
	time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond,
	500 * time.Millisecond, time.Second, 2500 * time.Millisecond, 5 * time.Second,
	10 * time.Second,
}

type Metrics struct {
	mu                        sync.Mutex
	wagerResults              map[string]uint64
	retries                   map[string]uint64
	idempotentReplays         uint64
	dlqMessages               uint64
	concurrencyConflicts      uint64
	outboxLagMilliseconds     int64
	processingCount           uint64
	processingDurationNanos   uint64
	processingDurationBuckets [len(processingBuckets)]uint64
	reconciliationDivergences uint64
}

func NewMetrics() *Metrics {
	return &Metrics{wagerResults: make(map[string]uint64), retries: make(map[string]uint64)}
}

func (metrics *Metrics) RecordWager(status string, replay bool, duration time.Duration) {
	if metrics == nil {
		return
	}
	status = normalizeStatus(status)
	if duration < 0 {
		duration = 0
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.wagerResults[status]++
	if replay {
		metrics.idempotentReplays++
	}
	metrics.processingCount++
	metrics.processingDurationNanos += uint64(duration)
	for index, bucket := range processingBuckets {
		if duration <= bucket {
			metrics.processingDurationBuckets[index]++
		}
	}
}

func (metrics *Metrics) RecordRetry(kind string) {
	if metrics == nil {
		return
	}
	switch kind {
	case "sqs", "reference", "outbox":
	default:
		kind = "other"
	}
	metrics.mu.Lock()
	metrics.retries[kind]++
	metrics.mu.Unlock()
}

func (metrics *Metrics) RecordDLQ() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.dlqMessages++
	metrics.mu.Unlock()
}

func (metrics *Metrics) RecordConflict() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.concurrencyConflicts++
	metrics.mu.Unlock()
}

func (metrics *Metrics) SetOutboxLag(lag time.Duration) {
	if metrics == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	metrics.mu.Lock()
	metrics.outboxLagMilliseconds = lag.Milliseconds()
	metrics.mu.Unlock()
}

func (metrics *Metrics) RecordReconciliation(consistent bool) {
	if metrics == nil || consistent {
		return
	}
	metrics.mu.Lock()
	metrics.reconciliationDivergences++
	metrics.mu.Unlock()
}

func (metrics *Metrics) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if metrics == nil {
		http.Error(writer, "metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	fmt.Fprintln(writer, "# HELP wager_transactions_total Wager processing results by terminal or pending status.")
	fmt.Fprintln(writer, "# TYPE wager_transactions_total counter")
	statuses := make([]string, 0, len(metrics.wagerResults))
	for status := range metrics.wagerResults {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		fmt.Fprintf(writer, "wager_transactions_total{status=%q} %d\n", status, metrics.wagerResults[status])
	}
	fmt.Fprintln(writer, "# HELP wager_idempotent_replays_total Persisted idempotent wager replays.")
	fmt.Fprintln(writer, "# TYPE wager_idempotent_replays_total counter")
	fmt.Fprintf(writer, "wager_idempotent_replays_total %d\n", metrics.idempotentReplays)
	fmt.Fprintln(writer, "# HELP wager_retries_total Processing retries by subsystem.")
	fmt.Fprintln(writer, "# TYPE wager_retries_total counter")
	retries := make([]string, 0, len(metrics.retries))
	for kind := range metrics.retries {
		retries = append(retries, kind)
	}
	sort.Strings(retries)
	for _, kind := range retries {
		fmt.Fprintf(writer, "wager_retries_total{kind=%q} %d\n", kind, metrics.retries[kind])
	}
	fmt.Fprintln(writer, "# HELP wager_dlq_messages_total Messages released for dead-letter redrive after exhausted receives.")
	fmt.Fprintln(writer, "# TYPE wager_dlq_messages_total counter")
	fmt.Fprintf(writer, "wager_dlq_messages_total %d\n", metrics.dlqMessages)
	fmt.Fprintln(writer, "# HELP wager_concurrency_conflicts_total Unresolved persistent uniqueness conflicts.")
	fmt.Fprintln(writer, "# TYPE wager_concurrency_conflicts_total counter")
	fmt.Fprintf(writer, "wager_concurrency_conflicts_total %d\n", metrics.concurrencyConflicts)
	fmt.Fprintln(writer, "# HELP wager_outbox_lag_milliseconds Age of the latest published outbox event.")
	fmt.Fprintln(writer, "# TYPE wager_outbox_lag_milliseconds gauge")
	fmt.Fprintf(writer, "wager_outbox_lag_milliseconds %d\n", metrics.outboxLagMilliseconds)
	fmt.Fprintln(writer, "# HELP wager_processing_duration_seconds Time spent processing wager requests.")
	fmt.Fprintln(writer, "# TYPE wager_processing_duration_seconds histogram")
	for index, bucket := range processingBuckets {
		fmt.Fprintf(writer, "wager_processing_duration_seconds_bucket{le=\"%g\"} %d\n", bucket.Seconds(), metrics.processingDurationBuckets[index])
	}
	fmt.Fprintf(writer, "wager_processing_duration_seconds_bucket{le=\"+Inf\"} %d\n", metrics.processingCount)
	fmt.Fprintf(writer, "wager_processing_duration_seconds_sum %g\n", float64(metrics.processingDurationNanos)/float64(time.Second))
	fmt.Fprintf(writer, "wager_processing_duration_seconds_count %d\n", metrics.processingCount)
	fmt.Fprintln(writer, "# HELP wallet_reconciliation_divergences_total Reconciliation checks that found a balance mismatch.")
	fmt.Fprintln(writer, "# TYPE wallet_reconciliation_divergences_total counter")
	fmt.Fprintf(writer, "wallet_reconciliation_divergences_total %d\n", metrics.reconciliationDivergences)
}

func normalizeStatus(status string) string {
	switch status {
	case "PENDING", "PENDING_REFERENCE", "PROCESSED", "REJECTED", "FAILED":
		return status
	default:
		return "UNKNOWN"
	}
}
