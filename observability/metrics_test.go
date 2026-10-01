package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeWagerRetriesDLQConflictsOutboxAndReconciliation(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordWager("PROCESSED", false, 12*time.Millisecond)
	metrics.RecordWager("PROCESSED", true, 8*time.Millisecond)
	metrics.RecordWager("REJECTED", false, 20*time.Millisecond)
	metrics.RecordRetry("sqs")
	metrics.RecordRetry("reference")
	metrics.RecordDLQ()
	metrics.RecordConflict()
	metrics.SetOutboxLag(1500 * time.Millisecond)
	metrics.RecordReconciliation(false)

	response := httptest.NewRecorder()
	metrics.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("metrics content type = %q, want text/plain", contentType)
	}
	for _, expected := range []string{
		`wager_transactions_total{status="PROCESSED"} 2`,
		`wager_idempotent_replays_total 1`,
		`wager_retries_total{kind="sqs"} 1`,
		`wager_retries_total{kind="reference"} 1`,
		`wager_dlq_messages_total 1`,
		`wager_concurrency_conflicts_total 1`,
		`wager_outbox_lag_milliseconds 1500`,
		`wallet_reconciliation_divergences_total 1`,
		`wager_processing_duration_seconds_count 3`,
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics output does not contain %q:\n%s", expected, response.Body.String())
		}
	}
}

func TestMetricsNormalizeUnboundedLabelValues(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordWager("provider-controlled-value", false, time.Second)
	metrics.RecordRetry("unexpected-kind")

	response := httptest.NewRecorder()
	metrics.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(response.Body.String(), "provider-controlled-value") || strings.Contains(response.Body.String(), "unexpected-kind") {
		t.Fatalf("metrics contain uncontrolled label values: %s", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `wager_transactions_total{status="UNKNOWN"} 1`) || !strings.Contains(response.Body.String(), `wager_retries_total{kind="other"} 1`) {
		t.Fatalf("unexpected metric labels were not normalized: %s", response.Body.String())
	}
}
