package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"challenge/application"
	"challenge/domain"
	"challenge/observability"
)

func TestOutboxPublisherUsesStableFIFOIdentityAndEventEnvelope(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	record := application.OutboxRecord{
		ID: "event-1", AggregateID: "wallet-1", Type: domain.EventTypeWagerTransactionProcessed,
		Version: 1, OccurredAt: now, CreatedAt: now.Add(-time.Minute), Payload: json.RawMessage(`{"transactionId":"transaction-1","walletId":"wallet-1"}`), Attempts: 1,
	}
	repository := &fakeOutboxRepository{records: []application.OutboxRecord{record}}
	calls := []string{}
	client := &fakeSQS{calls: &calls}
	metrics := observability.NewMetrics()
	publisher := OutboxPublisher{client: client, repository: repository, queueURL: "events-queue", now: func() time.Time { return now }, metrics: metrics}

	if err := publisher.publishBatch(context.Background()); err != nil {
		t.Fatalf("publish outbox batch: %v", err)
	}
	if client.sent == nil || *client.sent.MessageGroupId != "wallet-1" || *client.sent.MessageDeduplicationId != "event-1" {
		t.Fatalf("FIFO identities = %#v, want aggregate group and stable event ID", client.sent)
	}
	var envelope struct {
		EventID       string          `json:"eventId"`
		EventType     string          `json:"eventType"`
		AggregateID   string          `json:"aggregateId"`
		CorrelationID string          `json:"correlationId"`
		OccurredAt    time.Time       `json:"occurredAt"`
		Version       int             `json:"version"`
		Data          json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(*client.sent.MessageBody), &envelope); err != nil {
		t.Fatalf("decode published envelope: %v", err)
	}
	if envelope.EventID != "event-1" || envelope.EventType != string(record.Type) || envelope.AggregateID != "wallet-1" || envelope.CorrelationID != "transaction-1" || envelope.Version != 1 {
		t.Fatalf("published envelope = %#v", envelope)
	}
	if string(envelope.Data) != string(record.Payload) || repository.published != "event-1" || len(calls) != 1 || calls[0] != "send" {
		t.Fatalf("data/confirmation/calls = %s / %q / %v", envelope.Data, repository.published, calls)
	}
	if output := renderedMetrics(t, metrics); !strings.Contains(output, "wager_outbox_lag_milliseconds 60000") {
		t.Fatalf("published outbox lag was not measured: %s", output)
	}
}

func TestOutboxPublisherKeepsEventIDAcrossSendConfirmationFailure(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	record := application.OutboxRecord{
		ID: "event-stable", AggregateID: "wallet-1", Type: domain.EventTypeWagerTransactionProcessed,
		Version: 1, OccurredAt: now, CreatedAt: now, Payload: json.RawMessage(`{"transactionId":"transaction-1"}`), Attempts: 1,
	}
	repository := &fakeOutboxRepository{records: []application.OutboxRecord{record}, markErr: errors.New("database unavailable")}
	calls := []string{}
	client := &fakeSQS{calls: &calls}
	metrics := observability.NewMetrics()
	publisher := OutboxPublisher{client: client, repository: repository, queueURL: "events-queue", now: func() time.Time { return now }, metrics: metrics}

	if err := publisher.publishBatch(context.Background()); err == nil {
		t.Fatal("expected outbox confirmation failure")
	}
	firstDeduplicationID := *client.sent.MessageDeduplicationId
	repository.markErr = nil
	if err := publisher.publishBatch(context.Background()); err != nil {
		t.Fatalf("republish after confirmation failure: %v", err)
	}
	if output := renderedMetrics(t, metrics); !strings.Contains(output, `wager_retries_total{kind="outbox"} 1`) {
		t.Fatalf("outbox confirmation retry was not measured: %s", output)
	}
	if firstDeduplicationID != "event-stable" || *client.sent.MessageDeduplicationId != firstDeduplicationID {
		t.Fatalf("deduplication IDs changed across publication attempts: %q then %q", firstDeduplicationID, *client.sent.MessageDeduplicationId)
	}
}

type fakeOutboxRepository struct {
	records   []application.OutboxRecord
	published string
	markErr   error
}

func (repository *fakeOutboxRepository) ClaimOutbox(context.Context, time.Time, time.Time, int32) ([]application.OutboxRecord, error) {
	return repository.records, nil
}

func (repository *fakeOutboxRepository) MarkOutboxPublished(_ context.Context, eventID string, _ time.Time) error {
	if repository.markErr != nil {
		return repository.markErr
	}
	repository.published = eventID
	return nil
}

func (*fakeOutboxRepository) MarkOutboxFailed(context.Context, string, time.Time, string) error {
	return nil
}
