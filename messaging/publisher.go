package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"challenge/application"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type OutboxPublisher struct {
	client       SQSClient
	repository   application.OutboxRepository
	queueURL     string
	logger       *slog.Logger
	metrics      OutboxMetrics
	now          func() time.Time
	interval     time.Duration
	lease        time.Duration
	batchSize    int32
	lastLagNanos atomic.Int64
}

type OutboxMetrics interface {
	RecordRetry(string)
	SetOutboxLag(time.Duration)
}

func NewOutboxPublisher(client SQSClient, repository application.OutboxRepository, queueURL string, logger *slog.Logger, metrics ...OutboxMetrics) *OutboxPublisher {
	publisher := &OutboxPublisher{
		client: client, repository: repository, queueURL: queueURL, logger: logger,
		now: func() time.Time { return time.Now().UTC() }, interval: time.Second,
		lease: time.Minute, batchSize: 10,
	}
	if len(metrics) > 0 {
		publisher.metrics = metrics[0]
	}
	return publisher
}

func (publisher *OutboxPublisher) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if err := publisher.publishBatch(ctx); err != nil && ctx.Err() == nil && publisher.logger != nil {
			publisher.logger.Error("publish outbox batch", "error", err)
		}
		if !waitForRetry(ctx, publisher.interval) {
			return nil
		}
	}
	return nil
}

func (publisher *OutboxPublisher) publishBatch(ctx context.Context) error {
	now := publisher.now().UTC()
	events, err := publisher.repository.ClaimOutbox(ctx, now, now.Add(-publisher.lease), publisher.batchSize)
	if err != nil {
		return fmt.Errorf("claim pending outbox events: %w", err)
	}
	var failures []error
	for _, event := range events {
		body, err := encodeEventEnvelope(event)
		if err == nil {
			_, err = publisher.client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl: aws.String(publisher.queueURL), MessageBody: aws.String(body),
				MessageGroupId: aws.String(event.AggregateID), MessageDeduplicationId: aws.String(event.ID),
			})
		}
		if err != nil {
			if publisher.metrics != nil {
				publisher.metrics.RecordRetry("outbox")
			}
			nextAttempt := now.Add(outboxRetryDelay(event.Attempts))
			failure := truncateError(err.Error(), 512)
			if markErr := publisher.repository.MarkOutboxFailed(ctx, event.ID, nextAttempt, failure); markErr != nil {
				failures = append(failures, errors.Join(fmt.Errorf("publish outbox event %s: %w", event.ID, err), fmt.Errorf("schedule outbox retry: %w", markErr)))
			} else {
				failures = append(failures, fmt.Errorf("publish outbox event %s: %w", event.ID, err))
			}
			if publisher.logger != nil {
				publisher.logger.Error("outbox event publish failed", "eventId", event.ID, "aggregateId", event.AggregateID, "attempt", event.Attempts, "error", err)
			}
			continue
		}
		if err := publisher.repository.MarkOutboxPublished(ctx, event.ID, publisher.now().UTC()); err != nil {
			if publisher.metrics != nil {
				publisher.metrics.RecordRetry("outbox")
			}
			failures = append(failures, fmt.Errorf("confirm published outbox event %s: %w", event.ID, err))
			continue
		}
		lag := publisher.now().Sub(event.CreatedAt)
		if lag < 0 {
			lag = 0
		}
		publisher.lastLagNanos.Store(int64(lag))
		if publisher.metrics != nil {
			publisher.metrics.SetOutboxLag(lag)
		}
		if publisher.logger != nil {
			publisher.logger.Info("outbox event published", "eventId", event.ID, "aggregateId", event.AggregateID, "outboxLagMilliseconds", lag.Milliseconds(), "attempt", event.Attempts)
		}
	}
	return errors.Join(failures...)
}

func (publisher *OutboxPublisher) LastPublishedLag() time.Duration {
	return time.Duration(publisher.lastLagNanos.Load())
}

type eventEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

func encodeEventEnvelope(event application.OutboxRecord) (string, error) {
	if event.ID == "" || event.AggregateID == "" || event.Type == "" || event.Version < 1 || event.OccurredAt.IsZero() || !json.Valid(event.Payload) {
		return "", errors.New("invalid outbox event snapshot")
	}
	var correlation struct {
		TransactionID string `json:"transactionId"`
	}
	if err := json.Unmarshal(event.Payload, &correlation); err != nil || correlation.TransactionID == "" {
		return "", errors.New("outbox event snapshot has no transaction correlation ID")
	}
	body, err := json.Marshal(eventEnvelope{
		EventID: event.ID, EventType: string(event.Type), AggregateID: event.AggregateID,
		CorrelationID: correlation.TransactionID, OccurredAt: event.OccurredAt.UTC(),
		Version: event.Version, Data: append(json.RawMessage(nil), event.Payload...),
	})
	if err != nil {
		return "", fmt.Errorf("encode outbox event envelope: %w", err)
	}
	return string(body), nil
}

func outboxRetryDelay(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := min(int(attempt-1), 10)
	seconds := min(1<<shift, 15*60)
	return time.Duration(seconds) * time.Second
}

func truncateError(message string, maximum int) string {
	message = strings.TrimSpace(message)
	if len(message) > maximum {
		return message[:maximum]
	}
	return message
}
