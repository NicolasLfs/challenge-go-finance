package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"challenge/application"
	"challenge/domain"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const wagerMessageType = "WagerTransactionRequested"

var ErrInvalidMessage = errors.New("invalid wager message")

type decodedWagerMessage struct {
	messageID   string
	occurredAt  time.Time
	messageHash string
	payload     []byte
	wager       domain.ExternalWagerInput
}

type SQSClient interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type WagerProcessor interface {
	ProcessMessage(context.Context, application.ProcessWagerCommand, application.InboxMessage) (application.WagerResult, error)
}

type Metrics interface {
	RecordWager(string, bool, time.Duration)
	RecordRetry(string)
	RecordDLQ()
	RecordConflict()
}

type TransactionConsumer struct {
	client       SQSClient
	processor    WagerProcessor
	queueURL     string
	consumerName string
	logger       *slog.Logger
	metrics      Metrics
	batchSize    int32
	concurrency  int
}

func NewTransactionConsumer(client SQSClient, processor WagerProcessor, queueURL, consumerName string, logger *slog.Logger, metrics ...Metrics) *TransactionConsumer {
	consumer := &TransactionConsumer{
		client: client, processor: processor, queueURL: queueURL, consumerName: consumerName,
		logger: logger, batchSize: 10, concurrency: 10,
	}
	if len(metrics) > 0 {
		consumer.metrics = metrics[0]
	}
	return consumer
}

func (consumer *TransactionConsumer) Run(ctx context.Context) error {
	semaphore := make(chan struct{}, consumer.concurrency)
	for ctx.Err() == nil {
		output, err := consumer.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(consumer.queueURL), MaxNumberOfMessages: consumer.batchSize,
			WaitTimeSeconds: 20, VisibilityTimeout: 60,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			consumer.logError("receive wager messages", err)
			if !waitForRetry(ctx, time.Second) {
				return nil
			}
			continue
		}
		var batch sync.WaitGroup
		for _, message := range output.Messages {
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				consumer.releaseMessage(message)
				continue
			}
			batch.Add(1)
			go func(message types.Message) {
				defer batch.Done()
				defer func() { <-semaphore }()
				if err := consumer.handle(ctx, message); err != nil && ctx.Err() == nil {
					consumer.logError("handle wager message", err, "messageId", aws.ToString(message.MessageId))
				}
			}(message)
		}
		batch.Wait()
	}
	return nil
}

func (consumer *TransactionConsumer) handle(ctx context.Context, message types.Message) error {
	processingStarted := time.Now()
	decoded, err := decodeWagerMessage(strings.NewReader(aws.ToString(message.Body)))
	if err != nil {
		consumer.recordRetry(message)
		if releaseErr := consumer.changeVisibility(message, 0); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}
	if consumer.processor == nil {
		return errors.New("wager processor is not configured")
	}
	result, err := consumer.processor.ProcessMessage(ctx, application.ProcessWagerCommand{Wager: decoded.wager}, application.InboxMessage{
		ConsumerName: consumer.consumerName, MessageID: decoded.messageID,
		MessageHash: decoded.messageHash, Payload: decoded.payload,
	})
	if err != nil {
		visibility := retryVisibility(message)
		if isPermanentMessageError(err) {
			visibility = 0
		}
		consumer.recordRetry(message)
		if errors.Is(err, application.ErrIdempotencyConflict) || errors.Is(err, application.ErrExternalTransactionConflict) || errors.Is(err, application.ErrUniqueConflict) {
			if consumer.metrics != nil {
				consumer.metrics.RecordConflict()
			}
		}
		if releaseErr := consumer.changeVisibility(message, visibility); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}
	if consumer.metrics != nil {
		consumer.metrics.RecordWager(string(result.Status), result.IdempotentReplay, time.Since(processingStarted))
	}
	if result.Status == domain.WagerStatusFailed {
		consumer.recordRetry(message)
		if err := consumer.changeVisibility(message, 0); err != nil {
			return err
		}
		return fmt.Errorf("wager processing failed permanently: transaction %s", result.TransactionID)
	}
	_, err = consumer.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(consumer.queueURL), ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		return fmt.Errorf("delete committed wager message %s: %w", decoded.messageID, err)
	}
	return nil
}

func (consumer *TransactionConsumer) recordRetry(message types.Message) {
	if consumer.metrics == nil {
		return
	}
	consumer.metrics.RecordRetry("sqs")
	attempt, _ := strconv.Atoi(message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if attempt >= 5 {
		consumer.metrics.RecordDLQ()
	}
}

func (consumer *TransactionConsumer) changeVisibility(message types.Message, seconds int32) error {
	if aws.ToString(message.ReceiptHandle) == "" {
		return errors.New("SQS message has no receipt handle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := consumer.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(consumer.queueURL), ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: seconds,
	})
	if err != nil {
		return fmt.Errorf("change wager message visibility: %w", err)
	}
	return nil
}

func (consumer *TransactionConsumer) releaseMessage(message types.Message) {
	if err := consumer.changeVisibility(message, 0); err != nil {
		consumer.logError("release unstarted wager message", err)
	}
}

func (consumer *TransactionConsumer) logError(message string, err error, attributes ...any) {
	if consumer.logger != nil {
		consumer.logger.Error(message, append(attributes, "error", err)...)
	}
}

func retryVisibility(message types.Message) int32 {
	attempt, _ := strconv.Atoi(message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if attempt < 1 {
		attempt = 1
	}
	seconds := 1 << min(attempt-1, 6)
	return int32(min(seconds, 60))
}

func isPermanentMessageError(err error) bool {
	return errors.Is(err, ErrInvalidMessage) || errors.Is(err, application.ErrInboxConflict) ||
		errors.Is(err, application.ErrInboxIncomplete) || errors.Is(err, application.ErrIdempotencyConflict) ||
		errors.Is(err, application.ErrExternalTransactionConflict) || errors.Is(err, application.ErrNotFound) ||
		errors.Is(err, domain.ErrInvalidWager) ||
		errors.Is(err, domain.ErrInvalidMoney) || errors.Is(err, domain.ErrNegativeMoney)
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type wagerMessageEnvelope struct {
	MessageID  string              `json:"messageId"`
	Type       string              `json:"type"`
	OccurredAt time.Time           `json:"occurredAt"`
	Data       wagerMessagePayload `json:"data"`
}

type wagerMessagePayload struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	Money                          struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
}

func decodeWagerMessage(reader io.Reader) (decodedWagerMessage, error) {
	body, err := io.ReadAll(io.LimitReader(reader, 256*1024+1))
	if err != nil {
		return decodedWagerMessage{}, fmt.Errorf("read wager message: %w", err)
	}
	if len(body) == 0 || len(body) > 256*1024 {
		return decodedWagerMessage{}, ErrInvalidMessage
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope wagerMessageEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return decodedWagerMessage{}, fmt.Errorf("%w: decode envelope: %v", ErrInvalidMessage, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return decodedWagerMessage{}, fmt.Errorf("%w: trailing JSON content", ErrInvalidMessage)
	}
	data := envelope.Data
	if envelope.MessageID == "" || envelope.Type != wagerMessageType || envelope.OccurredAt.IsZero() ||
		data.ProviderID == "" || data.ExternalTransactionID == "" || data.IdempotencyKey == "" ||
		data.PlayerID == "" || data.WalletID == "" || data.RoundID == "" || data.GameID == "" {
		return decodedWagerMessage{}, ErrInvalidMessage
	}
	typ := domain.WagerType(data.Kind)
	if typ == domain.WagerTypeOpening || (typ != domain.WagerTypeBet && typ != domain.WagerTypeWin && typ != domain.WagerTypeLoss && typ != domain.WagerTypeRefund && typ != domain.WagerTypeRollback) {
		return decodedWagerMessage{}, ErrInvalidMessage
	}
	amount, err := domain.ParseMoney(data.Money.Amount, data.Money.Currency)
	if err != nil {
		return decodedWagerMessage{}, fmt.Errorf("%w: invalid money: %v", ErrInvalidMessage, err)
	}
	wager := domain.ExternalWagerInput{
		ExternalID: data.ExternalTransactionID, ProviderID: data.ProviderID,
		IdempotencyKey: data.IdempotencyKey, PlayerID: data.PlayerID, WalletID: data.WalletID,
		RoundID: data.RoundID, GameID: data.GameID, Type: typ, Amount: amount,
		ReferenceExternalID: data.ReferenceExternalTransactionID,
	}
	digest := sha256.Sum256(body)
	return decodedWagerMessage{
		messageID: envelope.MessageID, occurredAt: envelope.OccurredAt.UTC(),
		messageHash: hex.EncodeToString(digest[:]), payload: body, wager: wager,
	}, nil
}
