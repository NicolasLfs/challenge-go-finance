package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"challenge/application"
	"challenge/domain"
	"challenge/httpapi"
	"challenge/observability"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestDecodeWagerMessage(t *testing.T) {
	body := `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-987","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

	message, err := decodeWagerMessage(strings.NewReader(body))
	if err != nil {
		t.Fatalf("decode valid wager message: %v", err)
	}
	if message.messageID != "msg-123" || message.wager.IdempotencyKey != "provider-a:transaction-123" {
		t.Fatalf("decoded identity = (%q, %q)", message.messageID, message.wager.IdempotencyKey)
	}
	if got := message.wager.Amount.Decimal(); got != "25.00" {
		t.Fatalf("decoded amount = %q, want 25.00", got)
	}
	canonicalHash, err := application.CanonicalWagerHash(message.wager)
	if err != nil {
		t.Fatalf("hash decoded wager: %v", err)
	}
	if canonicalHash == message.messageHash {
		t.Fatal("transport message hash must be distinct from the canonical business-payload hash")
	}
}

func TestHTTPAndSQSUseSameBusinessHashExcludingTransportMetadata(t *testing.T) {
	processor := &capturingHTTPWagerUseCases{}
	server := httpapi.NewServer(httpapi.Dependencies{
		Wagers:        processor,
		Authenticator: staticHTTPAuthenticator{principal: httpapi.Principal{ProviderID: "provider-a"}},
	})
	httpBody := `{"providerId":"untrusted-provider","externalTransactionId":"transaction-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-987","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`
	request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(httpBody))
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("Idempotency-Key", "provider-a:transaction-123")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP wager status = %d, body %s", response.Code, response.Body)
	}
	if processor.command.Wager.ProviderID != "provider-a" {
		t.Fatalf("HTTP provider = %q, want authenticated provider", processor.command.Wager.ProviderID)
	}

	sqsMessage, err := decodeWagerMessage(strings.NewReader(validMessageBody()))
	if err != nil {
		t.Fatalf("decode equivalent SQS wager: %v", err)
	}
	httpHash, err := application.CanonicalWagerHash(processor.command.Wager)
	if err != nil {
		t.Fatalf("canonicalize HTTP wager: %v", err)
	}
	sqsHash, err := application.CanonicalWagerHash(sqsMessage.wager)
	if err != nil {
		t.Fatalf("canonicalize SQS wager: %v", err)
	}
	if httpHash != sqsHash {
		t.Fatalf("HTTP/SQS business hashes differ: %s != %s", httpHash, sqsHash)
	}

	otherTransport := strings.Replace(validMessageBody(), `"msg-123"`, `"another-message"`, 1)
	otherTransport = strings.Replace(otherTransport, `2026-09-08T12:00:00Z`, `2026-09-09T12:00:00Z`, 1)
	decodedTransport, err := decodeWagerMessage(strings.NewReader(otherTransport))
	if err != nil {
		t.Fatalf("decode SQS wager with different transport metadata: %v", err)
	}
	transportHash, err := application.CanonicalWagerHash(decodedTransport.wager)
	if err != nil {
		t.Fatalf("canonicalize transport-variant SQS wager: %v", err)
	}
	if transportHash != sqsHash {
		t.Fatalf("transport metadata changed business hash: %s != %s", transportHash, sqsHash)
	}
}

type staticHTTPAuthenticator struct{ principal httpapi.Principal }

func (authenticator staticHTTPAuthenticator) Authenticate(context.Context, string) (httpapi.Principal, error) {
	return authenticator.principal, nil
}

type capturingHTTPWagerUseCases struct {
	command application.ProcessWagerCommand
}

func (useCases *capturingHTTPWagerUseCases) Process(_ context.Context, command application.ProcessWagerCommand) (application.WagerResult, error) {
	useCases.command = command
	return application.WagerResult{TransactionID: "transaction-result", Status: domain.WagerStatusProcessed}, nil
}

func (*capturingHTTPWagerUseCases) GetByID(context.Context, string) (*domain.WagerTransaction, error) {
	return nil, application.ErrNotFound
}

func (*capturingHTTPWagerUseCases) GetByExternalID(context.Context, string, string) (*domain.WagerTransaction, error) {
	return nil, application.ErrNotFound
}

func TestDecodeWagerMessageRejectsInvalidEnvelopes(t *testing.T) {
	validData := `"data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-987","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`
	tests := []struct {
		name string
		body string
	}{
		{name: "missing message ID", body: `{` + validData + `}`},
		{name: "wrong message type", body: `{"messageId":"msg-1","type":"WalletOpened",` + validData + `}`},
		{name: "missing idempotency key", body: `{"messageId":"msg-1","type":"WagerTransactionRequested","data":{"providerId":"provider-a"}}`},
		{name: "opening is not external", body: `{"messageId":"msg-1","type":"WagerTransactionRequested","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"key","playerId":"player-1","walletId":"wallet-1","roundId":"round-1","gameId":"game-1","kind":"OPENING","money":{"amount":"25.00","currency":"BRL"}}}`},
		{name: "invalid money precision", body: strings.Replace(validMessageBody(), `25.00`, `25.001`, 1)},
		{name: "unknown envelope field", body: strings.Replace(validMessageBody(), `"occurredAt"`, `"unexpected":"value","occurredAt"`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeWagerMessage(strings.NewReader(test.body)); err == nil {
				t.Fatal("expected invalid message to be rejected")
			}
		})
	}
}

func validMessageBody() string {
	return `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-987","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`
}

func TestConsumerCommitsMessageBeforeDeletingIt(t *testing.T) {
	calls := []string{}
	client := &fakeSQS{calls: &calls}
	processor := processorFunc(func(_ context.Context, command application.ProcessWagerCommand, inbox application.InboxMessage) (application.WagerResult, error) {
		calls = append(calls, "process")
		if command.Wager.IdempotencyKey != "provider-a:transaction-123" || inbox.MessageID != "msg-123" {
			t.Fatalf("processor received wager/message: %#v / %#v", command.Wager, inbox)
		}
		digest := sha256.Sum256([]byte(validMessageBody()))
		if inbox.MessageHash != hex.EncodeToString(digest[:]) {
			t.Fatalf("message hash = %q, want SHA-256 of body", inbox.MessageHash)
		}
		return application.WagerResult{Status: domain.WagerStatusProcessed}, nil
	})
	consumer := TransactionConsumer{client: client, processor: processor, queueURL: "input-queue", consumerName: "wager-transactions"}

	if err := consumer.handle(context.Background(), types.Message{Body: aws.String(validMessageBody()), ReceiptHandle: aws.String("receipt")}); err != nil {
		t.Fatalf("handle valid message: %v", err)
	}
	if strings.Join(calls, ",") != "process,delete" {
		t.Fatalf("SQS actions = %v, want process before delete", calls)
	}
}

func TestConsumerRetriesTransientFailuresAndRedrivesInvalidMessages(t *testing.T) {
	t.Run("transient processing error gets delayed", func(t *testing.T) {
		calls := []string{}
		client := &fakeSQS{calls: &calls}
		metrics := observability.NewMetrics()
		consumer := TransactionConsumer{
			client: client, queueURL: "input-queue", consumerName: "wager-transactions",
			metrics: metrics,
			processor: processorFunc(func(context.Context, application.ProcessWagerCommand, application.InboxMessage) (application.WagerResult, error) {
				return application.WagerResult{}, errors.New("database unavailable")
			}),
		}
		message := types.Message{Body: aws.String(validMessageBody()), ReceiptHandle: aws.String("receipt"), Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): "2"}}
		if err := consumer.handle(context.Background(), message); err == nil {
			t.Fatal("expected transient failure")
		}
		if len(calls) != 1 || calls[0] != "visibility" || client.visibilitySeconds <= 0 {
			t.Fatalf("retry actions = %v, visibility = %d", calls, client.visibilitySeconds)
		}
		if output := renderedMetrics(t, metrics); !strings.Contains(output, `wager_retries_total{kind="sqs"} 1`) {
			t.Fatalf("transient retry was not measured: %s", output)
		}
	})

	t.Run("invalid envelope is released for redrive", func(t *testing.T) {
		calls := []string{}
		client := &fakeSQS{calls: &calls}
		metrics := observability.NewMetrics()
		consumer := TransactionConsumer{client: client, queueURL: "input-queue", consumerName: "wager-transactions", metrics: metrics}
		err := consumer.handle(context.Background(), types.Message{
			Body: aws.String(`{"messageId":"bad"}`), ReceiptHandle: aws.String("receipt"),
			Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): "5"},
		})
		if err == nil || len(calls) != 1 || calls[0] != "visibility" || client.visibilitySeconds != 0 {
			t.Fatalf("invalid-message handling = error %v, actions %v, visibility %d", err, calls, client.visibilitySeconds)
		}
		output := renderedMetrics(t, metrics)
		if !strings.Contains(output, `wager_dlq_messages_total 1`) || !strings.Contains(output, `wager_retries_total{kind="sqs"} 1`) {
			t.Fatalf("invalid-message redrive was not measured: %s", output)
		}
	})
}

func renderedMetrics(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	response := httptest.NewRecorder()
	metrics.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	return response.Body.String()
}

func TestConsumerDeletesConfirmedBusinessRejection(t *testing.T) {
	calls := []string{}
	client := &fakeSQS{calls: &calls}
	consumer := TransactionConsumer{
		client: client, queueURL: "input-queue", consumerName: "wager-transactions",
		processor: processorFunc(func(context.Context, application.ProcessWagerCommand, application.InboxMessage) (application.WagerResult, error) {
			return application.WagerResult{Status: domain.WagerStatusRejected, FailureCode: "INSUFFICIENT_BALANCE"}, nil
		}),
	}
	if err := consumer.handle(context.Background(), types.Message{Body: aws.String(validMessageBody()), ReceiptHandle: aws.String("receipt")}); err != nil {
		t.Fatalf("handle confirmed business rejection: %v", err)
	}
	if len(calls) != 1 || calls[0] != "delete" {
		t.Fatalf("confirmed rejection actions = %v, want delete after commit", calls)
	}
}

type processorFunc func(context.Context, application.ProcessWagerCommand, application.InboxMessage) (application.WagerResult, error)

func (processor processorFunc) ProcessMessage(ctx context.Context, command application.ProcessWagerCommand, inbox application.InboxMessage) (application.WagerResult, error) {
	return processor(ctx, command, inbox)
}

type fakeSQS struct {
	calls             *[]string
	visibilitySeconds int32
	sent              *sqs.SendMessageInput
	sendErr           error
}

func (client *fakeSQS) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{}, nil
}

func (client *fakeSQS) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	*client.calls = append(*client.calls, "delete")
	return &sqs.DeleteMessageOutput{}, nil
}

func (client *fakeSQS) ChangeMessageVisibility(_ context.Context, input *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	*client.calls = append(*client.calls, "visibility")
	client.visibilitySeconds = input.VisibilityTimeout
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (client *fakeSQS) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	*client.calls = append(*client.calls, "send")
	client.sent = input
	return &sqs.SendMessageOutput{}, client.sendErr
}
