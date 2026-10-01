package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"challenge/application"
	"challenge/domain"
	"challenge/repository"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLocalStackCommitBeforeDeleteRedeliveryAndHTTPReplay(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	endpoint := os.Getenv("SQS_ENDPOINT")
	if databaseURL == "" || endpoint == "" {
		t.Skip("DATABASE_URL and SQS_ENDPOINT are required for PostgreSQL/LocalStack integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(envOr("AWS_DEFAULT_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(envOr("AWS_ACCESS_KEY_ID", "test"), envOr("AWS_SECRET_ACCESS_KEY", "test"), "")),
	)
	if err != nil {
		t.Fatalf("load SQS test configuration: %v", err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	queueName := fmt.Sprintf("wager-test-%d.fifo", time.Now().UnixNano())
	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName), Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
			string(types.QueueAttributeNameVisibilityTimeout):         "60",
		},
	})
	if err != nil {
		t.Fatalf("create LocalStack FIFO queue: %v", err)
	}
	queueURL := aws.ToString(created.QueueUrl)
	eventQueueName := fmt.Sprintf("wager-events-test-%d.fifo", time.Now().UnixNano())
	eventQueue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(eventQueueName), Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
		},
	})
	if err != nil {
		t.Fatalf("create LocalStack events queue: %v", err)
	}
	eventQueueURL := aws.ToString(eventQueue.QueueUrl)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(eventQueueURL)})
	})
	runID := fmt.Sprintf("sqs-integration-%d", time.Now().UnixNano())
	ids := integrationIDs(runID)
	store := repository.NewPostgresRepository(pool)
	wallet, err := application.NewWalletService(store, idsWithPrefix(ids, runID+"-wallet")).OpenWallet(ctx, application.OpenWalletCommand{
		PlayerID: runID + "-player", Balance: mustMoney(t, "100.00"),
	})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	service := application.NewWagerService(store, idsWithPrefix(ids, runID+"-wager"))
	messageID := runID + "-message"
	wager := domain.ExternalWagerInput{
		ExternalID: runID + "-external", ProviderID: runID + "-provider", IdempotencyKey: runID + "-key",
		PlayerID: wallet.PlayerID(), WalletID: wallet.ID(), RoundID: runID + "-round", GameID: "integration-game",
		Type: domain.WagerTypeBet, Amount: mustMoney(t, "25.00"),
	}
	body, err := json.Marshal(struct {
		MessageID  string    `json:"messageId"`
		Type       string    `json:"type"`
		OccurredAt time.Time `json:"occurredAt"`
		Data       struct {
			ProviderID            string `json:"providerId"`
			ExternalTransactionID string `json:"externalTransactionId"`
			IdempotencyKey        string `json:"idempotencyKey"`
			PlayerID              string `json:"playerId"`
			WalletID              string `json:"walletId"`
			RoundID               string `json:"roundId"`
			GameID                string `json:"gameId"`
			Kind                  string `json:"kind"`
			Money                 struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"money"`
		} `json:"data"`
	}{
		MessageID: messageID, Type: wagerMessageType, OccurredAt: time.Now().UTC(),
		Data: struct {
			ProviderID            string `json:"providerId"`
			ExternalTransactionID string `json:"externalTransactionId"`
			IdempotencyKey        string `json:"idempotencyKey"`
			PlayerID              string `json:"playerId"`
			WalletID              string `json:"walletId"`
			RoundID               string `json:"roundId"`
			GameID                string `json:"gameId"`
			Kind                  string `json:"kind"`
			Money                 struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			} `json:"money"`
		}{
			ProviderID: wager.ProviderID, ExternalTransactionID: wager.ExternalID,
			IdempotencyKey: wager.IdempotencyKey, PlayerID: wager.PlayerID, WalletID: wager.WalletID,
			RoundID: wager.RoundID, GameID: wager.GameID, Kind: string(wager.Type),
			Money: struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
			}{Amount: wager.Amount.Decimal(), Currency: wager.Amount.Currency()},
		},
	})
	if err != nil {
		t.Fatalf("encode SQS wager message: %v", err)
	}
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(body)),
		MessageGroupId: aws.String(wallet.ID()), MessageDeduplicationId: aws.String(wager.ExternalID),
	}); err != nil {
		t.Fatalf("send wager to LocalStack: %v", err)
	}
	firstDelivery := receiveOne(t, ctx, client, queueURL)
	decoded, err := decodeWagerMessage(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("decode integration message: %v", err)
	}
	result, err := service.ProcessMessage(ctx, application.ProcessWagerCommand{Wager: decoded.wager}, application.InboxMessage{
		ConsumerName: "wager-transactions", MessageID: decoded.messageID,
		MessageHash: decoded.messageHash, Payload: decoded.payload,
	})
	if err != nil || result.Status != domain.WagerStatusProcessed || result.IdempotentReplay {
		t.Fatalf("commit first delivery before simulated crash: result %#v, error %v", result, err)
	}
	if _, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(queueURL), ReceiptHandle: firstDelivery.ReceiptHandle, VisibilityTimeout: 0,
	}); err != nil {
		t.Fatalf("release message after simulated pre-delete crash: %v", err)
	}
	redelivery := receiveOne(t, ctx, client, queueURL)
	consumer := NewTransactionConsumer(client, service, queueURL, "wager-transactions", nil)
	if err := consumer.handle(ctx, redelivery); err != nil {
		t.Fatalf("handle redelivery after financial commit: %v", err)
	}
	httpReplay, err := service.Process(ctx, application.ProcessWagerCommand{Wager: wager})
	if err != nil || !httpReplay.IdempotentReplay || httpReplay.Balance.Decimal() != "75.00" {
		t.Fatalf("HTTP replay after SQS processing: result %#v, error %v", httpReplay, err)
	}
	var debitCount, inboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.ID()).Scan(&debitCount); err != nil {
		t.Fatalf("count wallet debits: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_inbox WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NOT NULL`, "wager-transactions", messageID).Scan(&inboxCount); err != nil {
		t.Fatalf("count completed inbox record: %v", err)
	}
	if debitCount != 1 || inboxCount != 1 {
		t.Fatalf("durable SQS effects = debits %d, completed inbox %d, want 1 each", debitCount, inboxCount)
	}
	remaining, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1})
	if err != nil || len(remaining.Messages) != 0 {
		t.Fatalf("message remained after committed redelivery handling: count %d, error %v", len(remaining.Messages), err)
	}
	eventRecord := application.OutboxRecord{
		ID: runID + "-event", AggregateID: wallet.ID(), Type: domain.EventTypeWagerTransactionProcessed,
		Version: 1, OccurredAt: time.Now().UTC(), CreatedAt: time.Now().UTC(),
		Payload: json.RawMessage(`{"transactionId":"` + runID + `-transaction","walletId":"` + wallet.ID() + `"}`),
	}
	outbox := &fakeOutboxRepository{records: []application.OutboxRecord{eventRecord}}
	publisher := NewOutboxPublisher(client, outbox, eventQueueURL, nil)
	if err := publisher.publishBatch(ctx); err != nil {
		t.Fatalf("publish outbox event to LocalStack: %v", err)
	}
	published := receiveOne(t, ctx, client, eventQueueURL)
	var publishedEnvelope struct {
		EventID     string `json:"eventId"`
		EventType   string `json:"eventType"`
		AggregateID string `json:"aggregateId"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(published.Body)), &publishedEnvelope); err != nil {
		t.Fatalf("decode LocalStack outbox event: %v", err)
	}
	if publishedEnvelope.EventID != eventRecord.ID || publishedEnvelope.EventType != string(eventRecord.Type) || publishedEnvelope.AggregateID != wallet.ID() {
		t.Fatalf("LocalStack outbox envelope = %#v", publishedEnvelope)
	}
	if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(eventQueueURL), ReceiptHandle: published.ReceiptHandle}); err != nil {
		t.Fatalf("delete test outbox event: %v", err)
	}
}

func TestLocalStackRedrivesExhaustedMessageToDeadLetterQueue(t *testing.T) {
	endpoint := os.Getenv("SQS_ENDPOINT")
	if endpoint == "" {
		t.Skip("SQS_ENDPOINT is required for LocalStack integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(envOr("AWS_DEFAULT_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(envOr("AWS_ACCESS_KEY_ID", "test"), envOr("AWS_SECRET_ACCESS_KEY", "test"), "")),
	)
	if err != nil {
		t.Fatalf("load SQS test configuration: %v", err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	nameSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	deadLetterQueue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-dead-letter-test-" + nameSuffix + ".fifo"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
		},
	})
	if err != nil {
		t.Fatalf("create LocalStack dead-letter queue: %v", err)
	}
	deadLetterURL := aws.ToString(deadLetterQueue.QueueUrl)
	deadLetterAttributes, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(deadLetterURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("read LocalStack dead-letter queue ARN: %v", err)
	}
	redrivePolicy, err := json.Marshal(map[string]string{
		"deadLetterTargetArn": deadLetterAttributes.Attributes[string(types.QueueAttributeNameQueueArn)],
		"maxReceiveCount":     "2",
	})
	if err != nil {
		t.Fatalf("encode dead-letter redrive policy: %v", err)
	}
	inputQueue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("wager-redrive-test-" + nameSuffix + ".fifo"),
		Attributes: map[string]string{
			string(types.QueueAttributeNameFifoQueue):                 "true",
			string(types.QueueAttributeNameContentBasedDeduplication): "false",
			string(types.QueueAttributeNameRedrivePolicy):             string(redrivePolicy),
			string(types.QueueAttributeNameVisibilityTimeout):         "60",
		},
	})
	if err != nil {
		t.Fatalf("create LocalStack input queue with redrive: %v", err)
	}
	inputQueueURL := aws.ToString(inputQueue.QueueUrl)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(inputQueueURL)})
		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(deadLetterURL)})
	})
	invalidBody := `{"messageId":"invalid"}`
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(inputQueueURL), MessageBody: aws.String(invalidBody),
		MessageGroupId: aws.String("invalid-message-group"), MessageDeduplicationId: aws.String("invalid-message"),
	}); err != nil {
		t.Fatalf("send invalid message to LocalStack: %v", err)
	}
	consumer := NewTransactionConsumer(client, nil, inputQueueURL, "wager-transactions", nil)
	for attempt := 1; attempt <= 5; attempt++ {
		output, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(inputQueueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
		})
		if err != nil {
			t.Fatalf("receive invalid source delivery %d: %v", attempt, err)
		}
		if len(output.Messages) == 0 {
			break
		}
		if err := consumer.handle(ctx, output.Messages[0]); err == nil {
			t.Fatalf("invalid delivery %d unexpectedly succeeded", attempt)
		}
	}
	for ctx.Err() == nil {
		output, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(deadLetterURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
		})
		if err != nil {
			t.Fatalf("receive redriven LocalStack message: %v", err)
		}
		if len(output.Messages) == 0 {
			continue
		}
		if aws.ToString(output.Messages[0].Body) != invalidBody {
			t.Fatalf("dead-letter body = %q, want original invalid message", aws.ToString(output.Messages[0].Body))
		}
		return
	}
	t.Fatal("LocalStack did not redrive the exhausted message to the DLQ")
}

func receiveOne(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string) types.Message {
	t.Helper()
	output, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 2,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		t.Fatalf("receive SQS message: %v", err)
	}
	if len(output.Messages) != 1 {
		t.Fatalf("received %d SQS messages, want 1", len(output.Messages))
	}
	return output.Messages[0]
}

type integrationIDSequence struct{ counter atomic.Uint64 }

func integrationIDs(prefix string) *integrationIDSequence {
	return &integrationIDSequence{}
}

func idsWithPrefix(sequence *integrationIDSequence, prefix string) application.IDGenerator {
	return application.IDGeneratorFunc(func() (string, error) {
		return fmt.Sprintf("%s-%d", prefix, sequence.counter.Add(1)), nil
	})
}

func mustMoney(t *testing.T, value string) domain.Money {
	t.Helper()
	money, err := domain.ParseMoney(value, "BRL")
	if err != nil {
		t.Fatalf("parse money %q: %v", value, err)
	}
	return money
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
