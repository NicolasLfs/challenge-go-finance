package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"challenge/application"
	"challenge/httpapi"
	"challenge/internal/localenv"
	"challenge/messaging"
	"challenge/observability"
	"challenge/repository"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

type Config struct {
	DatabaseURL          string
	HTTPAddress          string
	OIDCIssuerURL        string
	OIDCAudience         string
	OIDCInternalClientID string
	AWSRegion            string
	AWSAccessKeyID       string
	AWSSecretAccessKey   string
	SQSEndpoint          string
	SQSReadyQueueURL     string
	SQSEventsQueueURL    string
}

var Module = fx.Module("wagering-application",
	fx.Provide(loadConfig),
	fx.Provide(newPostgresPool),
	fx.Provide(newSQSClient),
	fx.Provide(observability.NewMetrics),
	fx.Provide(newIDGenerator),
	fx.Provide(repository.NewPostgresRepository),
	fx.Provide(newApplicationRepository),
	fx.Provide(newOutboxRepository),
	fx.Provide(application.NewWalletService),
	fx.Provide(application.NewWagerService),
	fx.Provide(application.NewReconciliationService),
	fx.Provide(newLogger),
	fx.Provide(newOIDCAuthenticator),
	fx.Provide(newReadinessChecker),
	fx.Provide(newHTTPHandler),
	fx.Provide(newReferenceWorker),
	fx.Provide(newTransactionConsumer),
	fx.Provide(newOutboxPublisher),
	fx.Invoke(func(*application.WalletService, *application.WagerService, *application.ReconciliationService, *httpapi.Server) {
	}),
	fx.Invoke(registerReferenceWorker),
	fx.Invoke(registerTransactionConsumer),
	fx.Invoke(registerOutboxPublisher),
	fx.Invoke(registerHTTPServer),
)

func newHTTPHandler(wallets *application.WalletService, wagers *application.WagerService, reconciliation *application.ReconciliationService, authenticator *httpapi.OIDCAuthenticator, readiness *dependencyReadiness, metrics *observability.Metrics, logger *slog.Logger) *httpapi.Server {
	return httpapi.NewServer(httpapi.Dependencies{
		Wallets: wallets, Wagers: wagers, Reconciliation: reconciliation, Authenticator: authenticator, Readiness: readiness,
		Metrics: metrics, Logger: logger,
	})
}

func newOIDCAuthenticator(config Config) (*httpapi.OIDCAuthenticator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpapi.NewOIDCAuthenticator(ctx, config.OIDCIssuerURL, config.OIDCAudience, config.OIDCInternalClientID)
}

func newSQSClient(config Config) (*sqs.Client, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(config.AWSRegion)}
	if config.AWSAccessKeyID != "" || config.AWSSecretAccessKey != "" {
		if config.AWSAccessKeyID == "" || config.AWSSecretAccessKey == "" {
			return nil, errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together")
		}
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AWSAccessKeyID, config.AWSSecretAccessKey, "")))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return sqs.NewFromConfig(awsConfig, func(options *sqs.Options) {
		if config.SQSEndpoint != "" {
			options.BaseEndpoint = aws.String(config.SQSEndpoint)
		}
	}), nil
}

type dependencyReadiness struct {
	postgres       *pgxpool.Pool
	sqs            *sqs.Client
	queueURL       string
	eventsQueueURL string
}

func newReadinessChecker(postgres *pgxpool.Pool, client *sqs.Client, config Config) *dependencyReadiness {
	return &dependencyReadiness{postgres: postgres, sqs: client, queueURL: config.SQSReadyQueueURL, eventsQueueURL: config.SQSEventsQueueURL}
}

func (readiness *dependencyReadiness) Ready(ctx context.Context) error {
	if err := readiness.postgres.Ping(ctx); err != nil {
		return fmt.Errorf("PostgreSQL readiness: %w", err)
	}
	_, err := readiness.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(readiness.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("SQS readiness: %w", err)
	}
	_, err = readiness.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(readiness.eventsQueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("SQS events readiness: %w", err)
	}
	return nil
}

func registerHTTPServer(lifecycle fx.Lifecycle, config Config, handler *httpapi.Server, logger *slog.Logger) {
	server := &http.Server{Addr: config.HTTPAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	var listener net.Listener
	var done chan error
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			opened, err := net.Listen("tcp", config.HTTPAddress)
			if err != nil {
				return fmt.Errorf("listen for HTTP: %w", err)
			}
			listener = opened
			done = make(chan error, 1)
			go func() {
				err := server.Serve(listener)
				if !errors.Is(err, http.ErrServerClosed) {
					logger.Error("HTTP server stopped unexpectedly", "error", err)
				}
				done <- err
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if listener == nil {
				return nil
			}
			if err := server.Shutdown(ctx); err != nil {
				_ = server.Close()
				return fmt.Errorf("shut down HTTP server: %w", err)
			}
			select {
			case err := <-done:
				if errors.Is(err, http.ErrServerClosed) {
					return nil
				}
				return err
			case <-ctx.Done():
				return fmt.Errorf("wait for HTTP server shutdown: %w", ctx.Err())
			}
		},
	})
}

func newIDGenerator() application.IDGenerator {
	return application.IDGeneratorFunc(func() (string, error) {
		var value [16]byte
		if _, err := rand.Read(value[:]); err != nil {
			return "", err
		}
		value[6] = value[6]&0x0f | 0x40
		value[8] = value[8]&0x3f | 0x80
		encoded := hex.EncodeToString(value[:])
		return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
	})
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

func newReferenceWorker(service *application.WagerService, logger *slog.Logger, metrics *observability.Metrics) *application.ReferenceWorker {
	return application.NewReferenceWorker(service, time.Second, 100, func(err error) {
		metrics.RecordRetry("reference")
		logger.Error("reference worker iteration failed", "error", err)
	})
}

func newApplicationRepository(repository *repository.PostgresRepository) application.Repository {
	return repository
}

func newOutboxRepository(repository *repository.PostgresRepository) application.OutboxRepository {
	return repository
}

func newTransactionConsumer(client *sqs.Client, service *application.WagerService, config Config, logger *slog.Logger, metrics *observability.Metrics) *messaging.TransactionConsumer {
	return messaging.NewTransactionConsumer(client, service, config.SQSReadyQueueURL, "wager-transactions", logger, metrics)
}

func newOutboxPublisher(client *sqs.Client, repository application.OutboxRepository, config Config, logger *slog.Logger, metrics *observability.Metrics) *messaging.OutboxPublisher {
	return messaging.NewOutboxPublisher(client, repository, config.SQSEventsQueueURL, logger, metrics)
}

func registerReferenceWorker(lifecycle fx.Lifecycle, worker *application.ReferenceWorker) {
	var cancel context.CancelFunc
	done := make(chan struct{})
	started := false
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			workerContext, stop := context.WithCancel(context.Background())
			cancel = stop
			started = true
			go func() {
				defer close(done)
				_ = worker.Run(workerContext)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if !started {
				return nil
			}
			started = false
			if cancel != nil {
				cancel()
			}
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("wait for reference worker shutdown: %w", ctx.Err())
			}
		},
	})
}

func registerTransactionConsumer(lifecycle fx.Lifecycle, consumer *messaging.TransactionConsumer, logger *slog.Logger) {
	registerWorker(lifecycle, "SQS transaction consumer", consumer.Run, logger)
}

func registerOutboxPublisher(lifecycle fx.Lifecycle, publisher *messaging.OutboxPublisher, logger *slog.Logger) {
	registerWorker(lifecycle, "outbox publisher", publisher.Run, logger)
}

func registerWorker(lifecycle fx.Lifecycle, name string, run func(context.Context) error, logger *slog.Logger) {
	var cancel context.CancelFunc
	done := make(chan struct{})
	started := false
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			workerContext, stop := context.WithCancel(context.Background())
			cancel = stop
			started = true
			go func() {
				defer close(done)
				if err := run(workerContext); err != nil && workerContext.Err() == nil {
					logger.Error("worker stopped unexpectedly", "worker", name, "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if !started {
				return nil
			}
			started = false
			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("wait for %s shutdown: %w", name, ctx.Err())
			}
		},
	})
}

func loadConfig() (Config, error) {
	if err := localenv.Load(); err != nil {
		return Config{}, fmt.Errorf("load local environment: %w", err)
	}
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if _, err := pgxpool.ParseConfig(databaseURL); err != nil {
		return Config{}, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	getEnv := func(key, fallback string) string {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
		return fallback
	}
	config := Config{
		DatabaseURL: databaseURL, HTTPAddress: getEnv("APP_HTTP_ADDR", ":8080"),
		OIDCIssuerURL: getEnv("OIDC_ISSUER_URL", ""), OIDCAudience: getEnv("OIDC_AUDIENCE", "wagering-api"),
		OIDCInternalClientID: getEnv("OIDC_INTERNAL_CLIENT_ID", "internal-service"),
		AWSRegion:            getEnv("AWS_DEFAULT_REGION", "us-east-1"), AWSAccessKeyID: getEnv("AWS_ACCESS_KEY_ID", ""),
		AWSSecretAccessKey: getEnv("AWS_SECRET_ACCESS_KEY", ""), SQSEndpoint: getEnv("SQS_ENDPOINT", ""),
		SQSReadyQueueURL:  getEnv("SQS_TRANSACTIONS_QUEUE_URL", ""),
		SQSEventsQueueURL: getEnv("SQS_EVENTS_QUEUE_URL", ""),
	}
	if config.OIDCIssuerURL == "" || config.SQSReadyQueueURL == "" || config.SQSEventsQueueURL == "" {
		return Config{}, errors.New("OIDC_ISSUER_URL, SQS_TRANSACTIONS_QUEUE_URL, and SQS_EVENTS_QUEUE_URL are required")
	}
	return config, nil
}

func newPostgresPool(lifecycle fx.Lifecycle, config Config) (*pgxpool.Pool, error) {
	parsed, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL configuration: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), parsed)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			startCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := pool.Ping(startCtx); err != nil {
				pool.Close()
				return fmt.Errorf("ping PostgreSQL: %w", err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}
