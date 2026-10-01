package app

import (
	"context"
	"os"
	"testing"
	"time"

	"challenge/application"
	"challenge/domain"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func TestModuleValidatesFxGraph(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://wagering:local@localhost:5432/wagering?sslmode=disable")
	setModuleEnvironment(t)
	if err := fx.ValidateApp(Module); err != nil {
		t.Fatalf("validate Fx graph: %v", err)
	}
}

func TestLoadConfigReadsEnvFile(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile(".env", []byte("DATABASE_URL=postgres://wagering:local@localhost:5432/wagering?sslmode=disable\nOIDC_ISSUER_URL=http://localhost:8081/realms/wagering-local\nSQS_TRANSACTIONS_QUEUE_URL=http://localhost:4566/transactions\nSQS_EVENTS_QUEUE_URL=http://localhost:4566/events\n"), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	for _, key := range []string{"DATABASE_URL", "OIDC_ISSUER_URL", "SQS_TRANSACTIONS_QUEUE_URL", "SQS_EVENTS_QUEUE_URL"} {
		previous, existed := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, previous)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}

	config, err := loadConfig()
	if err != nil {
		t.Fatalf("load config from .env: %v", err)
	}
	if config.DatabaseURL != "postgres://wagering:local@localhost:5432/wagering?sslmode=disable" || config.OIDCIssuerURL != "http://localhost:8081/realms/wagering-local" {
		t.Fatalf("unexpected config loaded from .env: %+v", config)
	}
}

func TestModuleStartsAndStopsWithPostgres(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" || os.Getenv("OIDC_ISSUER_URL") == "" || os.Getenv("SQS_ENDPOINT") == "" || os.Getenv("SQS_TRANSACTIONS_QUEUE_URL") == "" || os.Getenv("SQS_EVENTS_QUEUE_URL") == "" {
		t.Skip("DATABASE_URL, OIDC_ISSUER_URL, SQS_ENDPOINT, SQS_TRANSACTIONS_QUEUE_URL, and SQS_EVENTS_QUEUE_URL are required for Fx integration")
	}
	t.Setenv("DATABASE_URL", databaseURL)
	t.Setenv("APP_HTTP_ADDR", "127.0.0.1:0")
	var readiness *dependencyReadiness
	application := fx.New(Module, fx.Populate(&readiness))
	if err := application.Err(); err != nil {
		t.Fatalf("construct Fx application: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := application.Start(ctx); err != nil {
		t.Fatalf("start Fx application: %v", err)
	}
	if err := readiness.Ready(ctx); err != nil {
		t.Fatalf("check PostgreSQL and SQS readiness: %v", err)
	}
	if err := application.Stop(ctx); err != nil {
		t.Fatalf("stop Fx application: %v", err)
	}
}

func setModuleEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("OIDC_ISSUER_URL", "http://localhost:8081/realms/wagering-local")
	t.Setenv("OIDC_AUDIENCE", "wagering-api")
	t.Setenv("OIDC_INTERNAL_CLIENT_ID", "internal-service")
	t.Setenv("SQS_ENDPOINT", "http://localhost:4566")
	t.Setenv("SQS_TRANSACTIONS_QUEUE_URL", "http://localhost:4566/queue/us-east-1/000000000000/wager-transactions.fifo")
	t.Setenv("SQS_EVENTS_QUEUE_URL", "http://localhost:4566/queue/us-east-1/000000000000/wager-events.fifo")

	t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
}

func TestFxLifecycleRunsStartAndStopHooks(t *testing.T) {
	started := false
	stopped := false
	fxApp := fx.New(
		fx.Provide(func(lifecycle fx.Lifecycle) *struct{} {
			lifecycle.Append(fx.Hook{
				OnStart: func(context.Context) error { started = true; return nil },
				OnStop:  func(context.Context) error { stopped = true; return nil },
			})
			return &struct{}{}
		}),
		fx.Invoke(func(*struct{}) {}),
		fx.NopLogger,
	)
	if err := fxApp.Err(); err != nil {
		t.Fatalf("construct Fx application: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fxApp.Start(ctx); err != nil {
		t.Fatalf("start Fx application: %v", err)
	}
	if !started {
		t.Fatal("Fx OnStart hook was not called")
	}
	if err := fxApp.Stop(ctx); err != nil {
		t.Fatalf("stop Fx application: %v", err)
	}
	if !stopped {
		t.Fatal("Fx OnStop hook was not called")
	}
}

func TestReferenceWorkerLifecycleStopsTheWorker(t *testing.T) {
	service := application.NewWagerService(emptyReferenceRepository{}, application.IDGeneratorFunc(func() (string, error) {
		return "unused", nil
	}))
	worker := application.NewReferenceWorker(service, time.Hour, 10, nil)
	fxApp := fx.New(
		fx.Provide(func() *application.ReferenceWorker { return worker }),
		fx.Invoke(registerReferenceWorker),
		fx.NopLogger,
	)
	if err := fxApp.Err(); err != nil {
		t.Fatalf("construct worker Fx application: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fxApp.Start(ctx); err != nil {
		t.Fatalf("start reference worker: %v", err)
	}
	if err := fxApp.Stop(ctx); err != nil {
		t.Fatalf("stop reference worker: %v", err)
	}
}

func TestPostgresPoolStartupFailureIsReported(t *testing.T) {
	var pool *pgxpool.Pool
	fxApp := fx.New(
		fx.Supply(Config{DatabaseURL: "postgres://test:test@127.0.0.1:1/test?sslmode=disable&connect_timeout=1"}),
		fx.Provide(newPostgresPool),
		fx.Invoke(func(*pgxpool.Pool) {}),
		fx.Populate(&pool),
		fx.NopLogger,
	)
	if err := fxApp.Err(); err != nil {
		t.Fatalf("construct pool Fx application: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := fxApp.Start(ctx); err == nil {
		t.Fatal("Fx startup succeeded against an unavailable PostgreSQL endpoint")
	}
	if _, err := pool.Acquire(ctx); err == nil {
		t.Fatal("PostgreSQL pool remained usable after startup failure")
	}
	if err := fxApp.Stop(ctx); err != nil {
		t.Fatalf("stop after failed Fx startup: %v", err)
	}
}

type emptyReferenceRepository struct{}

func (emptyReferenceRepository) WithinTransaction(context.Context, func(application.Transaction) error) error {
	return nil
}

func (emptyReferenceRepository) PendingReferences(context.Context, time.Time, int32) ([]*domain.WagerTransaction, error) {
	return nil, nil
}
