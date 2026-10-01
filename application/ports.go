package application

import (
	"context"
	"encoding/json"
	"time"

	"challenge/domain"
)

type Repository interface {
	WithinTransaction(context.Context, func(Transaction) error) error
	PendingReferences(context.Context, time.Time, int32) ([]*domain.WagerTransaction, error)
}

type OutboxRepository interface {
	ClaimOutbox(context.Context, time.Time, time.Time, int32) ([]OutboxRecord, error)
	MarkOutboxPublished(context.Context, string, time.Time) error
	MarkOutboxFailed(context.Context, string, time.Time, string) error
}

type Transaction interface {
	BeginInbox(context.Context, InboxMessage) (bool, error)
	CompleteInbox(context.Context, InboxMessage) error
	WalletByPlayerCurrency(context.Context, string, string) (*domain.Wallet, error)
	WalletByID(context.Context, string) (*domain.Wallet, error)
	WalletForUpdate(context.Context, string) (*domain.Wallet, error)
	CreateWallet(context.Context, *domain.Wallet) error
	CreateWagerTransaction(context.Context, *domain.WagerTransaction) error
	WagerByIdempotencyKey(context.Context, string, string) (*domain.WagerTransaction, error)
	WagerByExternalID(context.Context, string, string) (*domain.WagerTransaction, error)
	WagerByID(context.Context, string) (*domain.WagerTransaction, error)
	HasSuccessfulReversal(context.Context, string) (bool, error)
	LedgerEntries(context.Context, string) ([]LedgerEntry, error)
	LedgerPage(context.Context, string, time.Time, string, int32) ([]LedgerEntry, error)
	UpdateWallet(context.Context, *domain.Wallet) error
	UpdateWagerTransaction(context.Context, *domain.WagerTransaction) error
	AppendLedger(context.Context, LedgerEntry) error
	AppendOutbox(context.Context, OutboxEvent) error
}

type InboxMessage struct {
	ConsumerName, MessageID, MessageHash string
	Payload                              []byte
	Completed                            bool
}

type IDGenerator interface {
	NewID() (string, error)
}

type IDGeneratorFunc func() (string, error)

func (generator IDGeneratorFunc) NewID() (string, error) { return generator() }

type LedgerEntry struct {
	ID, WalletID, TransactionID string
	Direction                   domain.LedgerDirection
	Amount, BalanceBefore       domain.Money
	BalanceAfter                domain.Money
	CreatedAt                   time.Time
}

type OutboxEvent struct {
	ID, AggregateID string
	Type            domain.EventType
	Version         int
	OccurredAt      time.Time
	Payload         json.RawMessage
}

type OutboxRecord struct {
	ID, AggregateID string
	Type            domain.EventType
	Version         int
	OccurredAt      time.Time
	Payload         json.RawMessage
	Attempts        int32
	CreatedAt       time.Time
}
