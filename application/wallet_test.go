package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"challenge/domain"
)

func TestOpenWalletCreatesOpeningMovementOnlyForPositiveBalance(t *testing.T) {
	for _, test := range []struct {
		name             string
		amount           string
		wantTransactions int
		wantLedger       int
		wantEvents       int
	}{
		{name: "positive balance", amount: "100.00", wantTransactions: 1, wantLedger: 1, wantEvents: 2},
		{name: "zero balance", amount: "0.00"},
	} {
		t.Run(test.name, func(t *testing.T) {
			memory := &memoryRepository{}
			service := NewWalletService(memory, sequenceIDs())
			amount := mustParseMoney(t, test.amount, "BRL")

			wallet, err := service.OpenWallet(context.Background(), OpenWalletCommand{
				PlayerID: "player-1",
				Balance:  amount,
			})
			if err != nil {
				t.Fatalf("OpenWallet() error = %v", err)
			}
			if wallet.Balance() != amount || wallet.Version() != 1 {
				t.Fatalf("wallet state = balance %v, version %d", wallet.Balance(), wallet.Version())
			}
			if got := len(memory.transactions); got != test.wantTransactions {
				t.Errorf("transactions = %d, want %d", got, test.wantTransactions)
			}
			if got := len(memory.ledger); got != test.wantLedger {
				t.Errorf("ledger entries = %d, want %d", got, test.wantLedger)
			}
			if got := len(memory.events); got != test.wantEvents {
				t.Errorf("outbox events = %d, want %d", got, test.wantEvents)
			}
			if test.wantEvents > 0 {
				var payload struct {
					WalletVersion int64 `json:"walletVersion"`
					Money         struct {
						Amount string `json:"amount"`
					} `json:"money"`
					BalanceBefore struct {
						Amount string `json:"amount"`
					} `json:"balanceBefore"`
					BalanceAfter struct {
						Amount string `json:"amount"`
					} `json:"balanceAfter"`
				}
				if err := json.Unmarshal(memory.events[1].Payload, &payload); err != nil {
					t.Fatalf("decode opening balance event: %v", err)
				}
				if payload.WalletVersion != 1 || payload.Money.Amount != test.amount || payload.BalanceBefore.Amount != "0.00" || payload.BalanceAfter.Amount != test.amount {
					t.Errorf("opening balance event = %#v", payload)
				}
			}
		})
	}
}

func TestOpenWalletRejectsExistingPlayerCurrency(t *testing.T) {
	memory := &memoryRepository{existingWallet: true}
	service := NewWalletService(memory, sequenceIDs())

	_, err := service.OpenWallet(context.Background(), OpenWalletCommand{
		PlayerID: "player-1",
		Balance:  mustParseMoney(t, "0.00", "BRL"),
	})
	if !errors.Is(err, ErrWalletConflict) {
		t.Fatalf("OpenWallet() error = %v, want ErrWalletConflict", err)
	}
}

type memoryRepository struct {
	existingWallet bool
	wallet         *domain.Wallet
	transactions   []*domain.WagerTransaction
	ledger         []LedgerEntry
	events         []OutboxEvent
	inbox          map[string]InboxMessage
}

func (repository *memoryRepository) WithinTransaction(_ context.Context, operation func(Transaction) error) error {
	return operation(repository)
}

func (repository *memoryRepository) PendingReferences(_ context.Context, due time.Time, limit int32) ([]*domain.WagerTransaction, error) {
	var pending []*domain.WagerTransaction
	for _, wager := range repository.transactions {
		if wager.Status() == domain.WagerStatusPendingReference && !wager.NextReferenceAttemptAt().After(due) {
			pending = append(pending, wager)
			if len(pending) == int(limit) {
				break
			}
		}
	}
	return pending, nil
}

func (repository *memoryRepository) WalletByPlayerCurrency(context.Context, string, string) (*domain.Wallet, error) {
	if repository.existingWallet || repository.wallet != nil {
		return repository.wallet, nil
	}
	return nil, ErrNotFound
}

func (repository *memoryRepository) WalletByID(_ context.Context, walletID string) (*domain.Wallet, error) {
	if repository.wallet == nil || repository.wallet.ID() != walletID {
		return nil, ErrNotFound
	}
	return repository.wallet, nil
}

func (repository *memoryRepository) WalletForUpdate(context.Context, string) (*domain.Wallet, error) {
	if repository.wallet == nil {
		return nil, ErrNotFound
	}
	return repository.wallet, nil
}

func (repository *memoryRepository) CreateWallet(_ context.Context, wallet *domain.Wallet) error {
	repository.wallet = wallet
	return nil
}

func (repository *memoryRepository) CreateWagerTransaction(_ context.Context, wager *domain.WagerTransaction) error {
	repository.transactions = append(repository.transactions, wager)
	return nil
}

func (repository *memoryRepository) WagerByIdempotencyKey(_ context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	for _, wager := range repository.transactions {
		if wager.ProviderID() == providerID && wager.IdempotencyKey() == key {
			return wager, nil
		}
	}
	return nil, ErrNotFound
}

func (repository *memoryRepository) WagerByExternalID(_ context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	for _, wager := range repository.transactions {
		if wager.ProviderID() == providerID && wager.ExternalID() == externalID {
			return wager, nil
		}
	}
	return nil, ErrNotFound
}

func (repository *memoryRepository) WagerByID(_ context.Context, id string) (*domain.WagerTransaction, error) {
	for _, wager := range repository.transactions {
		if wager.ID() == id {
			return wager, nil
		}
	}
	return nil, ErrNotFound
}

func (repository *memoryRepository) HasSuccessfulReversal(_ context.Context, referenceID string) (bool, error) {
	for _, wager := range repository.transactions {
		data := wager.Data()
		if data.ResolvedReferenceID == referenceID && data.Status == domain.WagerStatusProcessed {
			return true, nil
		}
	}
	return false, nil
}

func (repository *memoryRepository) LedgerEntries(_ context.Context, walletID string) ([]LedgerEntry, error) {
	var entries []LedgerEntry
	for _, entry := range repository.ledger {
		if entry.WalletID == walletID {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (repository *memoryRepository) LedgerPage(_ context.Context, walletID string, before time.Time, cursorID string, limit int32) ([]LedgerEntry, error) {
	entries, err := repository.LedgerEntries(context.Background(), walletID)
	if err != nil {
		return nil, err
	}
	filtered := make([]LedgerEntry, 0, len(entries))
	for _, entry := range entries {
		if before.IsZero() || entry.CreatedAt.Before(before) || (entry.CreatedAt.Equal(before) && entry.ID < cursorID) {
			filtered = append(filtered, entry)
		}
	}
	if len(filtered) > int(limit) {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func (repository *memoryRepository) UpdateWallet(context.Context, *domain.Wallet) error { return nil }

func (repository *memoryRepository) UpdateWagerTransaction(context.Context, *domain.WagerTransaction) error {
	return nil
}

func (repository *memoryRepository) AppendLedger(_ context.Context, entry LedgerEntry) error {
	repository.ledger = append(repository.ledger, entry)
	return nil
}

func (repository *memoryRepository) AppendOutbox(_ context.Context, event OutboxEvent) error {
	repository.events = append(repository.events, event)
	return nil
}

func (repository *memoryRepository) BeginInbox(_ context.Context, message InboxMessage) (bool, error) {
	if repository.inbox == nil {
		repository.inbox = make(map[string]InboxMessage)
	}
	key := message.ConsumerName + ":" + message.MessageID
	previous, exists := repository.inbox[key]
	if !exists {
		repository.inbox[key] = message
		return false, nil
	}
	if previous.MessageHash != message.MessageHash {
		return false, ErrInboxConflict
	}
	if !previous.Completed {
		return false, ErrInboxIncomplete
	}
	return true, nil
}

func (repository *memoryRepository) CompleteInbox(_ context.Context, message InboxMessage) error {
	key := message.ConsumerName + ":" + message.MessageID
	stored, exists := repository.inbox[key]
	if !exists {
		return ErrNotFound
	}
	stored.Completed = true
	repository.inbox[key] = stored
	return nil
}

func sequenceIDs() IDGenerator {
	var next int
	return IDGeneratorFunc(func() (string, error) {
		next++
		return fmt.Sprintf("id-%d", next), nil
	})
}

func mustParseMoney(t *testing.T, amount, currency string) domain.Money {
	t.Helper()
	money, err := domain.ParseMoney(amount, currency)
	if err != nil {
		t.Fatalf("ParseMoney(%q, %q): %v", amount, currency, err)
	}
	return money
}
