package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"challenge/domain"
)

func TestProcessWagerPersistsDebitAndReplaysWithoutAnotherLedgerEntry(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	command := wagerCommand(t, "external-1", "key-1", "25.00")

	first, err := service.Process(context.Background(), command)
	if err != nil {
		t.Fatalf("first Process() error = %v", err)
	}
	second, err := service.Process(context.Background(), command)
	if err != nil {
		t.Fatalf("replay Process() error = %v", err)
	}
	if first.Status != domain.WagerStatusProcessed || second.Status != domain.WagerStatusProcessed || !second.IdempotentReplay {
		t.Fatalf("first/replay status = %s/%s, replay flag = %v", first.Status, second.Status, second.IdempotentReplay)
	}
	if first.Balance.Decimal() != "75.00" || second.Balance.Decimal() != "75.00" {
		t.Fatalf("first/replay balance = %s/%s, want 75.00/75.00", first.Balance, second.Balance)
	}
	if len(memory.ledger) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(memory.ledger))
	}
	if len(memory.transactions) != 1 {
		t.Fatalf("transactions = %d, want 1", len(memory.transactions))
	}
	var balanceEvent struct {
		Direction string `json:"direction"`
		Money     struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		BalanceBefore struct {
			Amount string `json:"amount"`
		} `json:"balanceBefore"`
		BalanceAfter struct {
			Amount string `json:"amount"`
		} `json:"balanceAfter"`
		WalletVersion int64 `json:"walletVersion"`
	}
	if err := json.Unmarshal(memory.events[1].Payload, &balanceEvent); err != nil {
		t.Fatalf("decode wallet balance event snapshot: %v", err)
	}
	if balanceEvent.Direction != "DEBIT" || balanceEvent.Money.Amount != "25.00" || balanceEvent.Money.Currency != "BRL" || balanceEvent.BalanceBefore.Amount != "100.00" || balanceEvent.BalanceAfter.Amount != "75.00" || balanceEvent.WalletVersion != 2 {
		t.Fatalf("wallet balance event data = %#v", balanceEvent)
	}
}

func TestProcessMessageUsesInboxAndRejectsMessageIDHashConflicts(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	command := wagerCommand(t, "external-1", "key-1", "25.00")
	inbox := InboxMessage{ConsumerName: "wager-transactions", MessageID: "message-1", MessageHash: "hash-1", Payload: []byte(`{"messageId":"message-1"}`)}

	first, err := service.ProcessMessage(context.Background(), command, inbox)
	if err != nil || first.Status != domain.WagerStatusProcessed || first.IdempotentReplay {
		t.Fatalf("first message result = %#v, error %v", first, err)
	}
	second, err := service.ProcessMessage(context.Background(), command, inbox)
	if err != nil || second.Status != domain.WagerStatusProcessed || !second.IdempotentReplay {
		t.Fatalf("message replay result = %#v, error %v", second, err)
	}
	if len(memory.ledger) != 1 || len(memory.transactions) != 1 || len(memory.inbox) != 1 {
		t.Fatalf("persisted entries = ledger %d, wagers %d, inbox %d; want 1 each", len(memory.ledger), len(memory.transactions), len(memory.inbox))
	}

	inbox.MessageHash = "different-hash"
	if _, err := service.ProcessMessage(context.Background(), command, inbox); !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("same message ID with another hash error = %v, want ErrInboxConflict", err)
	}
	if len(memory.ledger) != 1 {
		t.Fatalf("conflicting message changed ledger: %d entries", len(memory.ledger))
	}
}

func TestProcessWagerRejectsIdempotencyKeyWithDifferentPayload(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	if _, err := service.Process(context.Background(), wagerCommand(t, "external-1", "key-1", "25.00")); err != nil {
		t.Fatalf("first Process() error = %v", err)
	}
	_, err := service.Process(context.Background(), wagerCommand(t, "external-1", "key-1", "30.00"))
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting Process() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestProcessWagerRejectsExternalIdentityWithAnotherIdempotencyKey(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	if _, err := service.Process(context.Background(), wagerCommand(t, "external-1", "key-1", "25.00")); err != nil {
		t.Fatalf("first Process() error = %v", err)
	}
	_, err := service.Process(context.Background(), wagerCommand(t, "external-1", "key-2", "25.00"))
	if !errors.Is(err, ErrExternalTransactionConflict) {
		t.Fatalf("conflicting external identity error = %v, want ErrExternalTransactionConflict", err)
	}
}

func TestWagerHashDoesNotIncludeIdempotencyKey(t *testing.T) {
	first := wagerCommand(t, "external-1", "key-a", "25.00").Wager
	second := wagerCommand(t, "external-1", "key-b", "25.00").Wager
	firstHash, err := CanonicalWagerHash(first)
	if err != nil {
		t.Fatalf("CanonicalWagerHash(first): %v", err)
	}
	secondHash, err := CanonicalWagerHash(second)
	if err != nil {
		t.Fatalf("CanonicalWagerHash(second): %v", err)
	}
	if firstHash != secondHash {
		t.Fatalf("hash depends on idempotency key: %s != %s", firstHash, secondHash)
	}
}

func TestPendingRefundResumesAfterReferencedBetArrives(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	bet := wagerCommand(t, "bet-1", "bet-key", "25.00")
	if _, err := service.Process(context.Background(), bet); err != nil {
		t.Fatalf("process referenced bet: %v", err)
	}
	refund := wagerCommand(t, "refund-1", "refund-key", "25.00")
	refund.Wager.Type = domain.WagerTypeRefund
	refund.Wager.ReferenceExternalID = "bet-later"
	pending, err := service.Process(context.Background(), refund)
	if err != nil {
		t.Fatalf("process refund before reference: %v", err)
	}
	if pending.Status != domain.WagerStatusPendingReference || pending.Balance.Decimal() != "75.00" {
		t.Fatalf("pending refund result = status %s, balance %s", pending.Status, pending.Balance)
	}

	lateBet := wagerCommand(t, "bet-later", "late-bet-key", "25.00")
	if _, err := service.Process(context.Background(), lateBet); err != nil {
		t.Fatalf("process late referenced bet: %v", err)
	}
	replayedPending, err := service.Process(context.Background(), refund)
	if err != nil {
		t.Fatalf("replay pending refund: %v", err)
	}
	if !replayedPending.IdempotentReplay || replayedPending.Balance.Decimal() != "75.00" {
		t.Fatalf("pending replay = replay %v, balance %s, want replay true and 75.00", replayedPending.IdempotentReplay, replayedPending.Balance)
	}
	service.now = func() time.Time { return time.Now().UTC().Add(time.Hour) }
	if err := service.RetryPendingReferences(context.Background(), 10); err != nil {
		t.Fatalf("retry pending references: %v", err)
	}
	if got := memory.wallet.Balance().Decimal(); got != "75.00" {
		t.Fatalf("wallet balance after refund resolution = %s, want 75.00", got)
	}
	if len(memory.ledger) != 3 {
		t.Fatalf("ledger entries = %d, want 3", len(memory.ledger))
	}
}

func TestReconciliationComparesStoredBalanceWithLedgerWithoutMutation(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	zero := mustParseMoney(t, "0.00", "BRL")
	balance := mustParseMoney(t, "100.00", "BRL")
	memory.ledger = append(memory.ledger, LedgerEntry{
		ID: "opening-ledger", WalletID: "wallet-1", TransactionID: "opening",
		Direction: domain.LedgerDirectionCredit, Amount: balance, BalanceBefore: zero,
		BalanceAfter: balance,
	})
	service := NewReconciliationService(memory)
	before := memory.wallet.Balance()

	result, err := service.Reconcile(context.Background(), "wallet-1")
	if err != nil {
		t.Fatalf("Reconcile(): %v", err)
	}
	if result.Difference.MinorUnits() != 0 || result.LedgerBalance.Decimal() != "100.00" || result.EntryCount != 1 {
		t.Fatalf("reconciliation result = difference %s, ledger %s, entries %d", result.Difference, result.LedgerBalance, result.EntryCount)
	}
	if !memory.wallet.Balance().Equal(before) {
		t.Fatalf("reconciliation mutated wallet from %s to %s", before, memory.wallet.Balance())
	}
}

func TestLossProcessesWithoutBalanceOrLedgerChange(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	command := wagerCommand(t, "loss-1", "loss-key", "0.00")
	command.Wager.Type = domain.WagerTypeLoss

	result, err := service.Process(context.Background(), command)
	if err != nil || result.Status != domain.WagerStatusProcessed {
		t.Fatalf("LOSS result = %#v, error %v", result, err)
	}
	if result.Balance.Decimal() != "100.00" || len(memory.ledger) != 0 || len(memory.events) != 1 {
		t.Fatalf("LOSS side effects = balance %s, ledger %d, events %d", result.Balance, len(memory.ledger), len(memory.events))
	}
}

func TestBetCannotBeRefundedAndRolledBackTwice(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	bet := wagerCommand(t, "bet-1", "bet-key", "25.00")
	if _, err := service.Process(context.Background(), bet); err != nil {
		t.Fatalf("process bet: %v", err)
	}
	refund := wagerCommand(t, "refund-1", "refund-key", "25.00")
	refund.Wager.Type = domain.WagerTypeRefund
	refund.Wager.ReferenceExternalID = "bet-1"
	if result, err := service.Process(context.Background(), refund); err != nil || result.Status != domain.WagerStatusProcessed {
		t.Fatalf("process refund = %#v, error %v", result, err)
	}
	rollback := wagerCommand(t, "rollback-bet", "rollback-key", "25.00")
	rollback.Wager.Type = domain.WagerTypeRollback
	rollback.Wager.ReferenceExternalID = "bet-1"
	result, err := service.Process(context.Background(), rollback)
	if err != nil || result.Status != domain.WagerStatusRejected || result.FailureCode != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("rollback after refund = %#v, error %v", result, err)
	}
	if memory.wallet.Balance().Decimal() != "100.00" || len(memory.ledger) != 2 {
		t.Fatalf("duplicate reversal changed balance/ledger: %s/%d", memory.wallet.Balance(), len(memory.ledger))
	}
}

func TestRollbackOfRefundReversesTheRefund(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	bet := wagerCommand(t, "bet-1", "bet-key", "25.00")
	if _, err := service.Process(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	refund := wagerCommand(t, "refund-1", "refund-key", "25.00")
	refund.Wager.Type = domain.WagerTypeRefund
	refund.Wager.ReferenceExternalID = "bet-1"
	if _, err := service.Process(context.Background(), refund); err != nil {
		t.Fatal(err)
	}
	rollback := wagerCommand(t, "rollback-refund", "rollback-key", "25.00")
	rollback.Wager.Type = domain.WagerTypeRollback
	rollback.Wager.ReferenceExternalID = "refund-1"
	result, err := service.Process(context.Background(), rollback)
	if err != nil || result.Status != domain.WagerStatusProcessed || result.Balance.Decimal() != "75.00" {
		t.Fatalf("rollback of refund = %#v, error %v", result, err)
	}
}

func TestRollbackInsufficientBalanceHasDistinctFailureCode(t *testing.T) {
	memory := seededMemoryRepository(t, "0.00")
	service := NewWagerService(memory, sequenceIDs())
	win := wagerCommand(t, "win-1", "win-key", "25.00")
	win.Wager.Type = domain.WagerTypeWin
	if _, err := service.Process(context.Background(), win); err != nil {
		t.Fatal(err)
	}
	bet := wagerCommand(t, "bet-1", "bet-key", "25.00")
	if _, err := service.Process(context.Background(), bet); err != nil {
		t.Fatal(err)
	}
	rollback := wagerCommand(t, "rollback-win", "rollback-key", "25.00")
	rollback.Wager.Type = domain.WagerTypeRollback
	rollback.Wager.ReferenceExternalID = "win-1"
	result, err := service.Process(context.Background(), rollback)
	if err != nil || result.Status != domain.WagerStatusRejected || result.FailureCode != "REVERSAL_INSUFFICIENT_BALANCE" {
		t.Fatalf("rollback without funds = %#v, error %v", result, err)
	}
}

func TestMissingReferenceExpiresAsTerminalRejection(t *testing.T) {
	memory := seededMemoryRepository(t, "100.00")
	service := NewWagerService(memory, sequenceIDs())
	refund := wagerCommand(t, "refund-1", "refund-key", "25.00")
	refund.Wager.Type = domain.WagerTypeRefund
	refund.Wager.ReferenceExternalID = "never-arrives"
	pending, err := service.Process(context.Background(), refund)
	if err != nil || pending.Status != domain.WagerStatusPendingReference {
		t.Fatalf("initial refund = %#v, error %v", pending, err)
	}
	service.now = func() time.Time { return time.Now().UTC().Add(25 * time.Hour) }
	if err := service.RetryPendingReferences(context.Background(), 10); err != nil {
		t.Fatalf("expire missing reference: %v", err)
	}
	stored, err := memory.WagerByID(context.Background(), pending.TransactionID)
	if err != nil || stored.Status() != domain.WagerStatusRejected || stored.FailureCode() != "REFERENCE_NOT_FOUND" {
		t.Fatalf("expired reference transaction = %#v, error %v", stored, err)
	}
}

func TestReferencePendingAndRejectedHaveDistinctTerminalBehavior(t *testing.T) {
	t.Run("pending reference waits", func(t *testing.T) {
		memory := seededMemoryRepository(t, "100.00")
		service := NewWagerService(memory, sequenceIDs())
		first := wagerCommand(t, "refund-1", "refund-key-1", "25.00")
		first.Wager.Type = domain.WagerTypeRefund
		first.Wager.ReferenceExternalID = "missing-bet"
		if _, err := service.Process(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		second := wagerCommand(t, "refund-2", "refund-key-2", "25.00")
		second.Wager.Type = domain.WagerTypeRefund
		second.Wager.ReferenceExternalID = "refund-1"
		result, err := service.Process(context.Background(), second)
		if err != nil || result.Status != domain.WagerStatusPendingReference {
			t.Fatalf("refund referencing a pending refund = %#v, error %v", result, err)
		}
	})

	t.Run("rejected reference is terminal", func(t *testing.T) {
		memory := seededMemoryRepository(t, "0.00")
		service := NewWagerService(memory, sequenceIDs())
		bet := wagerCommand(t, "bet-1", "bet-key", "25.00")
		if result, err := service.Process(context.Background(), bet); err != nil || result.Status != domain.WagerStatusRejected {
			t.Fatalf("insufficient bet = %#v, error %v", result, err)
		}
		refund := wagerCommand(t, "refund-1", "refund-key", "25.00")
		refund.Wager.Type = domain.WagerTypeRefund
		refund.Wager.ReferenceExternalID = "bet-1"
		result, err := service.Process(context.Background(), refund)
		if err != nil || result.Status != domain.WagerStatusRejected || result.FailureCode != "REFERENCE_NOT_PROCESSED" {
			t.Fatalf("refund of rejected bet = %#v, error %v", result, err)
		}
	})
}

func seededMemoryRepository(t *testing.T, balance string) *memoryRepository {
	t.Helper()
	memory := &memoryRepository{}
	amount := mustParseMoney(t, balance, "BRL")
	wallet, err := domain.NewWallet("wallet-1", "player-1", amount)
	if err != nil {
		t.Fatalf("NewWallet(): %v", err)
	}
	memory.wallet = wallet
	return memory
}

func wagerCommand(t *testing.T, externalID, idempotencyKey, amount string) ProcessWagerCommand {
	t.Helper()
	return ProcessWagerCommand{Wager: domain.ExternalWagerInput{
		ID: "", ExternalID: externalID, ProviderID: "provider-1", IdempotencyKey: idempotencyKey,
		WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1",
		Type: domain.WagerTypeBet, Amount: mustParseMoney(t, amount, "BRL"),
	}}
}
