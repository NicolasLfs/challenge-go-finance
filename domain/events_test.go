package domain

import (
	"testing"
	"time"
)

func TestWagerTransactionEvents(t *testing.T) {
	processed, err := NewWagerTransactionProcessedEvent(WagerTransactionProcessedInput{
		EventID: "event-1", TransactionID: "tx-1", WalletID: "wallet-1", ProviderID: "provider-1",
		TransactionType: WagerTypeBet, Amount: MustMoney("10.00", "BRL"), OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if processed.Type() != EventTypeWagerTransactionProcessed || processed.Version() != 1 || processed.Amount() != "10.00" || processed.Currency() != "BRL" || processed.OccurredAt().Location() != time.UTC {
		t.Fatalf("processed event = %#v", processed)
	}

	rejected, err := NewWagerTransactionRejectedEvent("event-2", "tx-2", "wallet-1", "provider-1", "insufficient_balance", time.Now())
	if err != nil || rejected.Type() != EventTypeWagerTransactionRejected || rejected.FailureCode() != "insufficient_balance" {
		t.Fatalf("rejected event = %#v, %v", rejected, err)
	}
	pending, err := NewWagerTransactionPendingReferenceEvent("event-3", "tx-3", "wallet-1", "provider-1", "reference-1", time.Now())
	if err != nil || pending.Type() != EventTypeWagerTransactionPendingReference || pending.ReferenceExternalID() != "reference-1" {
		t.Fatalf("pending event = %#v, %v", pending, err)
	}
}

func TestOpeningProcessedEventDoesNotRequireProviderID(t *testing.T) {
	event, err := NewWagerTransactionProcessedEvent(WagerTransactionProcessedInput{
		EventID: "event-opening", TransactionID: "opening-1", WalletID: "wallet-1",
		TransactionType: WagerTypeOpening, Amount: MustMoney("100.00", "BRL"),
		OccurredAt: time.Now(),
	})
	if err != nil || event.Type() != EventTypeWagerTransactionProcessed {
		t.Fatalf("opening processed event = %#v, error %v", event, err)
	}
}

func TestWalletBalanceChangedEvent(t *testing.T) {
	event, err := NewWalletBalanceChangedEvent(WalletBalanceChangedInput{
		EventID: "event-1", TransactionID: "tx-1", WalletID: "wallet-1", Direction: LedgerDirectionDebit,
		Amount: MustMoney("10.00", "BRL"), BalanceBefore: MustMoney("20.00", "BRL"), BalanceAfter: MustMoney("10.00", "BRL"), WalletVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.Type() != EventTypeWalletBalanceChanged || event.Amount() != "10.00" || event.BalanceBefore() != "20.00" || event.BalanceAfter() != "10.00" || event.WalletVersion() != 2 {
		t.Fatalf("balance event = %#v", event)
	}
}
