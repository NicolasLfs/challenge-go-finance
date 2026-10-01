package domain

import (
	"errors"
	"testing"
)

func TestWagerTransactionTransitions(t *testing.T) {
	transaction, err := NewExternalWagerTransaction(ExternalWagerInput{
		ID: "tx-1", ExternalID: "ext-1", ProviderID: "provider-1", IdempotencyKey: "key-1",
		WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1",
		Type: WagerTypeBet, Amount: MustMoney("10.00", "BRL"), PayloadHash: "hash-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if transaction.Status() != WagerStatusPending {
		t.Fatalf("initial status = %s", transaction.Status())
	}
	if err := transaction.MarkProcessed(MustMoney("90.00", "BRL")); err != nil {
		t.Fatal(err)
	}
	if transaction.Status() != WagerStatusProcessed || transaction.ResultBalance().Decimal() != "90.00" {
		t.Fatalf("processed transaction = %s %v", transaction.Status(), transaction.ResultBalance())
	}
	if err := transaction.MarkRejected("insufficient_balance"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal transition error = %v", err)
	}
}

func TestWagerTransactionValidation(t *testing.T) {
	base := ExternalWagerInput{ID: "tx-1", ExternalID: "ext-1", ProviderID: "provider-1", IdempotencyKey: "key-1", WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1", Type: WagerTypeBet, Amount: MustMoney("10.00", "BRL"), PayloadHash: "hash-1"}

	for _, wagerType := range []WagerType{WagerTypeBet, WagerTypeWin, WagerTypeRefund, WagerTypeRollback} {
		input := base
		input.Type = wagerType
		if wagerType == WagerTypeRefund || wagerType == WagerTypeRollback {
			input.ReferenceExternalID = "reference-1"
		}
		if _, err := NewExternalWagerTransaction(input); err != nil {
			t.Errorf("valid %s rejected: %v", wagerType, err)
		}
	}

	loss := base
	loss.Type = WagerTypeLoss
	loss.Amount = MustMoney("0.00", "BRL")
	if _, err := NewExternalWagerTransaction(loss); err != nil {
		t.Fatalf("zero LOSS rejected: %v", err)
	}
	invalid := base
	invalid.Type = WagerTypeLoss
	if _, err := NewExternalWagerTransaction(invalid); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("non-zero LOSS error = %v", err)
	}
	invalid = base
	invalid.Type = WagerTypeRefund
	if _, err := NewExternalWagerTransaction(invalid); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("missing reference error = %v", err)
	}
	opening := base
	opening.Type = WagerTypeOpening
	if _, err := NewExternalWagerTransaction(opening); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("external OPENING error = %v", err)
	}
}

func TestWagerTransactionPendingReference(t *testing.T) {
	input := ExternalWagerInput{ID: "tx-1", ExternalID: "ext-1", ProviderID: "provider-1", IdempotencyKey: "key-1", WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1", Type: WagerTypeRefund, Amount: MustMoney("10.00", "BRL"), PayloadHash: "hash-1", ReferenceExternalID: "reference-1"}
	transaction, err := NewExternalWagerTransaction(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.MarkPendingReference(); err != nil || transaction.Status() != WagerStatusPendingReference {
		t.Fatalf("pending reference = %s, %v", transaction.Status(), err)
	}
}

func TestOpeningTransactionAcceptsZero(t *testing.T) {
	transaction, err := NewOpeningWagerTransaction("tx-opening", "wallet-1", "player-1", MustMoney("0.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if transaction.Type() != WagerTypeOpening || transaction.Amount().Decimal() != "0.00" || transaction.Status() != WagerStatusPending {
		t.Fatalf("opening transaction = %#v", transaction)
	}
}
