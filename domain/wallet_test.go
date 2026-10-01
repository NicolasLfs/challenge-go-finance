package domain

import (
	"errors"
	"testing"
	"time"
)

func TestWalletCreationRehydrationAndBalanceOperations(t *testing.T) {
	wallet, err := NewWallet("wallet-1", "player-1", MustMoney("100.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 1 || wallet.Balance().Decimal() != "100.00" {
		t.Fatalf("initial wallet = balance %s version %d", wallet.Balance().Decimal(), wallet.Version())
	}

	if err := wallet.Debit(MustMoney("40.00", "BRL")); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance().Decimal() != "60.00" || wallet.Version() != 2 {
		t.Fatalf("after debit = balance %s version %d", wallet.Balance().Decimal(), wallet.Version())
	}
	if err := wallet.Credit(MustMoney("10.00", "BRL")); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance().Decimal() != "70.00" || wallet.Version() != 3 {
		t.Fatalf("after credit = balance %s version %d", wallet.Balance().Decimal(), wallet.Version())
	}
	if err := wallet.Credit(MustMoney("0.00", "BRL")); err != nil {
		t.Fatal(err)
	}
	if wallet.Version() != 3 {
		t.Fatalf("zero credit changed version to %d", wallet.Version())
	}
	if err := wallet.Debit(MustMoney("71.00", "BRL")); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("insufficient debit error = %v", err)
	}
	if err := wallet.Debit(MustMoney("1.00", "USD")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("currency mismatch error = %v", err)
	}

	rehydrated, err := RehydrateWallet("wallet-1", "player-1", MustMoney("70.00", "BRL"), 3, time.Unix(1, 0), time.Unix(2, 0))
	if err != nil || rehydrated.Version() != 3 || rehydrated.Balance().Decimal() != "70.00" {
		t.Fatalf("RehydrateWallet() = %v, %v", rehydrated, err)
	}
}

func TestWalletRejectsInvalidState(t *testing.T) {
	if _, err := NewWallet("", "player-1", MustMoney("1.00", "BRL")); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("invalid identity error = %v", err)
	}
	if _, err := RehydrateWallet("wallet-1", "player-1", MustMoney("1.00", "BRL"), 0, time.Unix(1, 0), time.Unix(2, 0)); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("invalid version error = %v", err)
	}
	negative, err := MoneyFromMinorUnits(-1, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RehydrateWallet("wallet-1", "player-1", negative, 1, time.Unix(1, 0), time.Unix(2, 0)); !errors.Is(err, ErrInvalidWallet) {
		t.Fatalf("negative balance error = %v", err)
	}
}
