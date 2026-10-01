package domain

import (
	"fmt"
	"time"
)

type Wallet struct {
	id        string
	playerID  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(id, playerID string, initialBalance Money) (*Wallet, error) {
	now := time.Now().UTC()
	return newWallet(id, playerID, initialBalance, 1, now, now)
}

func RehydrateWallet(id, playerID string, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	return newWallet(id, playerID, balance, version, createdAt, updatedAt)
}

func newWallet(id, playerID string, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == "" || playerID == "" || balance.Currency() == "" || balance.MinorUnits() < 0 || version < 1 || createdAt.IsZero() || updatedAt.IsZero() {
		return nil, ErrInvalidWallet
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (wallet *Wallet) ID() string           { return wallet.id }
func (wallet *Wallet) PlayerID() string     { return wallet.playerID }
func (wallet *Wallet) Balance() Money       { return wallet.balance }
func (wallet *Wallet) Version() int64       { return wallet.version }
func (wallet *Wallet) CreatedAt() time.Time { return wallet.createdAt }
func (wallet *Wallet) UpdatedAt() time.Time { return wallet.updatedAt }

func (wallet *Wallet) Debit(amount Money) error {
	if err := wallet.validateAmount(amount); err != nil {
		return err
	}
	if amount.GreaterThan(wallet.balance) {
		return ErrInsufficientBalance
	}
	return wallet.apply(amount, false)
}

func (wallet *Wallet) Credit(amount Money) error {
	if err := wallet.validateAmount(amount); err != nil {
		return err
	}
	return wallet.apply(amount, true)
}

func (wallet *Wallet) validateAmount(amount Money) error {
	if amount.MinorUnits() < 0 {
		return fmt.Errorf("%w: negative balance operation", ErrInvalidMoney)
	}
	if wallet.balance.Currency() != amount.Currency() {
		return ErrCurrencyMismatch
	}
	return nil
}

func (wallet *Wallet) apply(amount Money, credit bool) error {
	var next Money
	var err error
	if credit {
		next, err = wallet.balance.Add(amount)
	} else {
		next, err = wallet.balance.Subtract(amount)
	}
	if err != nil {
		return err
	}
	if amount.MinorUnits() == 0 {
		return nil
	}
	wallet.balance = next
	wallet.version++
	wallet.updatedAt = time.Now().UTC()
	return nil
}
