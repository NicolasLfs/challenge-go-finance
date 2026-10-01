package domain

import (
	"errors"
	"time"
)

const eventVersion = 1

type EventType string

const (
	EventTypeWagerTransactionProcessed        EventType = "WagerTransactionProcessed"
	EventTypeWagerTransactionRejected         EventType = "WagerTransactionRejected"
	EventTypeWagerTransactionPendingReference EventType = "WagerTransactionPendingReference"
	EventTypeWalletBalanceChanged             EventType = "WalletBalanceChanged"
)

type Event interface {
	ID() string
	Type() EventType
	Version() int
	OccurredAt() time.Time
}

type WagerTransactionProcessedInput struct {
	EventID, TransactionID, WalletID, ProviderID string
	TransactionType                              WagerType
	Amount                                       Money
	OccurredAt                                   time.Time
}

type WagerTransactionProcessedEvent struct {
	eventID, transactionID, walletID, providerID string
	transactionType                              WagerType
	amount, currency                             string
	occurredAt                                   time.Time
}

func NewWagerTransactionProcessedEvent(input WagerTransactionProcessedInput) (WagerTransactionProcessedEvent, error) {
	if input.EventID == "" || input.TransactionID == "" || input.WalletID == "" || input.TransactionType == "" || (input.ProviderID == "" && input.TransactionType != WagerTypeOpening) || input.Amount.Currency() == "" || input.Amount.MinorUnits() < 0 || input.OccurredAt.IsZero() {
		return WagerTransactionProcessedEvent{}, ErrInvalidWager
	}
	return WagerTransactionProcessedEvent{eventID: input.EventID, transactionID: input.TransactionID, walletID: input.WalletID, providerID: input.ProviderID, transactionType: input.TransactionType, amount: input.Amount.Decimal(), currency: input.Amount.Currency(), occurredAt: input.OccurredAt.UTC()}, nil
}

func (event WagerTransactionProcessedEvent) ID() string { return event.eventID }
func (event WagerTransactionProcessedEvent) Type() EventType {
	return EventTypeWagerTransactionProcessed
}
func (event WagerTransactionProcessedEvent) Version() int          { return eventVersion }
func (event WagerTransactionProcessedEvent) OccurredAt() time.Time { return event.occurredAt }
func (event WagerTransactionProcessedEvent) TransactionID() string { return event.transactionID }
func (event WagerTransactionProcessedEvent) Amount() string        { return event.amount }
func (event WagerTransactionProcessedEvent) Currency() string      { return event.currency }

type WagerTransactionRejectedEvent struct {
	eventID, transactionID, walletID, providerID, failureCode string
	occurredAt                                                time.Time
}

func NewWagerTransactionRejectedEvent(eventID, transactionID, walletID, providerID, failureCode string, occurredAt time.Time) (WagerTransactionRejectedEvent, error) {
	if eventID == "" || transactionID == "" || walletID == "" || providerID == "" || failureCode == "" || occurredAt.IsZero() {
		return WagerTransactionRejectedEvent{}, ErrInvalidWager
	}
	return WagerTransactionRejectedEvent{eventID: eventID, transactionID: transactionID, walletID: walletID, providerID: providerID, failureCode: failureCode, occurredAt: occurredAt.UTC()}, nil
}

func (event WagerTransactionRejectedEvent) ID() string            { return event.eventID }
func (event WagerTransactionRejectedEvent) Type() EventType       { return EventTypeWagerTransactionRejected }
func (event WagerTransactionRejectedEvent) Version() int          { return eventVersion }
func (event WagerTransactionRejectedEvent) OccurredAt() time.Time { return event.occurredAt }
func (event WagerTransactionRejectedEvent) FailureCode() string   { return event.failureCode }

type WagerTransactionPendingReferenceEvent struct {
	eventID, transactionID, walletID, providerID, referenceExternalID string
	occurredAt                                                        time.Time
}

func NewWagerTransactionPendingReferenceEvent(eventID, transactionID, walletID, providerID, referenceExternalID string, occurredAt time.Time) (WagerTransactionPendingReferenceEvent, error) {
	if eventID == "" || transactionID == "" || walletID == "" || providerID == "" || referenceExternalID == "" || occurredAt.IsZero() {
		return WagerTransactionPendingReferenceEvent{}, ErrInvalidWager
	}
	return WagerTransactionPendingReferenceEvent{eventID: eventID, transactionID: transactionID, walletID: walletID, providerID: providerID, referenceExternalID: referenceExternalID, occurredAt: occurredAt.UTC()}, nil
}

func (event WagerTransactionPendingReferenceEvent) ID() string { return event.eventID }
func (event WagerTransactionPendingReferenceEvent) Type() EventType {
	return EventTypeWagerTransactionPendingReference
}
func (event WagerTransactionPendingReferenceEvent) Version() int          { return eventVersion }
func (event WagerTransactionPendingReferenceEvent) OccurredAt() time.Time { return event.occurredAt }
func (event WagerTransactionPendingReferenceEvent) ReferenceExternalID() string {
	return event.referenceExternalID
}

type LedgerDirection string

const (
	LedgerDirectionDebit  LedgerDirection = "DEBIT"
	LedgerDirectionCredit LedgerDirection = "CREDIT"
)

type WalletBalanceChangedInput struct {
	EventID, TransactionID, WalletID    string
	Direction                           LedgerDirection
	Amount, BalanceBefore, BalanceAfter Money
	WalletVersion                       int64
}

type WalletBalanceChangedEvent struct {
	eventID, transactionID, walletID              string
	direction                                     LedgerDirection
	amount, currency, balanceBefore, balanceAfter string
	walletVersion                                 int64
	occurredAt                                    time.Time
}

func NewWalletBalanceChangedEvent(input WalletBalanceChangedInput) (WalletBalanceChangedEvent, error) {
	if input.EventID == "" || input.TransactionID == "" || input.WalletID == "" || input.WalletVersion < 1 || (input.Direction != LedgerDirectionDebit && input.Direction != LedgerDirectionCredit) {
		return WalletBalanceChangedEvent{}, ErrInvalidWager
	}
	if input.Amount.MinorUnits() <= 0 || input.BalanceBefore.MinorUnits() < 0 || input.BalanceAfter.MinorUnits() < 0 || input.Amount.Currency() != input.BalanceBefore.Currency() || input.Amount.Currency() != input.BalanceAfter.Currency() {
		return WalletBalanceChangedEvent{}, ErrInvalidWager
	}
	var expected Money
	var err error
	if input.Direction == LedgerDirectionDebit {
		expected, err = input.BalanceBefore.Subtract(input.Amount)
	} else {
		expected, err = input.BalanceBefore.Add(input.Amount)
	}
	if err != nil || !expected.Equal(input.BalanceAfter) {
		return WalletBalanceChangedEvent{}, errors.Join(ErrInvalidWager, err)
	}
	return WalletBalanceChangedEvent{eventID: input.EventID, transactionID: input.TransactionID, walletID: input.WalletID, direction: input.Direction, amount: input.Amount.Decimal(), currency: input.Amount.Currency(), balanceBefore: input.BalanceBefore.Decimal(), balanceAfter: input.BalanceAfter.Decimal(), walletVersion: input.WalletVersion, occurredAt: time.Now().UTC()}, nil
}

func (event WalletBalanceChangedEvent) ID() string                 { return event.eventID }
func (event WalletBalanceChangedEvent) Type() EventType            { return EventTypeWalletBalanceChanged }
func (event WalletBalanceChangedEvent) Version() int               { return eventVersion }
func (event WalletBalanceChangedEvent) OccurredAt() time.Time      { return event.occurredAt }
func (event WalletBalanceChangedEvent) Direction() LedgerDirection { return event.direction }
func (event WalletBalanceChangedEvent) Amount() string             { return event.amount }
func (event WalletBalanceChangedEvent) Currency() string           { return event.currency }
func (event WalletBalanceChangedEvent) BalanceBefore() string      { return event.balanceBefore }
func (event WalletBalanceChangedEvent) BalanceAfter() string       { return event.balanceAfter }
func (event WalletBalanceChangedEvent) WalletVersion() int64       { return event.walletVersion }
