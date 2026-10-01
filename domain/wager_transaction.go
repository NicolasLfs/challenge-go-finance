package domain

import "time"

type WagerType string

const (
	WagerTypeOpening  WagerType = "OPENING"
	WagerTypeBet      WagerType = "BET"
	WagerTypeWin      WagerType = "WIN"
	WagerTypeLoss     WagerType = "LOSS"
	WagerTypeRefund   WagerType = "REFUND"
	WagerTypeRollback WagerType = "ROLLBACK"
)

type WagerStatus string

const (
	WagerStatusPending          WagerStatus = "PENDING"
	WagerStatusPendingReference WagerStatus = "PENDING_REFERENCE"
	WagerStatusProcessed        WagerStatus = "PROCESSED"
	WagerStatusRejected         WagerStatus = "REJECTED"
	WagerStatusFailed           WagerStatus = "FAILED"
)

type ExternalWagerInput struct {
	ID, ExternalID, ProviderID, IdempotencyKey string
	WalletID, PlayerID, RoundID, GameID        string
	Type                                       WagerType
	Amount                                     Money
	ReferenceExternalID                        string
	PayloadHash                                string
}

type WagerTransaction struct {
	id, externalID, providerID, idempotencyKey string
	walletID, playerID, roundID, gameID        string
	typ                                        WagerType
	amount                                     Money
	referenceExternalID, resolvedReferenceID   string
	payloadHash                                string
	status                                     WagerStatus
	failureCode                                string
	resultBalance                              Money
	referenceAttempts                          int32
	nextReferenceAttemptAt                     time.Time
	createdAt, updatedAt                       time.Time
}

type WagerTransactionData struct {
	ID, ExternalID, ProviderID, IdempotencyKey string
	WalletID, PlayerID, RoundID, GameID        string
	Type                                       WagerType
	Amount                                     Money
	ReferenceExternalID, ResolvedReferenceID   string
	PayloadHash, FailureCode                   string
	Status                                     WagerStatus
	ResultBalance                              Money
	ReferenceAttempts                          int32
	NextReferenceAttemptAt                     time.Time
	CreatedAt, UpdatedAt                       time.Time
}

func NewExternalWagerTransaction(input ExternalWagerInput) (*WagerTransaction, error) {
	if input.Type == WagerTypeOpening || !validExternalType(input.Type) || input.ID == "" || input.ExternalID == "" || input.ProviderID == "" || input.IdempotencyKey == "" || input.WalletID == "" || input.PlayerID == "" || input.RoundID == "" || input.GameID == "" || input.PayloadHash == "" || input.Amount.Currency() == "" {
		return nil, ErrInvalidWager
	}
	if input.Type == WagerTypeLoss && input.Amount.MinorUnits() != 0 {
		return nil, ErrInvalidWager
	}
	if input.Type != WagerTypeLoss && input.Amount.MinorUnits() <= 0 {
		return nil, ErrInvalidWager
	}
	if (input.Type == WagerTypeRefund || input.Type == WagerTypeRollback) && input.ReferenceExternalID == "" {
		return nil, ErrInvalidWager
	}
	if input.Type != WagerTypeRefund && input.Type != WagerTypeRollback && input.ReferenceExternalID != "" {
		return nil, ErrInvalidWager
	}
	now := time.Now().UTC()
	return &WagerTransaction{id: input.ID, externalID: input.ExternalID, providerID: input.ProviderID, idempotencyKey: input.IdempotencyKey, walletID: input.WalletID, playerID: input.PlayerID, roundID: input.RoundID, gameID: input.GameID, typ: input.Type, amount: input.Amount, referenceExternalID: input.ReferenceExternalID, payloadHash: input.PayloadHash, status: WagerStatusPending, createdAt: now, updatedAt: now}, nil
}

func NewOpeningWagerTransaction(id, walletID, playerID string, amount Money) (*WagerTransaction, error) {
	if id == "" || walletID == "" || playerID == "" || amount.Currency() == "" || amount.MinorUnits() < 0 {
		return nil, ErrInvalidWager
	}
	now := time.Now().UTC()
	return &WagerTransaction{id: id, walletID: walletID, playerID: playerID, typ: WagerTypeOpening, amount: amount, status: WagerStatusPending, createdAt: now, updatedAt: now}, nil
}

func (transaction *WagerTransaction) ID() string           { return transaction.id }
func (transaction *WagerTransaction) ExternalID() string   { return transaction.externalID }
func (transaction *WagerTransaction) ProviderID() string   { return transaction.providerID }
func (transaction *WagerTransaction) Type() WagerType      { return transaction.typ }
func (transaction *WagerTransaction) Amount() Money        { return transaction.amount }
func (transaction *WagerTransaction) Status() WagerStatus  { return transaction.status }
func (transaction *WagerTransaction) FailureCode() string  { return transaction.failureCode }
func (transaction *WagerTransaction) ResultBalance() Money { return transaction.resultBalance }
func (transaction *WagerTransaction) ReferenceExternalID() string {
	return transaction.referenceExternalID
}
func (transaction *WagerTransaction) ResolvedReferenceID() string {
	return transaction.resolvedReferenceID
}
func (transaction *WagerTransaction) CreatedAt() time.Time   { return transaction.createdAt }
func (transaction *WagerTransaction) UpdatedAt() time.Time   { return transaction.updatedAt }
func (transaction *WagerTransaction) WalletID() string       { return transaction.walletID }
func (transaction *WagerTransaction) PlayerID() string       { return transaction.playerID }
func (transaction *WagerTransaction) RoundID() string        { return transaction.roundID }
func (transaction *WagerTransaction) GameID() string         { return transaction.gameID }
func (transaction *WagerTransaction) IdempotencyKey() string { return transaction.idempotencyKey }
func (transaction *WagerTransaction) PayloadHash() string    { return transaction.payloadHash }

func (transaction *WagerTransaction) Data() WagerTransactionData {
	return WagerTransactionData{
		ID: transaction.id, ExternalID: transaction.externalID, ProviderID: transaction.providerID,
		IdempotencyKey: transaction.idempotencyKey, WalletID: transaction.walletID,
		PlayerID: transaction.playerID, RoundID: transaction.roundID, GameID: transaction.gameID,
		Type: transaction.typ, Amount: transaction.amount, ReferenceExternalID: transaction.referenceExternalID,
		ResolvedReferenceID: transaction.resolvedReferenceID, PayloadHash: transaction.payloadHash,
		FailureCode: transaction.failureCode, Status: transaction.status, ResultBalance: transaction.resultBalance,
		ReferenceAttempts: transaction.referenceAttempts, NextReferenceAttemptAt: transaction.nextReferenceAttemptAt,
		CreatedAt: transaction.createdAt, UpdatedAt: transaction.updatedAt,
	}
}

func RehydrateWagerTransaction(data WagerTransactionData) (*WagerTransaction, error) {
	if data.ID == "" || data.WalletID == "" || data.PlayerID == "" || data.Amount.Currency() == "" || data.CreatedAt.IsZero() || data.UpdatedAt.IsZero() {
		return nil, ErrInvalidWager
	}
	if data.Status != WagerStatusPending && data.Status != WagerStatusPendingReference && data.Status != WagerStatusProcessed && data.Status != WagerStatusRejected && data.Status != WagerStatusFailed {
		return nil, ErrInvalidWager
	}
	if data.Type == WagerTypeOpening {
		if data.ExternalID != "" || data.ProviderID != "" || data.IdempotencyKey != "" || data.PayloadHash != "" {
			return nil, ErrInvalidWager
		}
	} else if !validExternalType(data.Type) || data.ExternalID == "" || data.ProviderID == "" || data.IdempotencyKey == "" || data.PayloadHash == "" || data.RoundID == "" || data.GameID == "" {
		return nil, ErrInvalidWager
	}
	return &WagerTransaction{
		id: data.ID, externalID: data.ExternalID, providerID: data.ProviderID,
		idempotencyKey: data.IdempotencyKey, walletID: data.WalletID, playerID: data.PlayerID,
		roundID: data.RoundID, gameID: data.GameID, typ: data.Type, amount: data.Amount,
		referenceExternalID: data.ReferenceExternalID, resolvedReferenceID: data.ResolvedReferenceID,
		payloadHash: data.PayloadHash, status: data.Status, failureCode: data.FailureCode,
		resultBalance: data.ResultBalance, referenceAttempts: data.ReferenceAttempts,
		nextReferenceAttemptAt: data.NextReferenceAttemptAt.UTC(), createdAt: data.CreatedAt.UTC(), updatedAt: data.UpdatedAt.UTC(),
	}, nil
}

func (transaction *WagerTransaction) MarkPendingReference() error {
	if transaction.status != WagerStatusPending {
		return ErrInvalidTransition
	}
	transaction.status = WagerStatusPendingReference
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) ScheduleReferenceRetry(nextAttempt time.Time) error {
	if transaction.status != WagerStatusPendingReference || nextAttempt.IsZero() {
		return ErrInvalidTransition
	}
	transaction.referenceAttempts++
	transaction.nextReferenceAttemptAt = nextAttempt.UTC()
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) SetResolvedReference(referenceID string) error {
	if (transaction.status != WagerStatusPending && transaction.status != WagerStatusPendingReference) || referenceID == "" {
		return ErrInvalidTransition
	}
	transaction.resolvedReferenceID = referenceID
	if transaction.status == WagerStatusPendingReference {
		transaction.status = WagerStatusPending
	}
	transaction.nextReferenceAttemptAt = time.Time{}
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) ReferenceAttempts() int32 { return transaction.referenceAttempts }

func (transaction *WagerTransaction) NextReferenceAttemptAt() time.Time {
	return transaction.nextReferenceAttemptAt
}

func (transaction *WagerTransaction) MarkProcessed(resultBalance Money) error {
	if transaction.status != WagerStatusPending && transaction.status != WagerStatusPendingReference {
		return ErrInvalidTransition
	}
	if resultBalance.Currency() != transaction.amount.Currency() || resultBalance.MinorUnits() < 0 {
		return ErrInvalidWager
	}
	transaction.status = WagerStatusProcessed
	transaction.resultBalance = resultBalance
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) MarkRejected(failureCode string) error {
	if transaction.status != WagerStatusPending && transaction.status != WagerStatusPendingReference || failureCode == "" {
		return ErrInvalidTransition
	}
	transaction.status = WagerStatusRejected
	transaction.failureCode = failureCode
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) MarkRejectedWithBalance(failureCode string, balance Money) error {
	if balance.Currency() != transaction.amount.Currency() || balance.MinorUnits() < 0 {
		return ErrInvalidWager
	}
	if err := transaction.MarkRejected(failureCode); err != nil {
		return err
	}
	transaction.resultBalance = balance
	return nil
}

func (transaction *WagerTransaction) SetObservedBalance(balance Money) error {
	if transaction.status != WagerStatusPending && transaction.status != WagerStatusPendingReference || balance.Currency() != transaction.amount.Currency() || balance.MinorUnits() < 0 {
		return ErrInvalidTransition
	}
	transaction.resultBalance = balance
	return nil
}

func (transaction *WagerTransaction) MarkFailed(failureCode string) error {
	if transaction.status != WagerStatusPending && transaction.status != WagerStatusPendingReference || failureCode == "" {
		return ErrInvalidTransition
	}
	transaction.status = WagerStatusFailed
	transaction.failureCode = failureCode
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func (transaction *WagerTransaction) ResolveReference(referenceID string) error {
	if transaction.status != WagerStatusPendingReference || referenceID == "" {
		return ErrInvalidTransition
	}
	transaction.resolvedReferenceID = referenceID
	transaction.status = WagerStatusPending
	transaction.updatedAt = time.Now().UTC()
	return nil
}

func validExternalType(wagerType WagerType) bool {
	return wagerType == WagerTypeBet || wagerType == WagerTypeWin || wagerType == WagerTypeLoss || wagerType == WagerTypeRefund || wagerType == WagerTypeRollback
}
