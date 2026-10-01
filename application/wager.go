package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"challenge/domain"
)

var (
	ErrIdempotencyConflict         = errors.New("idempotency key reused with different payload")
	ErrExternalTransactionConflict = errors.New("external transaction already exists with another idempotency key")
)

type WagerService struct {
	repository Repository
	ids        IDGenerator
	now        func() time.Time
}

func NewWagerService(repository Repository, ids IDGenerator) *WagerService {
	return &WagerService{repository: repository, ids: ids, now: func() time.Time { return time.Now().UTC() }}
}

type ProcessWagerCommand struct {
	Wager domain.ExternalWagerInput
}

type WagerResult struct {
	TransactionID    string
	Status           domain.WagerStatus
	FailureCode      string
	Balance          domain.Money
	IdempotentReplay bool
}

func (service *WagerService) GetByID(ctx context.Context, transactionID string) (*domain.WagerTransaction, error) {
	var wager *domain.WagerTransaction
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		wager, err = transaction.WagerByID(ctx, transactionID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wager, nil
}

func (service *WagerService) GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	var wager *domain.WagerTransaction
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		wager, err = transaction.WagerByExternalID(ctx, providerID, externalID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wager, nil
}

func CanonicalWagerHash(wager domain.ExternalWagerInput) (string, error) {
	if wager.ExternalID == "" || wager.ProviderID == "" || wager.WalletID == "" || wager.PlayerID == "" || wager.RoundID == "" || wager.GameID == "" || wager.Type == domain.WagerTypeOpening || wager.Amount.Currency() == "" {
		return "", domain.ErrInvalidWager
	}
	canonical, err := json.Marshal(struct {
		ExternalTransactionID string `json:"externalTransactionId"`
		GameID                string `json:"gameId"`
		Kind                  string `json:"kind"`
		Money                 struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		PlayerID                       string `json:"playerId"`
		ProviderID                     string `json:"providerId"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
		RoundID                        string `json:"roundId"`
		WalletID                       string `json:"walletId"`
	}{
		ExternalTransactionID: wager.ExternalID,
		GameID:                wager.GameID,
		Kind:                  string(wager.Type),
		Money: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: wager.Amount.Decimal(), Currency: wager.Amount.Currency()},
		PlayerID:                       wager.PlayerID,
		ProviderID:                     wager.ProviderID,
		ReferenceExternalTransactionID: wager.ReferenceExternalID,
		RoundID:                        wager.RoundID,
		WalletID:                       wager.WalletID,
	})
	if err != nil {
		return "", fmt.Errorf("encode canonical wager: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func (service *WagerService) Process(ctx context.Context, command ProcessWagerCommand) (WagerResult, error) {
	return service.process(ctx, command, nil)
}

func (service *WagerService) ProcessMessage(ctx context.Context, command ProcessWagerCommand, message InboxMessage) (WagerResult, error) {
	if message.ConsumerName == "" || message.MessageID == "" || message.MessageHash == "" || len(message.Payload) == 0 {
		return WagerResult{}, ErrInboxConflict
	}
	return service.process(ctx, command, &message)
}

func (service *WagerService) process(ctx context.Context, command ProcessWagerCommand, inbox *InboxMessage) (WagerResult, error) {
	input := command.Wager
	if input.IdempotencyKey == "" {
		return WagerResult{}, domain.ErrInvalidWager
	}
	payloadHash, err := CanonicalWagerHash(input)
	if err != nil {
		return WagerResult{}, err
	}
	var result WagerResult
	err = service.repository.WithinTransaction(ctx, func(transaction Transaction) (operationErr error) {
		completeInbox := false
		if inbox != nil {
			duplicate, err := transaction.BeginInbox(ctx, *inbox)
			if err != nil {
				return err
			}
			if duplicate {
				previous, err := transaction.WagerByIdempotencyKey(ctx, input.ProviderID, input.IdempotencyKey)
				if err != nil {
					return fmt.Errorf("load wager for inbox replay: %w", err)
				}
				if previous.PayloadHash() != payloadHash {
					return ErrIdempotencyConflict
				}
				result, err = service.persistedResult(ctx, transaction, previous, true)
				return err
			}
			completeInbox = true
		}
		defer func() {
			if operationErr == nil && completeInbox {
				operationErr = transaction.CompleteInbox(ctx, *inbox)
			}
		}()

		previous, err := transaction.WagerByIdempotencyKey(ctx, input.ProviderID, input.IdempotencyKey)
		if err == nil {
			if previous.PayloadHash() != payloadHash {
				return ErrIdempotencyConflict
			}
			result, err = service.persistedResult(ctx, transaction, previous, true)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("find wager by idempotency key: %w", err)
		}
		previous, err = transaction.WagerByExternalID(ctx, input.ProviderID, input.ExternalID)
		if err == nil {
			result, err = service.replayExternalMatch(ctx, transaction, previous, input, payloadHash)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("find wager by external ID: %w", err)
		}

		wallet, err := transaction.WalletForUpdate(ctx, input.WalletID)
		if err != nil {
			return fmt.Errorf("lock wager wallet: %w", err)
		}
		if wallet.PlayerID() != input.PlayerID || wallet.Balance().Currency() != input.Amount.Currency() {
			return domain.ErrInvalidWager
		}
		previous, err = transaction.WagerByIdempotencyKey(ctx, input.ProviderID, input.IdempotencyKey)
		if err == nil {
			if previous.PayloadHash() != payloadHash {
				return ErrIdempotencyConflict
			}
			result, err = service.persistedResult(ctx, transaction, previous, true)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("recheck wager idempotency key: %w", err)
		}
		previous, err = transaction.WagerByExternalID(ctx, input.ProviderID, input.ExternalID)
		if err == nil {
			result, err = service.replayExternalMatch(ctx, transaction, previous, input, payloadHash)
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("recheck external wager ID: %w", err)
		}
		transactionID, err := service.ids.NewID()
		if err != nil {
			return fmt.Errorf("generate wager transaction ID: %w", err)
		}
		input.ID = transactionID
		input.PayloadHash = payloadHash
		wager, err := domain.NewExternalWagerTransaction(input)
		if err != nil {
			return err
		}
		balanceBefore := wallet.Balance()
		changed, pendingReference, failureCode, applyErr := service.applyNewWager(ctx, transaction, wager, wallet)
		if applyErr != nil {
			return applyErr
		}
		if pendingReference {
			if err := service.appendPendingReference(ctx, transaction, wager); err != nil {
				return err
			}
			if err := transaction.CreateWagerTransaction(ctx, wager); err != nil {
				return fmt.Errorf("create pending reference transaction: %w", err)
			}
			result = WagerResult{TransactionID: transactionID, Status: wager.Status(), Balance: wallet.Balance()}
			return nil
		}
		if failureCode != "" {
			if err := wager.MarkRejectedWithBalance(failureCode, wallet.Balance()); err != nil {
				return err
			}
			if err := transaction.CreateWagerTransaction(ctx, wager); err != nil {
				return fmt.Errorf("create rejected wager transaction: %w", err)
			}
			if err := service.appendRejected(ctx, transaction, wager); err != nil {
				return err
			}
			result = WagerResult{TransactionID: transactionID, Status: wager.Status(), FailureCode: failureCode, Balance: wallet.Balance()}
			return nil
		}
		if err := wager.MarkProcessed(wallet.Balance()); err != nil {
			return err
		}
		if err := transaction.CreateWagerTransaction(ctx, wager); err != nil {
			return fmt.Errorf("create processed wager transaction: %w", err)
		}
		if changed {
			if err := transaction.UpdateWallet(ctx, wallet); err != nil {
				return fmt.Errorf("update wager wallet: %w", err)
			}
			if err := service.appendLedger(ctx, transaction, wager, balanceBefore, wallet.Balance()); err != nil {
				return err
			}
		}
		if err := service.appendProcessed(ctx, transaction, wager); err != nil {
			return err
		}
		if changed {
			if err := service.appendBalanceChanged(ctx, transaction, wager, balanceBefore, wallet); err != nil {
				return err
			}
		}
		result = WagerResult{TransactionID: transactionID, Status: wager.Status(), Balance: wallet.Balance()}
		return nil
	})
	if errors.Is(err, ErrUniqueConflict) {
		return service.resolveConcurrentConflict(ctx, input, payloadHash, inbox)
	}
	if err != nil {
		return WagerResult{}, err
	}
	return result, nil
}

func (service *WagerService) resolveConcurrentConflict(ctx context.Context, input domain.ExternalWagerInput, payloadHash string, inbox *InboxMessage) (WagerResult, error) {
	var result WagerResult
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		if inbox != nil {
			duplicate, err := transaction.BeginInbox(ctx, *inbox)
			if err != nil {
				return err
			}
			if duplicate {
				return ErrInboxIncomplete
			}
		}
		wager, err := transaction.WagerByIdempotencyKey(ctx, input.ProviderID, input.IdempotencyKey)
		if err == nil {
			if wager.PayloadHash() != payloadHash {
				return ErrIdempotencyConflict
			}
			result, err = service.persistedResult(ctx, transaction, wager, true)
			if err == nil && inbox != nil {
				err = transaction.CompleteInbox(ctx, *inbox)
			}
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		wager, err = transaction.WagerByExternalID(ctx, input.ProviderID, input.ExternalID)
		if err == nil {
			result, err = service.replayExternalMatch(ctx, transaction, wager, input, payloadHash)
			if err == nil && inbox != nil {
				err = transaction.CompleteInbox(ctx, *inbox)
			}
			return err
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		return ErrUniqueConflict
	})
	if err != nil {
		return WagerResult{}, err
	}
	return result, nil
}

func (service *WagerService) replayExternalMatch(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, input domain.ExternalWagerInput, payloadHash string) (WagerResult, error) {
	if wager.IdempotencyKey() != input.IdempotencyKey {
		return WagerResult{}, ErrExternalTransactionConflict
	}
	if wager.PayloadHash() != payloadHash {
		return WagerResult{}, ErrIdempotencyConflict
	}
	return service.persistedResult(ctx, transaction, wager, true)
}

func applyWager(wager *domain.WagerTransaction, wallet *domain.Wallet) (changed, pendingReference bool, failureCode string, err error) {
	switch wager.Type() {
	case domain.WagerTypeBet:
		if err := wallet.Debit(wager.Amount()); errors.Is(err, domain.ErrInsufficientBalance) {
			return false, false, "INSUFFICIENT_BALANCE", nil
		} else if err != nil {
			return false, false, "", err
		}
		return wager.Amount().MinorUnits() > 0, false, "", nil
	case domain.WagerTypeWin:
		if err := wallet.Credit(wager.Amount()); err != nil {
			return false, false, "", err
		}
		return wager.Amount().MinorUnits() > 0, false, "", nil
	case domain.WagerTypeLoss:
		return false, false, "", nil
	case domain.WagerTypeRefund, domain.WagerTypeRollback:
		return false, false, "", domain.ErrInvalidTransition
	default:
		return false, false, "", domain.ErrInvalidWager
	}
}

func (service *WagerService) applyNewWager(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, wallet *domain.Wallet) (bool, bool, string, error) {
	if wager.Type() != domain.WagerTypeRefund && wager.Type() != domain.WagerTypeRollback {
		return applyWager(wager, wallet)
	}
	reference, err := transaction.WagerByExternalID(ctx, wager.ProviderID(), wager.ReferenceExternalID())
	if errors.Is(err, ErrNotFound) {
		if err := wager.SetObservedBalance(wallet.Balance()); err != nil {
			return false, false, "", err
		}
		return false, true, "", service.scheduleReference(wager)
	}
	if err != nil {
		return false, false, "", fmt.Errorf("load referenced wager: %w", err)
	}
	if reference.Status() == domain.WagerStatusPending || reference.Status() == domain.WagerStatusPendingReference {
		if err := wager.SetObservedBalance(wallet.Balance()); err != nil {
			return false, false, "", err
		}
		return false, true, "", service.scheduleReference(wager)
	}
	if reference.Status() != domain.WagerStatusProcessed {
		return false, false, "REFERENCE_NOT_PROCESSED", nil
	}
	if failureCode := validateReference(wager, reference); failureCode != "" {
		return false, false, failureCode, nil
	}
	reversed, err := transaction.HasSuccessfulReversal(ctx, reference.ID())
	if err != nil {
		return false, false, "", err
	}
	if reversed {
		return false, false, "REFERENCE_ALREADY_REVERSED", nil
	}
	if err := wager.SetResolvedReference(reference.ID()); err != nil {
		return false, false, "", err
	}
	return applyReversal(wager, reference, wallet)
}

func (service *WagerService) scheduleReference(wager *domain.WagerTransaction) error {
	if wager.Status() == domain.WagerStatusPending {
		if err := wager.MarkPendingReference(); err != nil {
			return err
		}
	}
	return wager.ScheduleReferenceRetry(service.now().Add(referenceRetryDelay(wager.ReferenceAttempts() + 1)))
}

func validateReference(wager, reference *domain.WagerTransaction) string {
	dependent, original := wager.Data(), reference.Data()
	if dependent.ProviderID != original.ProviderID || dependent.PlayerID != original.PlayerID || dependent.WalletID != original.WalletID || dependent.RoundID != original.RoundID || dependent.Amount.Currency() != original.Amount.Currency() || !dependent.Amount.Equal(original.Amount) {
		return "REFERENCE_MISMATCH"
	}
	if dependent.Type == domain.WagerTypeRefund && original.Type != domain.WagerTypeBet {
		return "REFERENCE_TYPE_INVALID"
	}
	if dependent.Type == domain.WagerTypeRollback && original.Type != domain.WagerTypeBet && original.Type != domain.WagerTypeWin && original.Type != domain.WagerTypeRefund {
		return "REFERENCE_TYPE_INVALID"
	}
	return ""
}

func applyReversal(wager, reference *domain.WagerTransaction, wallet *domain.Wallet) (bool, bool, string, error) {
	credit := wager.Type() == domain.WagerTypeRefund || reference.Type() == domain.WagerTypeBet
	if credit {
		if err := wallet.Credit(wager.Amount()); err != nil {
			return false, false, "", err
		}
		return true, false, "", nil
	}
	if err := wallet.Debit(wager.Amount()); errors.Is(err, domain.ErrInsufficientBalance) {
		return false, false, "REVERSAL_INSUFFICIENT_BALANCE", nil
	} else if err != nil {
		return false, false, "", err
	}
	return true, false, "", nil
}

func referenceRetryDelay(attempt int32) time.Duration {
	delay := time.Second
	for index := int32(1); index < attempt && delay < 15*time.Minute; index++ {
		delay *= 2
	}
	if delay > 15*time.Minute {
		return 15 * time.Minute
	}
	return delay
}

func (service *WagerService) persistedResult(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, replay bool) (WagerResult, error) {
	balance := wager.ResultBalance()
	if balance.Currency() == "" {
		wallet, err := transaction.WalletForUpdate(ctx, wager.WalletID())
		if err != nil {
			return WagerResult{}, fmt.Errorf("load wallet for wager replay: %w", err)
		}
		balance = wallet.Balance()
	}
	return WagerResult{
		TransactionID: wager.ID(), Status: wager.Status(), FailureCode: wager.FailureCode(),
		Balance: balance, IdempotentReplay: replay,
	}, nil
}

func (service *WagerService) appendLedger(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, before, after domain.Money) error {
	id, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate ledger ID: %w", err)
	}
	direction := movementDirection(before, after)
	return transaction.AppendLedger(ctx, LedgerEntry{
		ID: id, WalletID: wager.WalletID(), TransactionID: wager.ID(), Direction: direction,
		Amount: wager.Amount(), BalanceBefore: before, BalanceAfter: after, CreatedAt: time.Now().UTC(),
	})
}

func (service *WagerService) appendProcessed(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction) error {
	eventID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate processed event ID: %w", err)
	}
	event, err := domain.NewWagerTransactionProcessedEvent(domain.WagerTransactionProcessedInput{
		EventID: eventID, TransactionID: wager.ID(), WalletID: wager.WalletID(), ProviderID: wager.ProviderID(),
		TransactionType: wager.Type(), Amount: wager.Amount(), OccurredAt: wager.UpdatedAt(),
	})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		EventID         string           `json:"eventId"`
		TransactionID   string           `json:"transactionId"`
		WalletID        string           `json:"walletId"`
		ProviderID      string           `json:"providerId"`
		TransactionType domain.WagerType `json:"transactionType"`
		Amount          string           `json:"amount"`
		Currency        string           `json:"currency"`
		OccurredAt      time.Time        `json:"occurredAt"`
	}{event.ID(), wager.ID(), wager.WalletID(), wager.ProviderID(), wager.Type(), wager.Amount().Decimal(), wager.Amount().Currency(), event.OccurredAt()})
	if err != nil {
		return fmt.Errorf("encode processed event: %w", err)
	}
	return transaction.AppendOutbox(ctx, OutboxEvent{ID: event.ID(), AggregateID: wager.WalletID(), Type: event.Type(), Version: event.Version(), OccurredAt: event.OccurredAt(), Payload: payload})
}

func (service *WagerService) appendRejected(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction) error {
	eventID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate rejected event ID: %w", err)
	}
	event, err := domain.NewWagerTransactionRejectedEvent(eventID, wager.ID(), wager.WalletID(), wager.ProviderID(), wager.FailureCode(), wager.UpdatedAt())
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		EventID       string    `json:"eventId"`
		TransactionID string    `json:"transactionId"`
		WalletID      string    `json:"walletId"`
		ProviderID    string    `json:"providerId"`
		FailureCode   string    `json:"failureCode"`
		OccurredAt    time.Time `json:"occurredAt"`
	}{event.ID(), wager.ID(), wager.WalletID(), wager.ProviderID(), event.FailureCode(), event.OccurredAt()})
	if err != nil {
		return fmt.Errorf("encode rejected event: %w", err)
	}
	return transaction.AppendOutbox(ctx, OutboxEvent{ID: event.ID(), AggregateID: wager.WalletID(), Type: event.Type(), Version: event.Version(), OccurredAt: event.OccurredAt(), Payload: payload})
}

func (service *WagerService) appendPendingReference(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction) error {
	eventID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate pending reference event ID: %w", err)
	}
	event, err := domain.NewWagerTransactionPendingReferenceEvent(eventID, wager.ID(), wager.WalletID(), wager.ProviderID(), wager.ReferenceExternalID(), wager.UpdatedAt())
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		EventID             string    `json:"eventId"`
		TransactionID       string    `json:"transactionId"`
		WalletID            string    `json:"walletId"`
		ProviderID          string    `json:"providerId"`
		ReferenceExternalID string    `json:"referenceExternalTransactionId"`
		OccurredAt          time.Time `json:"occurredAt"`
	}{event.ID(), wager.ID(), wager.WalletID(), wager.ProviderID(), event.ReferenceExternalID(), event.OccurredAt()})
	if err != nil {
		return fmt.Errorf("encode pending reference event: %w", err)
	}
	return transaction.AppendOutbox(ctx, OutboxEvent{ID: event.ID(), AggregateID: wager.WalletID(), Type: event.Type(), Version: event.Version(), OccurredAt: event.OccurredAt(), Payload: payload})
}

func (service *WagerService) appendBalanceChanged(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, before domain.Money, wallet *domain.Wallet) error {
	eventID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate balance changed event ID: %w", err)
	}
	direction := movementDirection(before, wallet.Balance())
	event, err := domain.NewWalletBalanceChangedEvent(domain.WalletBalanceChangedInput{
		EventID: eventID, TransactionID: wager.ID(), WalletID: wager.WalletID(), Direction: direction,
		Amount: wager.Amount(), BalanceBefore: before, BalanceAfter: wallet.Balance(), WalletVersion: wallet.Version(),
	})
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		EventID       string                 `json:"eventId"`
		TransactionID string                 `json:"transactionId"`
		WalletID      string                 `json:"walletId"`
		Direction     domain.LedgerDirection `json:"direction"`
		Money         struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
		BalanceBefore struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balanceBefore"`
		BalanceAfter struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balanceAfter"`
		WalletVersion int64     `json:"walletVersion"`
		OccurredAt    time.Time `json:"occurredAt"`
	}{
		EventID: event.ID(), TransactionID: wager.ID(), WalletID: wager.WalletID(), Direction: event.Direction(),
		Money: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: event.Amount(), Currency: event.Currency()},
		BalanceBefore: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: event.BalanceBefore(), Currency: event.Currency()},
		BalanceAfter: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: event.BalanceAfter(), Currency: event.Currency()},
		WalletVersion: event.WalletVersion(), OccurredAt: event.OccurredAt(),
	})
	if err != nil {
		return fmt.Errorf("encode balance changed event: %w", err)
	}
	return transaction.AppendOutbox(ctx, OutboxEvent{ID: event.ID(), AggregateID: wager.WalletID(), Type: event.Type(), Version: event.Version(), OccurredAt: event.OccurredAt(), Payload: payload})
}

func movementDirection(before, after domain.Money) domain.LedgerDirection {
	if after.LessThan(before) {
		return domain.LedgerDirectionDebit
	}
	return domain.LedgerDirectionCredit
}
