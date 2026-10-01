package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"challenge/domain"
)

var ErrWalletConflict = errors.New("wallet already exists for player and currency")

type WalletService struct {
	repository Repository
	ids        IDGenerator
}

func NewWalletService(repository Repository, ids IDGenerator) *WalletService {
	return &WalletService{repository: repository, ids: ids}
}

type OpenWalletCommand struct {
	PlayerID string
	Balance  domain.Money
}

type LedgerCursor struct {
	CreatedAt time.Time
	ID        string
}

func (service *WalletService) OpenWallet(ctx context.Context, command OpenWalletCommand) (*domain.Wallet, error) {
	if command.PlayerID == "" || command.Balance.Currency() == "" || command.Balance.MinorUnits() < 0 {
		return nil, domain.ErrInvalidWallet
	}
	walletID, err := service.ids.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate wallet ID: %w", err)
	}
	wallet, err := domain.NewWallet(walletID, command.PlayerID, command.Balance)
	if err != nil {
		return nil, err
	}
	err = service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		_, err := transaction.WalletByPlayerCurrency(ctx, command.PlayerID, command.Balance.Currency())
		if err == nil {
			return ErrWalletConflict
		}
		if !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("check existing wallet: %w", err)
		}
		if err := transaction.CreateWallet(ctx, wallet); err != nil {
			return fmt.Errorf("create wallet: %w", err)
		}
		if command.Balance.MinorUnits() == 0 {
			return nil
		}
		return service.recordOpening(ctx, transaction, wallet, command.Balance)
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (service *WalletService) GetWallet(ctx context.Context, walletID string) (*domain.Wallet, error) {
	var wallet *domain.Wallet
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		var err error
		wallet, err = transaction.WalletByID(ctx, walletID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (service *WalletService) Ledger(ctx context.Context, walletID string, cursor *LedgerCursor, limit int) ([]LedgerEntry, error) {
	if limit < 1 || limit > 101 {
		return nil, domain.ErrInvalidWallet
	}
	var entries []LedgerEntry
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		if _, err := transaction.WalletByID(ctx, walletID); err != nil {
			return err
		}
		var createdAt time.Time
		var cursorID string
		if cursor != nil {
			createdAt, cursorID = cursor.CreatedAt, cursor.ID
		}
		var err error
		entries, err = transaction.LedgerPage(ctx, walletID, createdAt, cursorID, int32(limit))
		return err
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (service *WalletService) recordOpening(ctx context.Context, transaction Transaction, wallet *domain.Wallet, amount domain.Money) error {
	transactionID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate opening transaction ID: %w", err)
	}
	opening, err := domain.NewOpeningWagerTransaction(transactionID, wallet.ID(), wallet.PlayerID(), amount)
	if err != nil {
		return err
	}
	if err := opening.MarkProcessed(wallet.Balance()); err != nil {
		return err
	}
	if err := transaction.CreateWagerTransaction(ctx, opening); err != nil {
		return fmt.Errorf("create opening transaction: %w", err)
	}

	ledgerID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate opening ledger ID: %w", err)
	}
	zero, err := domain.ZeroMoney(amount.Currency())
	if err != nil {
		return err
	}
	occurredAt := time.Now().UTC()
	if err := transaction.AppendLedger(ctx, LedgerEntry{
		ID: ledgerID, WalletID: wallet.ID(), TransactionID: transactionID,
		Direction: domain.LedgerDirectionCredit, Amount: amount,
		BalanceBefore: zero, BalanceAfter: amount, CreatedAt: occurredAt,
	}); err != nil {
		return fmt.Errorf("append opening ledger: %w", err)
	}

	processedID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate opening event ID: %w", err)
	}
	processed, err := domain.NewWagerTransactionProcessedEvent(domain.WagerTransactionProcessedInput{
		EventID: processedID, TransactionID: transactionID, WalletID: wallet.ID(),
		TransactionType: domain.WagerTypeOpening, Amount: amount, OccurredAt: occurredAt,
	})
	if err != nil {
		return err
	}
	processedPayload, err := json.Marshal(struct {
		EventID         string           `json:"eventId"`
		TransactionID   string           `json:"transactionId"`
		WalletID        string           `json:"walletId"`
		TransactionType domain.WagerType `json:"transactionType"`
		Amount          string           `json:"amount"`
		Currency        string           `json:"currency"`
		OccurredAt      time.Time        `json:"occurredAt"`
	}{processed.ID(), transactionID, wallet.ID(), domain.WagerTypeOpening, amount.Decimal(), amount.Currency(), processed.OccurredAt()})
	if err != nil {
		return fmt.Errorf("encode opening event: %w", err)
	}
	if err := transaction.AppendOutbox(ctx, OutboxEvent{
		ID: processed.ID(), AggregateID: wallet.ID(), Type: processed.Type(),
		Version: processed.Version(), OccurredAt: processed.OccurredAt(), Payload: processedPayload,
	}); err != nil {
		return fmt.Errorf("append opening event: %w", err)
	}

	balanceEventID, err := service.ids.NewID()
	if err != nil {
		return fmt.Errorf("generate opening balance event ID: %w", err)
	}
	balanceChanged, err := domain.NewWalletBalanceChangedEvent(domain.WalletBalanceChangedInput{
		EventID: balanceEventID, TransactionID: transactionID, WalletID: wallet.ID(),
		Direction: domain.LedgerDirectionCredit, Amount: amount, BalanceBefore: zero,
		BalanceAfter: amount, WalletVersion: wallet.Version(),
	})
	if err != nil {
		return err
	}
	balancePayload, err := json.Marshal(struct {
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
		EventID: balanceChanged.ID(), TransactionID: transactionID, WalletID: wallet.ID(), Direction: balanceChanged.Direction(),
		Money: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: balanceChanged.Amount(), Currency: balanceChanged.Currency()},
		BalanceBefore: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: balanceChanged.BalanceBefore(), Currency: balanceChanged.Currency()},
		BalanceAfter: struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		}{Amount: balanceChanged.BalanceAfter(), Currency: balanceChanged.Currency()},
		WalletVersion: balanceChanged.WalletVersion(), OccurredAt: balanceChanged.OccurredAt(),
	})
	if err != nil {
		return fmt.Errorf("encode balance event: %w", err)
	}
	if err := transaction.AppendOutbox(ctx, OutboxEvent{
		ID: balanceChanged.ID(), AggregateID: wallet.ID(), Type: balanceChanged.Type(),
		Version: balanceChanged.Version(), OccurredAt: balanceChanged.OccurredAt(), Payload: balancePayload,
	}); err != nil {
		return fmt.Errorf("append balance event: %w", err)
	}
	return nil
}
