package application

import (
	"context"
	"fmt"

	"challenge/domain"
)

type ReconciliationService struct {
	repository Repository
}

func NewReconciliationService(repository Repository) *ReconciliationService {
	return &ReconciliationService{repository: repository}
}

type Reconciliation struct {
	WalletID      string
	StoredBalance domain.Money
	LedgerBalance domain.Money
	Difference    domain.Money
	EntryCount    int
}

func (service *ReconciliationService) Reconcile(ctx context.Context, walletID string) (Reconciliation, error) {
	var result Reconciliation
	err := service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		wallet, err := transaction.WalletForUpdate(ctx, walletID)
		if err != nil {
			return fmt.Errorf("load wallet for reconciliation: %w", err)
		}
		ledgerBalance, err := domain.ZeroMoney(wallet.Balance().Currency())
		if err != nil {
			return err
		}
		entries, err := transaction.LedgerEntries(ctx, walletID)
		if err != nil {
			return fmt.Errorf("load ledger for reconciliation: %w", err)
		}
		for _, entry := range entries {
			var delta domain.Money
			switch entry.Direction {
			case domain.LedgerDirectionCredit:
				delta = entry.Amount
			case domain.LedgerDirectionDebit:
				delta, err = entry.Amount.Negate()
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid ledger direction %q", entry.Direction)
			}
			ledgerBalance, err = ledgerBalance.Add(delta)
			if err != nil {
				return fmt.Errorf("sum ledger entries: %w", err)
			}
		}
		difference, err := wallet.Balance().Subtract(ledgerBalance)
		if err != nil {
			return fmt.Errorf("calculate reconciliation difference: %w", err)
		}
		result = Reconciliation{
			WalletID: walletID, StoredBalance: wallet.Balance(), LedgerBalance: ledgerBalance,
			Difference: difference, EntryCount: len(entries),
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	return result, nil
}
