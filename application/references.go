package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"challenge/domain"
)

const (
	maximumReferenceAttempts = 20
	referenceTTL             = 24 * time.Hour
)

func (service *WagerService) RetryPendingReferences(ctx context.Context, limit int32) error {
	if limit <= 0 {
		return fmt.Errorf("pending reference limit must be positive")
	}
	pending, err := service.repository.PendingReferences(ctx, service.now(), limit)
	if err != nil {
		return fmt.Errorf("list pending wager references: %w", err)
	}
	for _, wager := range pending {
		if err := service.retryPendingReference(ctx, wager.ID()); err != nil {
			return fmt.Errorf("retry wager %s reference: %w", wager.ID(), err)
		}
	}
	return nil
}

func (service *WagerService) retryPendingReference(ctx context.Context, wagerID string) error {
	return service.repository.WithinTransaction(ctx, func(transaction Transaction) error {
		candidate, err := transaction.WagerByID(ctx, wagerID)
		if err != nil {
			return fmt.Errorf("reload pending wager: %w", err)
		}
		if candidate.Status() != domain.WagerStatusPendingReference {
			return nil
		}
		wallet, err := transaction.WalletForUpdate(ctx, candidate.WalletID())
		if err != nil {
			return fmt.Errorf("lock pending wager wallet: %w", err)
		}
		wager, err := transaction.WagerByID(ctx, wagerID)
		if err != nil {
			return fmt.Errorf("reload locked pending wager: %w", err)
		}
		if wager.Status() != domain.WagerStatusPendingReference {
			return nil
		}
		reference, err := transaction.WagerByExternalID(ctx, wager.ProviderID(), wager.ReferenceExternalID())
		if errors.Is(err, ErrNotFound) {
			return service.retryOrReject(ctx, transaction, wager, wallet, true)
		}
		if err != nil {
			return fmt.Errorf("load pending wager reference: %w", err)
		}
		if reference.Status() == domain.WagerStatusPending || reference.Status() == domain.WagerStatusPendingReference {
			return service.retryOrReject(ctx, transaction, wager, wallet, false)
		}
		if reference.Status() != domain.WagerStatusProcessed {
			return service.rejectPendingReference(ctx, transaction, wager, wallet, "REFERENCE_NOT_PROCESSED")
		}
		if failureCode := validateReference(wager, reference); failureCode != "" {
			return service.rejectPendingReference(ctx, transaction, wager, wallet, failureCode)
		}
		reversed, err := transaction.HasSuccessfulReversal(ctx, reference.ID())
		if err != nil {
			return err
		}
		if reversed {
			return service.rejectPendingReference(ctx, transaction, wager, wallet, "REFERENCE_ALREADY_REVERSED")
		}
		if err := wager.SetResolvedReference(reference.ID()); err != nil {
			return err
		}
		before := wallet.Balance()
		changed, _, failureCode, err := applyReversal(wager, reference, wallet)
		if err != nil {
			return err
		}
		if failureCode != "" {
			return service.rejectPendingReference(ctx, transaction, wager, wallet, failureCode)
		}
		if err := wager.MarkProcessed(wallet.Balance()); err != nil {
			return err
		}
		if err := transaction.UpdateWagerTransaction(ctx, wager); err != nil {
			return fmt.Errorf("complete resolved wager: %w", err)
		}
		if changed {
			if err := transaction.UpdateWallet(ctx, wallet); err != nil {
				return fmt.Errorf("update resolved wager wallet: %w", err)
			}
			if err := service.appendLedger(ctx, transaction, wager, before, wallet.Balance()); err != nil {
				return err
			}
		}
		if err := service.appendProcessed(ctx, transaction, wager); err != nil {
			return err
		}
		if changed {
			if err := service.appendBalanceChanged(ctx, transaction, wager, before, wallet); err != nil {
				return err
			}
		}
		return nil
	})
}

func (service *WagerService) retryOrReject(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, wallet *domain.Wallet, referenceMissing bool) error {
	now := service.now()
	if wager.ReferenceAttempts() >= maximumReferenceAttempts || now.Sub(wager.CreatedAt()) >= referenceTTL {
		failureCode := "REFERENCE_NOT_PROCESSED"
		if referenceMissing {
			failureCode = "REFERENCE_NOT_FOUND"
		}
		return service.rejectPendingReference(ctx, transaction, wager, wallet, failureCode)
	}
	if err := service.scheduleReference(wager); err != nil {
		return err
	}
	if err := transaction.UpdateWagerTransaction(ctx, wager); err != nil {
		return fmt.Errorf("reschedule pending reference: %w", err)
	}
	return nil
}

func (service *WagerService) rejectPendingReference(ctx context.Context, transaction Transaction, wager *domain.WagerTransaction, wallet *domain.Wallet, failureCode string) error {
	if wallet == nil {
		var err error
		wallet, err = transaction.WalletForUpdate(ctx, wager.WalletID())
		if err != nil {
			return fmt.Errorf("load wallet for reference rejection: %w", err)
		}
	}
	if err := wager.MarkRejectedWithBalance(failureCode, wallet.Balance()); err != nil {
		return err
	}
	if err := transaction.UpdateWagerTransaction(ctx, wager); err != nil {
		return fmt.Errorf("persist reference rejection: %w", err)
	}
	if err := service.appendRejected(ctx, transaction, wager); err != nil {
		return err
	}
	return nil
}
