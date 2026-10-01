package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"challenge/application"
	"challenge/db"
	"challenge/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (repository *PostgresRepository) WithinTransaction(ctx context.Context, operation func(application.Transaction) error) error {
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin application transaction: %w", err)
	}
	transaction := &postgresTransaction{queries: db.New(tx)}
	if err := operation(transaction); err != nil {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback application transaction: %w", rollbackErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		commitErr := fmt.Errorf("commit application transaction: %w", err)
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return errors.Join(commitErr, fmt.Errorf("rollback application transaction after commit failure: %w", rollbackErr))
		}
		return commitErr
	}
	return nil
}

func (repository *PostgresRepository) PendingReferences(ctx context.Context, now time.Time, limit int32) ([]*domain.WagerTransaction, error) {
	rows, err := db.New(repository.pool).GetPendingReferenceTransactions(ctx, db.GetPendingReferenceTransactionsParams{
		NextReferenceAttemptAt: timestamp(now), Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("query pending reference wagers: %w", err)
	}
	transactions := make([]*domain.WagerTransaction, 0, len(rows))
	for _, row := range rows {
		wager, err := wagerFromDB(row, nil)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, wager)
	}
	return transactions, nil
}

func (repository *PostgresRepository) ClaimOutbox(ctx context.Context, now, staleBefore time.Time, limit int32) ([]application.OutboxRecord, error) {
	rows, err := db.New(repository.pool).ClaimOutboxEvents(ctx, db.ClaimOutboxEventsParams{
		NextAttemptAt: timestamp(now), ClaimedAt: timestamp(staleBefore), Limit: limit, ClaimedAt_2: timestamp(now),
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	events := make([]application.OutboxRecord, 0, len(rows))
	for _, row := range rows {
		events = append(events, application.OutboxRecord{
			ID: row.EventID, AggregateID: row.AggregateID, Type: domain.EventType(row.EventType),
			Version: int(row.EventVersion), OccurredAt: row.OccurredAt.Time.UTC(),
			Payload: append([]byte(nil), row.Payload...), Attempts: row.Attempts, CreatedAt: row.CreatedAt.Time.UTC(),
		})
	}
	return events, nil
}

func (repository *PostgresRepository) MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
	_, err := db.New(repository.pool).MarkOutboxPublished(ctx, db.MarkOutboxPublishedParams{
		EventID: eventID, PublishedAt: timestamp(publishedAt),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (repository *PostgresRepository) MarkOutboxFailed(ctx context.Context, eventID string, nextAttempt time.Time, failure string) error {
	_, err := db.New(repository.pool).MarkOutboxFailed(ctx, db.MarkOutboxFailedParams{
		EventID: eventID, NextAttemptAt: timestamp(nextAttempt), LastError: text(failure),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("reschedule outbox event: %w", err)
	}
	return nil
}

type postgresTransaction struct {
	queries *db.Queries
}

func (transaction *postgresTransaction) BeginInbox(ctx context.Context, message application.InboxMessage) (bool, error) {
	_, err := transaction.queries.InsertInboxMessage(ctx, db.InsertInboxMessageParams{
		ConsumerName: message.ConsumerName, MessageID: message.MessageID, MessageHash: message.MessageHash,
		MessagePayload: message.Payload, ReceivedAt: timestamp(time.Now().UTC()),
	})
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("insert inbox message: %w", err)
	}
	previous, err := transaction.queries.GetInboxMessage(ctx, db.GetInboxMessageParams{
		ConsumerName: message.ConsumerName, MessageID: message.MessageID,
	})
	if err != nil {
		return false, fmt.Errorf("load existing inbox message: %w", err)
	}
	if previous.MessageHash != message.MessageHash {
		return false, application.ErrInboxConflict
	}
	if !previous.CompletedAt.Valid {
		return false, application.ErrInboxIncomplete
	}
	return true, nil
}

func (transaction *postgresTransaction) CompleteInbox(ctx context.Context, message application.InboxMessage) error {
	_, err := transaction.queries.MarkInboxCompleted(ctx, db.MarkInboxCompletedParams{
		ConsumerName: message.ConsumerName, MessageID: message.MessageID, CompletedAt: timestamp(time.Now().UTC()),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) WalletByPlayerCurrency(ctx context.Context, playerID, currency string) (*domain.Wallet, error) {
	row, err := transaction.queries.GetWalletByPlayerCurrency(ctx, db.GetWalletByPlayerCurrencyParams{PlayerID: playerID, Currency: currency})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, application.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query wallet by player and currency: %w", err)
	}
	return walletFromDB(row)
}

func (transaction *postgresTransaction) WalletByID(ctx context.Context, walletID string) (*domain.Wallet, error) {
	row, err := transaction.queries.GetWallet(ctx, walletID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, application.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query wallet by ID: %w", err)
	}
	return walletFromDB(row)
}

func (transaction *postgresTransaction) WalletForUpdate(ctx context.Context, walletID string) (*domain.Wallet, error) {
	row, err := transaction.queries.GetWalletForUpdate(ctx, walletID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, application.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock wallet: %w", err)
	}
	return walletFromDB(row)
}

func walletFromDB(row db.Wallet) (*domain.Wallet, error) {
	balance, err := domain.MoneyFromMinorUnits(row.BalanceMinorUnits, row.Currency)
	if err != nil {
		return nil, err
	}
	wallet, err := domain.RehydrateWallet(row.ID, row.PlayerID, balance, row.Version, row.CreatedAt.Time, row.UpdatedAt.Time)
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (transaction *postgresTransaction) CreateWallet(ctx context.Context, wallet *domain.Wallet) error {
	_, err := transaction.queries.CreateWallet(ctx, db.CreateWalletParams{
		ID: wallet.ID(), PlayerID: wallet.PlayerID(), Currency: wallet.Balance().Currency(),
		BalanceMinorUnits: wallet.Balance().MinorUnits(), Version: wallet.Version(),
		CreatedAt: timestamp(wallet.CreatedAt()), UpdatedAt: timestamp(wallet.UpdatedAt()),
	})
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" && postgresError.ConstraintName == "wallets_player_currency_unique" {
			return fmt.Errorf("%w: %s", application.ErrWalletConflict, postgresError.ConstraintName)
		}
		return fmt.Errorf("insert wallet: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) WagerByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	row, err := transaction.queries.GetWagerTransactionByIdempotencyKey(ctx, db.GetWagerTransactionByIdempotencyKeyParams{
		ProviderID: text(providerID), IdempotencyKey: text(key),
	})
	return wagerFromDB(row, err)
}

func (transaction *postgresTransaction) WagerByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	row, err := transaction.queries.GetWagerTransactionByExternalID(ctx, db.GetWagerTransactionByExternalIDParams{
		ProviderID: text(providerID), ExternalTransactionID: text(externalID),
	})
	return wagerFromDB(row, err)
}

func (transaction *postgresTransaction) WagerByID(ctx context.Context, id string) (*domain.WagerTransaction, error) {
	row, err := transaction.queries.GetWagerTransaction(ctx, id)
	return wagerFromDB(row, err)
}

func (transaction *postgresTransaction) HasSuccessfulReversal(ctx context.Context, referenceID string) (bool, error) {
	reversed, err := transaction.queries.HasSuccessfulReversal(ctx, text(referenceID))
	if err != nil {
		return false, fmt.Errorf("check successful reversal: %w", err)
	}
	return reversed, nil
}

func (transaction *postgresTransaction) LedgerEntries(ctx context.Context, walletID string) ([]application.LedgerEntry, error) {
	rows, err := transaction.queries.GetLedgerEntriesForWallet(ctx, walletID)
	if err != nil {
		return nil, fmt.Errorf("read wallet ledger: %w", err)
	}
	entries := make([]application.LedgerEntry, 0, len(rows))
	for _, row := range rows {
		amount, err := domain.MoneyFromMinorUnits(row.AmountMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		before, err := domain.MoneyFromMinorUnits(row.BalanceBeforeMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		after, err := domain.MoneyFromMinorUnits(row.BalanceAfterMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		entries = append(entries, application.LedgerEntry{
			ID: row.ID, WalletID: row.WalletID, TransactionID: row.TransactionID,
			Direction: domain.LedgerDirection(row.Direction), Amount: amount,
			BalanceBefore: before, BalanceAfter: after, CreatedAt: row.CreatedAt.Time,
		})
	}
	return entries, nil
}

func (transaction *postgresTransaction) LedgerPage(ctx context.Context, walletID string, createdAt time.Time, cursorID string, limit int32) ([]application.LedgerEntry, error) {
	var cursorTime pgtype.Timestamptz
	if !createdAt.IsZero() {
		cursorTime = timestamp(createdAt)
	}
	rows, err := transaction.queries.GetLedgerPage(ctx, db.GetLedgerPageParams{
		WalletID: walletID, Column2: cursorTime, Column3: cursorID, Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("query wallet ledger page: %w", err)
	}
	entries := make([]application.LedgerEntry, 0, len(rows))
	for _, row := range rows {
		amount, err := domain.MoneyFromMinorUnits(row.AmountMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		before, err := domain.MoneyFromMinorUnits(row.BalanceBeforeMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		after, err := domain.MoneyFromMinorUnits(row.BalanceAfterMinorUnits, row.Currency)
		if err != nil {
			return nil, err
		}
		entries = append(entries, application.LedgerEntry{
			ID: row.ID, WalletID: row.WalletID, TransactionID: row.TransactionID,
			Direction: domain.LedgerDirection(row.Direction), Amount: amount,
			BalanceBefore: before, BalanceAfter: after, CreatedAt: row.CreatedAt.Time,
		})
	}
	return entries, nil
}

func wagerFromDB(row db.WagerTransaction, err error) (*domain.WagerTransaction, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, application.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query wager transaction: %w", err)
	}
	amount, err := domain.MoneyFromMinorUnits(row.AmountMinorUnits, row.Currency)
	if err != nil {
		return nil, err
	}
	var resultBalance domain.Money
	if row.ResultBalanceMinorUnits.Valid {
		resultBalance, err = domain.MoneyFromMinorUnits(row.ResultBalanceMinorUnits.Int64, row.ResultBalanceCurrency.String)
		if err != nil {
			return nil, err
		}
	}
	return domain.RehydrateWagerTransaction(domain.WagerTransactionData{
		ID: row.ID, ExternalID: row.ExternalTransactionID.String, ProviderID: row.ProviderID.String,
		IdempotencyKey: row.IdempotencyKey.String, WalletID: row.WalletID, PlayerID: row.PlayerID,
		RoundID: row.RoundID.String, GameID: row.GameID.String, Type: domain.WagerType(row.TransactionType),
		Amount: amount, ReferenceExternalID: row.ReferenceExternalTransactionID.String,
		ResolvedReferenceID: row.ResolvedReferenceID.String, PayloadHash: row.PayloadHash.String,
		FailureCode: row.FailureCode.String, Status: domain.WagerStatus(row.Status),
		ResultBalance: resultBalance, CreatedAt: row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time,
		ReferenceAttempts: row.ReferenceAttempts, NextReferenceAttemptAt: timeValue(row.NextReferenceAttemptAt),
	})
}

func (transaction *postgresTransaction) UpdateWallet(ctx context.Context, wallet *domain.Wallet) error {
	_, err := transaction.queries.UpdateWalletBalance(ctx, db.UpdateWalletBalanceParams{
		ID: wallet.ID(), BalanceMinorUnits: wallet.Balance().MinorUnits(), Version: wallet.Version(),
		UpdatedAt: timestamp(wallet.UpdatedAt()),
	})
	if err != nil {
		return fmt.Errorf("update wallet balance: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) UpdateWagerTransaction(ctx context.Context, wager *domain.WagerTransaction) error {
	data := wager.Data()
	_, err := transaction.queries.UpdateWagerTransactionState(ctx, db.UpdateWagerTransactionStateParams{
		ID: data.ID, Status: string(data.Status), FailureCode: text(data.FailureCode),
		ResolvedReferenceID:     text(data.ResolvedReferenceID),
		ResultBalanceMinorUnits: optionalInt(data.ResultBalance),
		ResultBalanceCurrency:   optionalMoneyCurrency(data.ResultBalance), ReferenceAttempts: data.ReferenceAttempts,
		NextReferenceAttemptAt: optionalTimestamp(data.NextReferenceAttemptAt), UpdatedAt: timestamp(data.UpdatedAt),
	})
	if err != nil {
		return fmt.Errorf("update wager state: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) CreateWagerTransaction(ctx context.Context, wager *domain.WagerTransaction) error {
	data := wager.Data()
	_, err := transaction.queries.CreateWagerTransaction(ctx, db.CreateWagerTransactionParams{
		ID: data.ID, SourceKind: sourceKind(data.Type),
		ExternalTransactionID: text(data.ExternalID), ProviderID: text(data.ProviderID),
		IdempotencyKey: text(data.IdempotencyKey), PayloadHash: text(data.PayloadHash),
		WalletID: data.WalletID, PlayerID: data.PlayerID, RoundID: text(data.RoundID), GameID: text(data.GameID),
		TransactionType: string(data.Type), AmountMinorUnits: data.Amount.MinorUnits(), Currency: data.Amount.Currency(),
		ReferenceExternalTransactionID: text(data.ReferenceExternalID), ResolvedReferenceID: text(data.ResolvedReferenceID),
		Status: string(data.Status), FailureCode: text(data.FailureCode),
		ResultBalanceMinorUnits: optionalInt(data.ResultBalance), ResultBalanceCurrency: optionalMoneyCurrency(data.ResultBalance),
		ReferenceAttempts: data.ReferenceAttempts, NextReferenceAttemptAt: optionalTimestamp(data.NextReferenceAttemptAt),
		CreatedAt: timestamp(data.CreatedAt), UpdatedAt: timestamp(data.UpdatedAt),
	})
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return fmt.Errorf("%w: %s", application.ErrUniqueConflict, postgresError.ConstraintName)
		}
		return fmt.Errorf("insert wager transaction: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) AppendLedger(ctx context.Context, entry application.LedgerEntry) error {
	_, err := transaction.queries.InsertLedgerEntry(ctx, db.InsertLedgerEntryParams{
		ID: entry.ID, WalletID: entry.WalletID, TransactionID: entry.TransactionID,
		Direction: string(entry.Direction), AmountMinorUnits: entry.Amount.MinorUnits(),
		Currency: entry.Amount.Currency(), BalanceBeforeMinorUnits: entry.BalanceBefore.MinorUnits(),
		BalanceAfterMinorUnits: entry.BalanceAfter.MinorUnits(), CreatedAt: timestamp(entry.CreatedAt),
	})
	if err != nil {
		return fmt.Errorf("insert wallet ledger entry: %w", err)
	}
	return nil
}

func (transaction *postgresTransaction) AppendOutbox(ctx context.Context, event application.OutboxEvent) error {
	_, err := transaction.queries.InsertOutboxEvent(ctx, db.InsertOutboxEventParams{
		EventID: event.ID, AggregateID: event.AggregateID, EventType: string(event.Type),
		EventVersion: int32(event.Version), Payload: event.Payload,
		OccurredAt: timestamp(event.OccurredAt), CreatedAt: timestamp(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func timestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func optionalTimestamp(value time.Time) pgtype.Timestamptz {
	if value.IsZero() {
		return pgtype.Timestamptz{}
	}
	return timestamp(value)
}

func timeValue(value pgtype.Timestamptz) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time
}

func text(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: value != ""}
}

func sourceKind(typ domain.WagerType) string {
	if typ == domain.WagerTypeOpening {
		return "INTERNAL"
	}
	return "EXTERNAL"
}

func optionalInt(money domain.Money) pgtype.Int8 {
	if money.Currency() == "" {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: money.MinorUnits(), Valid: true}
}

func optionalMoneyCurrency(money domain.Money) pgtype.Text {
	return text(money.Currency())
}

var _ application.Repository = (*PostgresRepository)(nil)
var _ application.Transaction = (*postgresTransaction)(nil)
