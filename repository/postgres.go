package repository

import (
	"context"

	"challenge/db"

	"github.com/jackc/pgx/v5"
)

type Store struct {
	queries *db.Queries
}

func NewStore(connection db.DBTX) *Store {
	return &Store{queries: db.New(connection)}
}

func (store *Store) Queries() *db.Queries {
	return store.queries
}

func (store *Store) QueriesInTx(tx pgx.Tx) *db.Queries {
	return store.queries.WithTx(tx)
}

func (store *Store) InTx(ctx context.Context, tx pgx.Tx, fn func(*db.Queries) error) error {
	if err := fn(store.QueriesInTx(tx)); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return nil
}
