-- name: GetHealthcheck :one
SELECT id, status FROM healthcheck LIMIT 1;

-- name: CreateWallet :one
INSERT INTO wallets (id, player_id, currency, balance_minor_units, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, player_id, currency, balance_minor_units, version, created_at, updated_at;

-- name: GetWallet :one
SELECT id, player_id, currency, balance_minor_units, version, created_at, updated_at
FROM wallets WHERE id = $1;

-- name: GetWalletForUpdate :one
SELECT id, player_id, currency, balance_minor_units, version, created_at, updated_at
FROM wallets WHERE id = $1 FOR UPDATE;

-- name: GetWalletByPlayerCurrency :one
SELECT id, player_id, currency, balance_minor_units, version, created_at, updated_at
FROM wallets WHERE player_id = $1 AND currency = $2;

-- name: UpdateWalletBalance :one
UPDATE wallets
SET balance_minor_units = $2, version = $3, updated_at = $4
WHERE id = $1
RETURNING id, player_id, currency, balance_minor_units, version, created_at, updated_at;

-- name: CreateWagerTransaction :one
INSERT INTO wager_transactions (
		id, source_kind, external_transaction_id, provider_id, idempotency_key, payload_hash,
		wallet_id, player_id, round_id, game_id, transaction_type, amount_minor_units, currency,
		reference_external_transaction_id, resolved_reference_id, status, failure_code,
		result_balance_minor_units, result_balance_currency, reference_attempts, next_reference_attempt_at,
		created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
RETURNING *;

-- name: GetWagerTransaction :one
SELECT * FROM wager_transactions WHERE id = $1;

-- name: GetWagerTransactionByExternalID :one
SELECT * FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2 FOR UPDATE;

-- name: GetWagerTransactionByIdempotencyKey :one
SELECT * FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2;

-- name: UpdateWagerTransactionState :one
UPDATE wager_transactions
SET status = $2, failure_code = $3, resolved_reference_id = $4,
		result_balance_minor_units = $5, result_balance_currency = $6,
		reference_attempts = $7, next_reference_attempt_at = $8, updated_at = $9
WHERE id = $1
RETURNING *;

-- name: GetPendingReferenceTransactions :many
SELECT * FROM wager_transactions
WHERE status = 'PENDING_REFERENCE'
	AND (next_reference_attempt_at IS NULL OR next_reference_attempt_at <= $1)
ORDER BY next_reference_attempt_at NULLS FIRST, created_at, id
LIMIT $2;

-- name: InsertLedgerEntry :one
INSERT INTO wallet_ledger_entries (
		id, wallet_id, transaction_id, direction, amount_minor_units, currency,
		balance_before_minor_units, balance_after_minor_units, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING *;

-- name: GetLedgerByWallet :many
SELECT * FROM wallet_ledger_entries
WHERE wallet_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2;

-- name: GetLedgerPage :many
SELECT * FROM wallet_ledger_entries
WHERE wallet_id = $1
	AND ($2::timestamptz IS NULL OR (created_at, id) < ($2::timestamptz, $3::text))
ORDER BY created_at DESC, id DESC
LIMIT $4;

-- name: GetLedgerEntriesForWallet :many
SELECT * FROM wallet_ledger_entries
WHERE wallet_id = $1
ORDER BY created_at, id;

-- name: HasSuccessfulReversal :one
SELECT EXISTS (
	SELECT 1 FROM wager_transactions
	WHERE resolved_reference_id = $1 AND status = 'PROCESSED'
);

-- name: InsertInboxMessage :one
INSERT INTO wager_inbox (consumer_name, message_id, message_hash, message_payload, received_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (consumer_name, message_id) DO NOTHING
RETURNING consumer_name, message_id, message_hash, message_payload, received_at, completed_at;

-- name: GetInboxMessage :one
SELECT consumer_name, message_id, message_hash, message_payload, received_at, completed_at
FROM wager_inbox WHERE consumer_name = $1 AND message_id = $2;

-- name: MarkInboxCompleted :one
UPDATE wager_inbox SET completed_at = $3
WHERE consumer_name = $1 AND message_id = $2
RETURNING consumer_name, message_id, message_hash, message_payload, received_at, completed_at;

-- name: InsertOutboxEvent :one
INSERT INTO wager_outbox (
		event_id, aggregate_id, event_type, event_version, payload, occurred_at,
		attempts, next_attempt_at, created_at
) VALUES ($1, $2, $3, $4, $5, $6, 0, $6, $7)
RETURNING *;

-- name: ClaimOutboxEvents :many
WITH candidates AS (
		SELECT event_id FROM wager_outbox AS pending_event
		WHERE pending_event.published_at IS NULL
			AND pending_event.next_attempt_at <= $1
			AND (pending_event.claimed_at IS NULL OR pending_event.claimed_at < $2)
		ORDER BY pending_event.next_attempt_at, pending_event.created_at, pending_event.event_id
		FOR UPDATE SKIP LOCKED
		LIMIT $3
)
UPDATE wager_outbox AS event
SET claimed_at = $4, attempts = event.attempts + 1
FROM candidates
WHERE event.event_id = candidates.event_id
RETURNING event.*;

-- name: MarkOutboxPublished :one
UPDATE wager_outbox
SET published_at = $2, claimed_at = NULL, last_error = NULL
WHERE event_id = $1 AND published_at IS NULL
RETURNING *;

-- name: MarkOutboxFailed :one
UPDATE wager_outbox
SET claimed_at = NULL, next_attempt_at = $2, last_error = $3
WHERE event_id = $1 AND published_at IS NULL
RETURNING *;

-- name: GetPendingOutboxEvents :many
SELECT * FROM wager_outbox
WHERE published_at IS NULL
ORDER BY next_attempt_at, created_at, event_id
LIMIT $1;
