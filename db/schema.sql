CREATE TABLE IF NOT EXISTS healthcheck (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallets (
    id TEXT PRIMARY KEY,
    player_id TEXT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_minor_units BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor_units >= 0),
    CONSTRAINT wallets_version_positive CHECK (version >= 1),
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

CREATE TABLE IF NOT EXISTS wager_transactions (
    id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    external_transaction_id TEXT,
    provider_id TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    wallet_id TEXT NOT NULL REFERENCES wallets(id),
    player_id TEXT NOT NULL,
    round_id TEXT,
    game_id TEXT,
    transaction_type TEXT NOT NULL,
    amount_minor_units BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    reference_external_transaction_id TEXT,
    resolved_reference_id TEXT REFERENCES wager_transactions(id),
    status TEXT NOT NULL,
    failure_code TEXT,
    result_balance_minor_units BIGINT,
    result_balance_currency CHAR(3),
    reference_attempts INTEGER NOT NULL DEFAULT 0,
    next_reference_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wager_source_kind_valid CHECK (source_kind IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT wager_type_valid CHECK (transaction_type IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT wager_status_valid CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT wager_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wager_amount_non_negative CHECK (amount_minor_units >= 0),
    CONSTRAINT wager_result_balance_non_negative CHECK (result_balance_minor_units IS NULL OR result_balance_minor_units >= 0),
    CONSTRAINT wager_source_metadata CHECK (
        (source_kind = 'INTERNAL' AND transaction_type = 'OPENING' AND external_transaction_id IS NULL AND provider_id IS NULL AND idempotency_key IS NULL AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL AND reference_external_transaction_id IS NULL)
        OR
        (source_kind = 'EXTERNAL' AND transaction_type <> 'OPENING' AND external_transaction_id IS NOT NULL AND provider_id IS NOT NULL AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wager_result_currency_pair CHECK ((result_balance_minor_units IS NULL) = (result_balance_currency IS NULL)),
    CONSTRAINT wager_result_currency_matches CHECK (result_balance_currency IS NULL OR result_balance_currency = currency)
);

CREATE UNIQUE INDEX IF NOT EXISTS wager_external_identity_unique
    ON wager_transactions (provider_id, external_transaction_id)
    WHERE source_kind = 'EXTERNAL';
CREATE UNIQUE INDEX IF NOT EXISTS wager_idempotency_unique
    ON wager_transactions (provider_id, idempotency_key)
    WHERE source_kind = 'EXTERNAL';
CREATE UNIQUE INDEX IF NOT EXISTS wager_opening_wallet_unique
    ON wager_transactions (wallet_id)
    WHERE source_kind = 'INTERNAL' AND transaction_type = 'OPENING';
CREATE INDEX IF NOT EXISTS wager_reference_lookup_idx
    ON wager_transactions (provider_id, external_transaction_id, status);
CREATE INDEX IF NOT EXISTS wager_pending_reference_idx
    ON wager_transactions (next_reference_attempt_at)
    WHERE status = 'PENDING_REFERENCE';

CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
    id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL REFERENCES wallets(id),
    transaction_id TEXT NOT NULL REFERENCES wager_transactions(id),
    direction TEXT NOT NULL,
    amount_minor_units BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_before_minor_units BIGINT NOT NULL,
    balance_after_minor_units BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledger_direction_valid CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ledger_amount_positive CHECK (amount_minor_units > 0),
    CONSTRAINT ledger_balances_non_negative CHECK (balance_before_minor_units >= 0 AND balance_after_minor_units >= 0),
    CONSTRAINT ledger_wallet_transaction_unique UNIQUE (wallet_id, transaction_id)
);
CREATE INDEX IF NOT EXISTS wallet_ledger_wallet_created_idx
    ON wallet_ledger_entries (wallet_id, created_at, id);

CREATE OR REPLACE FUNCTION prevent_ledger_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet ledger is append-only';
END;
$$;

DROP TRIGGER IF EXISTS wallet_ledger_no_update ON wallet_ledger_entries;
CREATE TRIGGER wallet_ledger_no_update
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TABLE IF NOT EXISTS wager_inbox (
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    message_hash TEXT NOT NULL,
    message_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE IF NOT EXISTS wager_outbox (
    event_id TEXT PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_version INTEGER NOT NULL DEFAULT 1,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT outbox_version_positive CHECK (event_version >= 1)
);
CREATE INDEX IF NOT EXISTS wager_outbox_pending_idx
    ON wager_outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL;
