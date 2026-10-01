DROP TABLE IF EXISTS wager_outbox;
DROP TABLE IF EXISTS wager_inbox;
DROP TRIGGER IF EXISTS wallet_ledger_no_update ON wallet_ledger_entries;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS prevent_ledger_mutation();
DROP TABLE IF EXISTS healthcheck;