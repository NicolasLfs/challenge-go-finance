package db

import (
	"os"
	"strings"
	"testing"
)

func TestSQLContractContainsFinancialPersistenceModel(t *testing.T) {
	schema := readSQLContractFile(t, "schema.sql")
	query := readSQLContractFile(t, "query.sql")

	for _, table := range []string{
		"wallets",
		"wager_transactions",
		"wallet_ledger_entries",
		"wager_inbox",
		"wager_outbox",
	} {
		if !strings.Contains(schema, "CREATE TABLE IF NOT EXISTS "+table) {
			t.Errorf("schema is missing table %q", table)
		}
	}

	for _, constraint := range []string{
		"(player_id, currency)",
		"CHECK (balance_minor_units >= 0)",
		"(provider_id, external_transaction_id)",
		"(wallet_id, transaction_id)",
		"(consumer_name, message_id)",
	} {
		if !strings.Contains(schema, constraint) {
			t.Errorf("schema is missing constraint fragment %q", constraint)
		}
	}

	for _, queryName := range []string{
		"CreateWallet",
		"GetWalletForUpdate",
		"CreateWagerTransaction",
		"GetWagerTransactionByExternalID",
		"InsertLedgerEntry",
		"InsertInboxMessage",
		"ClaimOutboxEvents",
	} {
		if !strings.Contains(query, "-- name: "+queryName+" ") {
			t.Errorf("query file is missing %q", queryName)
		}
	}
}

func readSQLContractFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(content)
}
