package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"challenge/application"
	"challenge/domain"
	"challenge/observability"
)

func TestProviderTransactionUsesAuthenticatedProviderAndRejectsOpening(t *testing.T) {
	wagers := &testWagers{}
	server := newTestServer(t, Principal{ProviderID: "provider-alpha"}, &testWallets{}, wagers)
	body := `{"providerId":"provider-beta","externalTransactionId":"external-1","playerId":"player-1","walletId":"wallet-1","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`
	request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer valid")
	request.Header.Set("Idempotency-Key", "provider-alpha:external-1")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want %d: %s", response.Code, http.StatusOK, response.Body)
	}
	if wagers.command.Wager.ProviderID != "provider-alpha" {
		t.Fatalf("provider ID passed to service = %q, want authenticated provider", wagers.command.Wager.ProviderID)
	}

	body = strings.Replace(body, `"kind":"BET"`, `"kind":"OPENING"`, 1)
	request = httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer valid")
	request.Header.Set("Idempotency-Key", "provider-alpha:external-2")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("external OPENING status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestProviderCannotAccessInternalWalletRoutes(t *testing.T) {
	server := newTestServer(t, Principal{ProviderID: "provider-alpha"}, &testWallets{}, &testWagers{})
	request := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(`{"playerId":"player-1","initialBalance":{"amount":"10.00","currency":"BRL"}}`))
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("POST /wallets status = %d, want %d", response.Code, http.StatusForbidden)
	}
	request = httptest.NewRequest(http.MethodGet, "/wallets/wallet-1", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("GET /wallets/:walletId status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestInternalIdentityCanOpenWallet(t *testing.T) {
	wallets := &testWallets{}
	server := newTestServer(t, Principal{Internal: true}, wallets, &testWagers{})
	request := httptest.NewRequest(http.MethodPost, "/wallets", strings.NewReader(`{"playerId":"player-1","initialBalance":{"amount":"10.00","currency":"BRL"}}`))
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("internal POST /wallets status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body)
	}
	if wallets.wallet == nil || wallets.wallet.Balance().Decimal() != "10.00" {
		t.Fatalf("internal wallet creation was not passed to the use case: %#v", wallets.wallet)
	}
}

func TestLedgerPaginationUsesOpaqueCursorAndValidatedLimit(t *testing.T) {
	wallet, err := domain.NewWallet("wallet-test", "player-1", mustMoney(t, "10.00"))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	wallets := &testWallets{
		wallet: wallet,
		ledgerEntries: []application.LedgerEntry{
			{ID: "ledger-b", WalletID: wallet.ID(), TransactionID: "tx-b", Direction: domain.LedgerDirectionCredit, Amount: mustMoney(t, "1.00"), BalanceBefore: mustMoney(t, "9.00"), BalanceAfter: mustMoney(t, "10.00"), CreatedAt: createdAt},
			{ID: "ledger-a", WalletID: wallet.ID(), TransactionID: "tx-a", Direction: domain.LedgerDirectionDebit, Amount: mustMoney(t, "1.00"), BalanceBefore: mustMoney(t, "10.00"), BalanceAfter: mustMoney(t, "9.00"), CreatedAt: createdAt.Add(-time.Second)},
		},
	}
	server := newTestServer(t, Principal{Internal: true}, wallets, &testWagers{})
	request := httptest.NewRequest(http.MethodGet, "/wallets/wallet-test/ledger?limit=1", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || wallets.ledgerLimit != 2 {
		t.Fatalf("ledger status/limit = %d/%d, want 200/2", response.Code, wallets.ledgerLimit)
	}
	var page ledgerResponse
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode ledger page: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].ID != "ledger-b" || page.NextCursor == "" {
		t.Fatalf("first ledger page = %#v", page)
	}
	cursor, err := decodeCursor(page.NextCursor)
	if err != nil || cursor.ID != "ledger-b" || !cursor.CreatedAt.Equal(createdAt) {
		t.Fatalf("decoded cursor = %#v, error %v", cursor, err)
	}
	request = httptest.NewRequest(http.MethodGet, "/wallets/wallet-test/ledger?limit=101", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range ledger limit status = %d, want 400", response.Code)
	}
}

func TestProviderTransactionReadsAreIsolated(t *testing.T) {
	wagers := &testWagers{transaction: testTransaction(t, "provider-beta", "external-1")}
	server := newTestServer(t, Principal{ProviderID: "provider-alpha"}, &testWallets{}, wagers)
	request := httptest.NewRequest(http.MethodGet, "/wagering/transactions/transaction-1", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-provider transaction lookup status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if strings.Contains(response.Body.String(), "provider-beta") {
		t.Fatalf("cross-provider response leaked owner: %s", response.Body)
	}

	request = httptest.NewRequest(http.MethodGet, "/providers/provider-beta/wagering/transactions/external-1", nil)
	request.Header.Set("Authorization", "Bearer valid")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-provider external lookup status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestHealthLiveIsPublicAndProtectedRoutesRejectMissingCredentials(t *testing.T) {
	server := newTestServer(t, Principal{}, &testWallets{}, &testWagers{})
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("liveness status = %d, want %d", response.Code, http.StatusOK)
	}
	response = httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("readiness status = %d, want %d", response.Code, http.StatusOK)
	}
	unready := NewServer(Dependencies{Readiness: testReadiness{unavailable: true}})
	response = httptest.NewRecorder()
	unready.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	response = httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/wallets/wallet-1", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated wallet lookup status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestMetricsEndpointIsPublicAndUsesPrometheusTextFormat(t *testing.T) {
	metrics := observability.NewMetrics()
	metrics.RecordWager("PROCESSED", true, time.Millisecond)
	server := NewServer(Dependencies{Metrics: metrics})
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("metrics status/content type = %d/%q", response.Code, response.Header().Get("Content-Type"))
	}
	if !strings.Contains(response.Body.String(), `wager_idempotent_replays_total 1`) {
		t.Fatalf("metrics endpoint omitted recorded replay: %s", response.Body.String())
	}
}

func TestReconciliationDivergenceIsLoggedAsStructuredJSON(t *testing.T) {
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, nil))
	metrics := observability.NewMetrics()
	server := NewServer(Dependencies{
		Reconciliation: testReconciliation{result: application.Reconciliation{
			WalletID: "wallet-1", StoredBalance: mustMoney(t, "10.00"),
			LedgerBalance: mustMoney(t, "9.00"), Difference: mustMoney(t, "1.00"), EntryCount: 2,
		}},
		Authenticator: testAuthenticator{principal: Principal{Internal: true}},
		Metrics:       metrics, Logger: logger,
	})
	request := httptest.NewRequest(http.MethodPost, "/wallets/wallet-1/reconciliation", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer valid")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("reconciliation status = %d: %s", response.Code, response.Body)
	}
	var reconciliationBody struct {
		Consistent bool `json:"consistent"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &reconciliationBody); err != nil || reconciliationBody.Consistent {
		t.Fatalf("reconciliation response = %s, error %v; want consistent=false", response.Body, err)
	}
	decoder := json.NewDecoder(&logOutput)
	decoder.UseNumber()
	var fields map[string]any
	if err := decoder.Decode(&fields); err != nil {
		t.Fatalf("decode reconciliation log: %v", err)
	}
	if fields["msg"] != "wallet reconciliation divergence" || fields["walletId"] != "wallet-1" || fields["differenceMinorUnits"] != json.Number("100") {
		t.Fatalf("reconciliation log fields = %#v", fields)
	}
	metricsOutput := httptest.NewRecorder()
	metrics.ServeHTTP(metricsOutput, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metricsOutput.Body.String(), "wallet_reconciliation_divergences_total 1") {
		t.Fatalf("reconciliation divergence metric was not incremented: %s", metricsOutput.Body.String())
	}
}

func newTestServer(t *testing.T, principal Principal, wallets WalletUseCases, wagers WagerUseCases) http.Handler {
	t.Helper()
	return NewServer(Dependencies{
		Wallets: wallets, Wagers: wagers, Reconciliation: testReconciliation{},
		Authenticator: testAuthenticator{principal: principal},
		Readiness:     testReadiness{},
	})
}

type testAuthenticator struct{ principal Principal }

func (authenticator testAuthenticator) Authenticate(_ context.Context, token string) (Principal, error) {
	if token != "valid" {
		return Principal{}, ErrUnauthenticated
	}
	return authenticator.principal, nil
}

type testReadiness struct{ unavailable bool }

func (readiness testReadiness) Ready(context.Context) error {
	if readiness.unavailable {
		return context.DeadlineExceeded
	}
	return nil
}

type testWallets struct {
	wallet        *domain.Wallet
	ledgerEntries []application.LedgerEntry
	ledgerCursor  *application.LedgerCursor
	ledgerLimit   int
}

func (wallets *testWallets) OpenWallet(_ context.Context, command application.OpenWalletCommand) (*domain.Wallet, error) {
	wallet, err := domain.NewWallet("wallet-test", command.PlayerID, command.Balance)
	if err != nil {
		return nil, err
	}
	wallets.wallet = wallet
	return wallet, nil
}
func (wallets *testWallets) GetWallet(context.Context, string) (*domain.Wallet, error) {
	if wallets.wallet == nil {
		return nil, application.ErrNotFound
	}
	return wallets.wallet, nil
}
func (wallets *testWallets) Ledger(_ context.Context, _ string, cursor *application.LedgerCursor, limit int) ([]application.LedgerEntry, error) {
	wallets.ledgerCursor = cursor
	wallets.ledgerLimit = limit
	if len(wallets.ledgerEntries) > limit {
		return wallets.ledgerEntries[:limit], nil
	}
	return wallets.ledgerEntries, nil
}

type testReconciliation struct{ result application.Reconciliation }

func (reconciliation testReconciliation) Reconcile(context.Context, string) (application.Reconciliation, error) {
	return reconciliation.result, nil
}

type testWagers struct {
	command     application.ProcessWagerCommand
	transaction *domain.WagerTransaction
}

func (wagers *testWagers) Process(_ context.Context, command application.ProcessWagerCommand) (application.WagerResult, error) {
	wagers.command = command
	return application.WagerResult{TransactionID: "transaction-1", Status: domain.WagerStatusProcessed}, nil
}
func (wagers *testWagers) GetByID(context.Context, string) (*domain.WagerTransaction, error) {
	if wagers.transaction == nil {
		return nil, application.ErrNotFound
	}
	return wagers.transaction, nil
}
func (wagers *testWagers) GetByExternalID(context.Context, string, string) (*domain.WagerTransaction, error) {
	if wagers.transaction == nil {
		return nil, application.ErrNotFound
	}
	return wagers.transaction, nil
}

func testTransaction(t *testing.T, providerID, externalID string) *domain.WagerTransaction {
	t.Helper()
	amount, err := domain.ParseMoney("25.00", "BRL")
	if err != nil {
		t.Fatalf("parse test money: %v", err)
	}
	wager, err := domain.NewExternalWagerTransaction(domain.ExternalWagerInput{
		ID: "transaction-1", ExternalID: externalID, ProviderID: providerID,
		IdempotencyKey: "key-1", WalletID: "wallet-1", PlayerID: "player-1",
		RoundID: "round-1", GameID: "game-1", Type: domain.WagerTypeBet,
		Amount: amount, PayloadHash: "hash",
	})
	if err != nil {
		t.Fatalf("create test transaction: %v", err)
	}
	if err := wager.MarkProcessed(amount); err != nil {
		t.Fatalf("process test transaction: %v", err)
	}
	return wager
}

func mustMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	money, err := domain.ParseMoney(amount, "BRL")
	if err != nil {
		t.Fatalf("parse test money: %v", err)
	}
	return money
}
