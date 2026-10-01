package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"challenge/application"
	"challenge/repository"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestKeycloakAuthenticatedHTTPIsolation(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	issuer := os.Getenv("OIDC_ISSUER_URL")
	if databaseURL == "" || issuer == "" || os.Getenv("OIDC_TEST_PROVIDER_ALPHA_SECRET") == "" || os.Getenv("OIDC_TEST_PROVIDER_BETA_SECRET") == "" || os.Getenv("OIDC_TEST_INTERNAL_SECRET") == "" {
		t.Skip("PostgreSQL and Keycloak integration environment is not configured")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	alphaToken := getClientCredentialsToken(t, ctx, client, issuer, "provider-alpha", os.Getenv("OIDC_TEST_PROVIDER_ALPHA_SECRET"))
	betaToken := getClientCredentialsToken(t, ctx, client, issuer, "provider-beta", os.Getenv("OIDC_TEST_PROVIDER_BETA_SECRET"))
	internalToken := getClientCredentialsToken(t, ctx, client, issuer, "internal-service", os.Getenv("OIDC_TEST_INTERNAL_SECRET"))
	identity, err := NewOIDCAuthenticator(ctx, issuer, envOrDefault("OIDC_AUDIENCE", "wagering-api"), envOrDefault("OIDC_INTERNAL_CLIENT_ID", "internal-service"))
	if err != nil {
		t.Fatalf("create OIDC authenticator: %v", err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	ids := application.IDGeneratorFunc(func() (string, error) {
		return fmt.Sprintf("http-test-%d", time.Now().UnixNano()), nil
	})
	store := repository.NewPostgresRepository(pool)
	wallets := application.NewWalletService(store, ids)
	wagers := application.NewWagerService(store, ids)
	api := httptest.NewServer(NewServer(Dependencies{
		Wallets: wallets, Wagers: wagers, Reconciliation: application.NewReconciliationService(store),
		Authenticator: identity,
	}))
	t.Cleanup(api.Close)

	playerID := fmt.Sprintf("http-test-player-%d", time.Now().UnixNano())
	externalSuffix := fmt.Sprintf("%d", time.Now().UnixNano())
	alphaExternalID := "alpha-external-" + externalSuffix
	var created struct {
		ID string `json:"id"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wallets", internalToken,
		map[string]any{"playerId": playerID, "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}, http.StatusCreated, &created)
	if created.ID == "" {
		t.Fatal("wallet response omitted ID")
	}
	var readWallet struct {
		ID      string `json:"id"`
		Version int64  `json:"version"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/wallets/"+created.ID, internalToken, nil, http.StatusOK, &readWallet)
	if readWallet.ID != created.ID || readWallet.Version != 1 {
		t.Fatalf("internal wallet read = %#v", readWallet)
	}
	var ledger struct {
		Entries []json.RawMessage `json:"entries"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/wallets/"+created.ID+"/ledger", internalToken, nil, http.StatusOK, &ledger)
	if len(ledger.Entries) != 1 {
		t.Fatalf("opening ledger entries = %d, want 1", len(ledger.Entries))
	}
	var reconciliation struct {
		Consistent bool `json:"consistent"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wallets/"+created.ID+"/reconciliation", internalToken, map[string]any{}, http.StatusOK, &reconciliation)
	if !reconciliation.Consistent {
		t.Fatal("newly opened wallet did not reconcile")
	}
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wallets", alphaToken,
		map[string]any{"playerId": playerID + "-forbidden", "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, http.StatusForbidden, nil)
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/wallets/"+created.ID, alphaToken, nil, http.StatusForbidden, nil)
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wallets", "invalid-token",
		map[string]any{"playerId": playerID + "-invalid", "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, http.StatusUnauthorized, nil)

	var wagerResult struct {
		TransactionID    string `json:"transactionId"`
		Status           string `json:"status"`
		IdempotentReplay bool   `json:"idempotentReplay"`
	}
	wagerBody := map[string]any{
		"providerId": "provider-beta", "externalTransactionId": alphaExternalID,
		"playerId": playerID, "walletId": created.ID, "roundId": "round-http",
		"gameId": "game-http", "kind": "BET",
		"money": map[string]string{"amount": "10.00", "currency": "BRL"},
	}
	response := requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wagering/transactions", alphaToken, wagerBody, http.StatusOK, &wagerResult)
	if response.Header.Get("Content-Type") != "application/json" || wagerResult.TransactionID == "" || wagerResult.Status != "PROCESSED" {
		t.Fatalf("unexpected wager response: %#v", wagerResult)
	}
	var replay struct {
		TransactionID    string `json:"transactionId"`
		IdempotentReplay bool   `json:"idempotentReplay"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wagering/transactions", alphaToken, wagerBody, http.StatusOK, &replay)
	if replay.TransactionID != wagerResult.TransactionID || !replay.IdempotentReplay {
		t.Fatalf("provider replay crossed or reapplied the operation: %#v", replay)
	}
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/wagering/transactions/"+wagerResult.TransactionID, betaToken, nil, http.StatusNotFound, nil)
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/wagering/transactions/"+wagerResult.TransactionID, alphaToken, nil, http.StatusOK, nil)
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/providers/provider-alpha/wagering/transactions/"+alphaExternalID, betaToken, nil, http.StatusNotFound, nil)

	wagerBody["providerId"] = "provider-alpha"
	var betaOperation struct {
		TransactionID    string `json:"transactionId"`
		IdempotentReplay bool   `json:"idempotentReplay"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodPost, "/wagering/transactions", betaToken, wagerBody, http.StatusOK, &betaOperation)
	if betaOperation.TransactionID == wagerResult.TransactionID || betaOperation.IdempotentReplay {
		t.Fatalf("another provider received the first provider's replay: %#v", betaOperation)
	}
	var owner struct {
		ProviderID string `json:"providerId"`
	}
	requestJSON(t, ctx, client, api.URL, http.MethodGet, "/providers/provider-beta/wagering/transactions/"+alphaExternalID, betaToken, nil, http.StatusOK, &owner)
	if owner.ProviderID != "provider-beta" {
		t.Fatalf("persisted provider = %q, want authenticated provider-beta", owner.ProviderID)
	}

	response, err = client.Get(api.URL + "/health/live")
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("public liveness response = %v, error %v", response, err)
	}
	response.Body.Close()
	if strings.TrimSpace(alphaToken) == "" {
		t.Fatal("Keycloak returned an empty provider token")
	}
}

func getClientCredentialsToken(t *testing.T, ctx context.Context, client *http.Client, issuer, clientID, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(issuer, "/")+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("create token request: %v", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request token for %s: %v", clientID, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("token request for %s returned HTTP %d", clientID, response.StatusCode)
	}
	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || result.AccessToken == "" {
		t.Fatalf("decode token response for %s: %v", clientID, err)
	}
	return result.AccessToken
}

func requestJSON(t *testing.T, ctx context.Context, client *http.Client, baseURL, method, path, token string, body any, expectedStatus int, output any) *http.Response {
	t.Helper()
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("encode %s request: %v", path, err)
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("create %s request: %v", path, err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPost && path == "/wagering/transactions" {
		idempotencyKey := fmt.Sprintf("integration:%d", time.Now().UnixNano())
		if fields, ok := body.(map[string]any); ok {
			if externalID, ok := fields["externalTransactionId"].(string); ok {
				idempotencyKey = "integration:" + externalID
			}
		}
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("send %s %s: %v", method, path, err)
	}
	if response.StatusCode != expectedStatus {
		defer response.Body.Close()
		t.Fatalf("%s %s status = %d, want %d", method, path, response.StatusCode, expectedStatus)
	}
	if output != nil {
		defer response.Body.Close()
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
	} else {
		response.Body.Close()
	}
	return response
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
