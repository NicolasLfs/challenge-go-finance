package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"challenge/application"
	"challenge/domain"
)

type WalletUseCases interface {
	OpenWallet(context.Context, application.OpenWalletCommand) (*domain.Wallet, error)
	GetWallet(context.Context, string) (*domain.Wallet, error)
	Ledger(context.Context, string, *application.LedgerCursor, int) ([]application.LedgerEntry, error)
}

type ReconciliationUseCases interface {
	Reconcile(context.Context, string) (application.Reconciliation, error)
}

type WagerUseCases interface {
	Process(context.Context, application.ProcessWagerCommand) (application.WagerResult, error)
	GetByID(context.Context, string) (*domain.WagerTransaction, error)
	GetByExternalID(context.Context, string, string) (*domain.WagerTransaction, error)
}

type Readiness interface {
	Ready(context.Context) error
}

type Metrics interface {
	http.Handler
	RecordWager(string, bool, time.Duration)
	RecordConflict()
	RecordReconciliation(bool)
}

type Dependencies struct {
	Wallets        WalletUseCases
	Wagers         WagerUseCases
	Reconciliation ReconciliationUseCases
	Authenticator  Authenticator
	Readiness      Readiness
	Metrics        Metrics
	Logger         *slog.Logger
}

type Server struct {
	wallets        WalletUseCases
	wagers         WagerUseCases
	reconciliation ReconciliationUseCases
	authenticator  Authenticator
	readiness      Readiness
	metrics        Metrics
	logger         *slog.Logger
	handler        http.Handler
}

type principalContextKey struct{}

func NewServer(dependencies Dependencies) *Server {
	server := &Server{
		wallets: dependencies.Wallets, wagers: dependencies.Wagers, reconciliation: dependencies.Reconciliation,
		authenticator: dependencies.Authenticator, readiness: dependencies.Readiness, metrics: dependencies.Metrics,
		logger: dependencies.Logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", server.live)
	mux.HandleFunc("GET /health/ready", server.ready)
	mux.Handle("GET /metrics", server.metricsHandler())
	mux.Handle("POST /wallets", server.protected(true, http.HandlerFunc(server.openWallet)))
	mux.Handle("GET /wallets/{walletID}", server.protected(true, http.HandlerFunc(server.getWallet)))
	mux.Handle("GET /wallets/{walletID}/ledger", server.protected(true, http.HandlerFunc(server.getLedger)))
	mux.Handle("POST /wallets/{walletID}/reconciliation", server.protected(true, http.HandlerFunc(server.reconcile)))
	mux.Handle("POST /wagering/transactions", server.protected(false, http.HandlerFunc(server.processWager)))
	mux.Handle("GET /wagering/transactions/{transactionID}", server.protected(false, http.HandlerFunc(server.getWager)))
	mux.Handle("GET /providers/{providerID}/wagering/transactions/{externalTransactionID}", server.protected(false, http.HandlerFunc(server.getWagerByExternalID)))
	server.handler = mux
	return server
}

func (server *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	server.handler.ServeHTTP(writer, request)
}

func (server *Server) protected(internalOnly bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		header := strings.Fields(request.Header.Get("Authorization"))
		if len(header) != 2 || !strings.EqualFold(header[0], "Bearer") || server.authenticator == nil {
			writer.Header().Set("WWW-Authenticate", `Bearer realm="wagering-api"`)
			writeError(writer, http.StatusUnauthorized, "UNAUTHENTICATED", "valid bearer token required")
			return
		}
		principal, err := server.authenticator.Authenticate(request.Context(), header[1])
		if err != nil {
			writer.Header().Set("WWW-Authenticate", `Bearer realm="wagering-api"`)
			writeError(writer, http.StatusUnauthorized, "UNAUTHENTICATED", "valid bearer token required")
			return
		}
		if (internalOnly && !principal.Internal) || (!internalOnly && !principal.Internal && principal.ProviderID == "") {
			writeError(writer, http.StatusForbidden, "FORBIDDEN", "identity is not authorized for this operation")
			return
		}
		next.ServeHTTP(writer, request.WithContext(context.WithValue(request.Context(), principalContextKey{}, principal)))
	})
}

func (server *Server) live(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "live"})
}

func (server *Server) metricsHandler() http.Handler {
	if server.metrics == nil {
		return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writeError(writer, http.StatusServiceUnavailable, "METRICS_UNAVAILABLE", "metrics are unavailable")
		})
	}
	return server.metrics
}

func (server *Server) ready(writer http.ResponseWriter, request *http.Request) {
	if server.readiness == nil {
		writeError(writer, http.StatusServiceUnavailable, "NOT_READY", "readiness dependencies are unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := server.readiness.Ready(ctx); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "NOT_READY", "a required dependency is unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func (server *Server) openWallet(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		PlayerID       string `json:"playerId"`
		InitialBalance struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"initialBalance"`
	}
	if err := decodeJSON(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "request body is invalid")
		return
	}
	amount, err := domain.ParseMoney(body.InitialBalance.Amount, body.InitialBalance.Currency)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "initialBalance is invalid")
		return
	}
	wallet, err := server.wallets.OpenWallet(request.Context(), application.OpenWalletCommand{PlayerID: body.PlayerID, Balance: amount})
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, walletResponse(wallet))
}

func (server *Server) getWallet(writer http.ResponseWriter, request *http.Request) {
	wallet, err := server.wallets.GetWallet(request.Context(), request.PathValue("walletID"))
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, walletResponse(wallet))
}

func (server *Server) getLedger(writer http.ResponseWriter, request *http.Request) {
	limit := 50
	if rawLimit := request.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	cursor, err := decodeCursor(request.URL.Query().Get("cursor"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "cursor is invalid")
		return
	}
	entries, err := server.wallets.Ledger(request.Context(), request.PathValue("walletID"), cursor, limit+1)
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	response := ledgerResponse{Entries: make([]ledgerEntryResponse, 0, len(entries))}
	for _, entry := range entries {
		response.Entries = append(response.Entries, ledgerEntryResponse{
			ID: entry.ID, WalletID: entry.WalletID, TransactionID: entry.TransactionID,
			Direction: entry.Direction, Amount: moneyResponse(entry.Amount),
			BalanceBefore: moneyResponse(entry.BalanceBefore), BalanceAfter: moneyResponse(entry.BalanceAfter),
			CreatedAt: entry.CreatedAt.UTC(),
		})
	}
	if hasMore {
		last := entries[len(entries)-1]
		response.NextCursor = encodeCursor(application.LedgerCursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	writeJSON(writer, http.StatusOK, response)
}

func (server *Server) reconcile(writer http.ResponseWriter, request *http.Request) {
	result, err := server.reconciliation.Reconcile(request.Context(), request.PathValue("walletID"))
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	if server.metrics != nil {
		server.metrics.RecordReconciliation(result.Difference.MinorUnits() == 0)
	}
	if result.Difference.MinorUnits() != 0 && server.logger != nil {
		server.logger.WarnContext(request.Context(), "wallet reconciliation divergence",
			"walletId", result.WalletID, "currency", result.Difference.Currency(),
			"differenceMinorUnits", result.Difference.MinorUnits(), "checkedEntries", result.EntryCount,
		)
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"walletId": result.WalletID, "storedBalance": moneyResponse(result.StoredBalance),
		"calculatedBalance": moneyResponse(result.LedgerBalance), "difference": moneyResponse(result.Difference),
		"consistent": result.Difference.MinorUnits() == 0, "checkedEntries": result.EntryCount,
	})
}

func (server *Server) processWager(writer http.ResponseWriter, request *http.Request) {
	if strings.TrimSpace(request.Header.Get("Idempotency-Key")) == "" {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "Idempotency-Key header is required")
		return
	}
	var body struct {
		ProviderID                     string `json:"providerId"`
		ExternalTransactionID          string `json:"externalTransactionId"`
		PlayerID                       string `json:"playerId"`
		WalletID                       string `json:"walletId"`
		RoundID                        string `json:"roundId"`
		GameID                         string `json:"gameId"`
		Kind                           string `json:"kind"`
		ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
		Money                          struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"money"`
	}
	if err := decodeJSON(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "request body is invalid")
		return
	}
	wagerType := domain.WagerType(body.Kind)
	if wagerType == domain.WagerTypeOpening {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "OPENING is reserved for internal wallet creation")
		return
	}
	amount, err := domain.ParseMoney(body.Money.Amount, body.Money.Currency)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "money is invalid")
		return
	}
	principal := request.Context().Value(principalContextKey{}).(Principal)
	if principal.Internal {
		writeError(writer, http.StatusForbidden, "FORBIDDEN", "internal identity cannot submit provider transactions")
		return
	}
	processingStarted := time.Now()
	result, err := server.wagers.Process(request.Context(), application.ProcessWagerCommand{Wager: domain.ExternalWagerInput{
		ProviderID: principal.ProviderID, ExternalID: body.ExternalTransactionID,
		IdempotencyKey: request.Header.Get("Idempotency-Key"), PlayerID: body.PlayerID,
		WalletID: body.WalletID, RoundID: body.RoundID, GameID: body.GameID,
		Type: wagerType, Amount: amount, ReferenceExternalID: body.ReferenceExternalTransactionID,
	}})
	if server.metrics != nil {
		if err != nil {
			if errors.Is(err, application.ErrIdempotencyConflict) || errors.Is(err, application.ErrExternalTransactionConflict) || errors.Is(err, application.ErrUniqueConflict) {
				server.metrics.RecordConflict()
			}
		} else {
			server.metrics.RecordWager(string(result.Status), result.IdempotentReplay, time.Since(processingStarted))
		}
	}
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	status := http.StatusOK
	if result.Status == domain.WagerStatusRejected {
		status = http.StatusUnprocessableEntity
	} else if result.Status == domain.WagerStatusPending || result.Status == domain.WagerStatusPendingReference {
		status = http.StatusAccepted
	} else if result.Status == domain.WagerStatusFailed {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, map[string]any{
		"transactionId": result.TransactionID, "status": result.Status,
		"failureCode": optionalString(result.FailureCode), "balance": moneyResponse(result.Balance),
		"idempotentReplay": result.IdempotentReplay,
	})
}

func (server *Server) getWager(writer http.ResponseWriter, request *http.Request) {
	wager, err := server.wagers.GetByID(request.Context(), request.PathValue("transactionID"))
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	principal := request.Context().Value(principalContextKey{}).(Principal)
	if !principal.Internal && wager.ProviderID() != principal.ProviderID {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "transaction not found")
		return
	}
	writeJSON(writer, http.StatusOK, wagerResponse(wager))
}

func (server *Server) getWagerByExternalID(writer http.ResponseWriter, request *http.Request) {
	principal := request.Context().Value(principalContextKey{}).(Principal)
	providerID := request.PathValue("providerID")
	if !principal.Internal && providerID != principal.ProviderID {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "transaction not found")
		return
	}
	wager, err := server.wagers.GetByExternalID(request.Context(), providerID, request.PathValue("externalTransactionID"))
	if err != nil {
		server.writeUseCaseError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, wagerResponse(wager))
}

func (server *Server) writeUseCaseError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, application.ErrNotFound):
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, application.ErrWalletConflict), errors.Is(err, application.ErrIdempotencyConflict), errors.Is(err, application.ErrExternalTransactionConflict), errors.Is(err, application.ErrUniqueConflict):
		writeError(writer, http.StatusConflict, "CONFLICT", "request conflicts with an existing resource")
	case errors.Is(err, domain.ErrInvalidMoney), errors.Is(err, domain.ErrNegativeMoney), errors.Is(err, domain.ErrMoneyOverflow), errors.Is(err, domain.ErrCurrencyMismatch), errors.Is(err, domain.ErrInvalidWallet), errors.Is(err, domain.ErrInvalidWager), errors.Is(err, domain.ErrInvalidTransition):
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "request violates a domain constraint")
	default:
		writeError(writer, http.StatusServiceUnavailable, "UNAVAILABLE", "request could not be completed")
	}
}

type walletResponseBody struct {
	ID       string    `json:"id"`
	PlayerID string    `json:"playerId"`
	Balance  moneyBody `json:"balance"`
	Version  int64     `json:"version"`
}

type moneyBody struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func walletResponse(wallet *domain.Wallet) walletResponseBody {
	return walletResponseBody{ID: wallet.ID(), PlayerID: wallet.PlayerID(), Balance: moneyResponse(wallet.Balance()), Version: wallet.Version()}
}

func moneyResponse(money domain.Money) moneyBody {
	return moneyBody{Amount: money.Decimal(), Currency: money.Currency()}
}

func optionalString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

type ledgerEntryResponse struct {
	ID            string                 `json:"id"`
	WalletID      string                 `json:"walletId"`
	TransactionID string                 `json:"transactionId"`
	Direction     domain.LedgerDirection `json:"direction"`
	Amount        moneyBody              `json:"amount"`
	BalanceBefore moneyBody              `json:"balanceBefore"`
	BalanceAfter  moneyBody              `json:"balanceAfter"`
	CreatedAt     time.Time              `json:"createdAt"`
}

type ledgerCursorBody struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

func encodeCursor(cursor application.LedgerCursor) string {
	encoded, _ := json.Marshal(ledgerCursorBody{CreatedAt: cursor.CreatedAt.UTC(), ID: cursor.ID})
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeCursor(raw string) (*application.LedgerCursor, error) {
	if raw == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var value ledgerCursorBody
	if err := json.Unmarshal(decoded, &value); err != nil || value.ID == "" || value.CreatedAt.IsZero() {
		return nil, errors.New("invalid ledger cursor")
	}
	return &application.LedgerCursor{CreatedAt: value.CreatedAt.UTC(), ID: value.ID}, nil
}

func wagerResponse(wager *domain.WagerTransaction) map[string]any {
	data := wager.Data()
	response := map[string]any{
		"transactionId": data.ID, "externalTransactionId": optionalString(data.ExternalID),
		"providerId": optionalString(data.ProviderID), "walletId": data.WalletID, "playerId": data.PlayerID,
		"roundId": optionalString(data.RoundID), "gameId": optionalString(data.GameID),
		"kind": data.Type, "money": moneyResponse(data.Amount), "status": data.Status,
		"failureCode": optionalString(data.FailureCode), "referenceExternalTransactionId": optionalString(data.ReferenceExternalID),
		"createdAt": data.CreatedAt, "updatedAt": data.UpdatedAt,
	}
	if data.ResultBalance.Currency() != "" {
		response["balance"] = moneyResponse(data.ResultBalance)
	}
	return response
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing request data")
	}
	return nil
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]string{"code": code, "message": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
