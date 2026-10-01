package repository

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"challenge/application"
	"challenge/db"
	"challenge/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreInTxRollsBackAllChangesOnCallbackError(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	store := NewStore(pool)
	walletID := "integration-wallet-atomicity"
	_, err = pool.Exec(ctx, "DELETE FROM wallets WHERE id = $1", walletID)
	if err != nil {
		t.Fatalf("clean integration wallet: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM wallets WHERE id = $1", walletID)
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	sentinel := errors.New("forced failure after wallet insert")
	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	err = store.InTx(ctx, tx, func(queries *db.Queries) error {
		_, err := queries.CreateWallet(ctx, db.CreateWalletParams{
			ID:                walletID,
			PlayerID:          "integration-player",
			Currency:          "BRL",
			BalanceMinorUnits: 10000,
			Version:           1,
			CreatedAt:         now,
			UpdatedAt:         now,
		})
		if err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}

	_, err = store.Queries().GetWallet(ctx, walletID)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("wallet was committed after rollback, got error: %v", err)
	}
}

func TestPostgresApplicationUseCasesAndConcurrentWalletUpdates(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	runID := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	store := NewPostgresRepository(pool)
	walletService := application.NewWalletService(store, integrationIDs(runID+"-open"))
	initialBalance := mustIntegrationMoney(t, "100.00")
	wallet, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{PlayerID: runID + "-player", Balance: initialBalance})
	if err != nil {
		t.Fatalf("open positive-balance wallet: %v", err)
	}
	if _, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{PlayerID: runID + "-player", Balance: initialBalance}); !errors.Is(err, application.ErrWalletConflict) {
		t.Fatalf("duplicate wallet error = %v, want ErrWalletConflict", err)
	}
	var openingCount, openingLedgerCount, openingEventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND transaction_type = 'OPENING'`, wallet.ID()).Scan(&openingCount); err != nil {
		t.Fatalf("count opening transactions: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, wallet.ID()).Scan(&openingLedgerCount); err != nil {
		t.Fatalf("count opening ledger entries: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_outbox WHERE aggregate_id = $1`, wallet.ID()).Scan(&openingEventCount); err != nil {
		t.Fatalf("count opening outbox events: %v", err)
	}
	if openingCount != 1 || openingLedgerCount != 1 || openingEventCount != 2 {
		t.Fatalf("opening records = transaction %d, ledger %d, outbox %d", openingCount, openingLedgerCount, openingEventCount)
	}

	wagerAmount, _ := domain.ParseMoney("80.00", "BRL")
	services := []*application.WagerService{
		application.NewWagerService(store, integrationIDs(runID+"-a")),
		application.NewWagerService(store, integrationIDs(runID+"-b")),
	}
	commands := []application.ProcessWagerCommand{
		integrationWager(runID, "bet-a", "key-a", wallet.ID(), wallet.PlayerID(), wagerAmount),
		integrationWager(runID, "bet-b", "key-b", wallet.ID(), wallet.PlayerID(), wagerAmount),
	}
	start := make(chan struct{})
	results := make([]application.WagerResult, len(services))
	errorsByRequest := make([]error, len(services))
	var wait sync.WaitGroup
	for index := range services {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results[index], errorsByRequest[index] = services[index].Process(ctx, commands[index])
		}(index)
	}
	close(start)
	wait.Wait()
	processed, rejected := 0, 0
	processedIndex := -1
	for index, result := range results {
		if errorsByRequest[index] != nil {
			t.Fatalf("concurrent wager %d: %v", index, errorsByRequest[index])
		}
		switch result.Status {
		case domain.WagerStatusProcessed:
			processed++
			processedIndex = index
		case domain.WagerStatusRejected:
			if result.FailureCode != "INSUFFICIENT_BALANCE" {
				t.Fatalf("rejected wager failure code = %q", result.FailureCode)
			}
			rejected++
		default:
			t.Fatalf("unexpected wager status %s", result.Status)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed/rejected = %d/%d, want 1/1", processed, rejected)
	}
	var balance int64
	var debitCount int
	if err := pool.QueryRow(ctx, `SELECT balance_minor_units FROM wallets WHERE id = $1`, wallet.ID()).Scan(&balance); err != nil {
		t.Fatalf("read concurrent wallet balance: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.ID()).Scan(&debitCount); err != nil {
		t.Fatalf("count concurrent debits: %v", err)
	}
	if balance != 2000 || debitCount != 1 {
		t.Fatalf("concurrent wallet balance/debits = %d/%d, want 2000/1", balance, debitCount)
	}
	reconciliation, err := application.NewReconciliationService(store).Reconcile(ctx, wallet.ID())
	if err != nil || reconciliation.Difference.MinorUnits() != 0 || reconciliation.EntryCount != 2 {
		t.Fatalf("wallet reconciliation = %#v, error %v", reconciliation, err)
	}
	replay, err := services[processedIndex].Process(ctx, commands[processedIndex])
	if err != nil || !replay.IdempotentReplay || replay.Balance.MinorUnits() != 2000 {
		t.Fatalf("persisted wager replay = %#v, error %v", replay, err)
	}

	zeroBalance := mustIntegrationMoney(t, "0.00")
	zeroWallet, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{PlayerID: runID + "-zero-player", Balance: zeroBalance})
	if err != nil {
		t.Fatalf("open zero-balance wallet: %v", err)
	}
	var zeroOpeningCount, zeroLedgerCount, zeroOutboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, zeroWallet.ID()).Scan(&zeroOpeningCount); err != nil {
		t.Fatalf("count zero-balance transactions: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, zeroWallet.ID()).Scan(&zeroLedgerCount); err != nil {
		t.Fatalf("count zero-balance ledger entries: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_outbox WHERE aggregate_id = $1`, zeroWallet.ID()).Scan(&zeroOutboxCount); err != nil {
		t.Fatalf("count zero-balance outbox events: %v", err)
	}
	if zeroOpeningCount != 0 || zeroLedgerCount != 0 || zeroOutboxCount != 0 {
		t.Fatalf("zero opening records = transaction %d, ledger %d, outbox %d", zeroOpeningCount, zeroLedgerCount, zeroOutboxCount)
	}

	reversalWallet, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{PlayerID: runID + "-reversal-player", Balance: initialBalance})
	if err != nil {
		t.Fatalf("open reversal wallet: %v", err)
	}
	reversalServices := []*application.WagerService{
		application.NewWagerService(store, integrationIDs(runID+"-reversal-a")),
		application.NewWagerService(store, integrationIDs(runID+"-reversal-b")),
	}
	referencedBet := integrationWager(runID, "reversal-bet", "reversal-bet-key", reversalWallet.ID(), reversalWallet.PlayerID(), mustIntegrationMoney(t, "20.00"))
	if _, err := reversalServices[0].Process(ctx, referencedBet); err != nil {
		t.Fatalf("process wager to be reversed: %v", err)
	}
	reversals := []application.ProcessWagerCommand{
		integrationWager(runID, "reversal-refund", "reversal-refund-key", reversalWallet.ID(), reversalWallet.PlayerID(), mustIntegrationMoney(t, "20.00")),
		integrationWager(runID, "reversal-rollback", "reversal-rollback-key", reversalWallet.ID(), reversalWallet.PlayerID(), mustIntegrationMoney(t, "20.00")),
	}
	reversals[0].Wager.Type = domain.WagerTypeRefund
	reversals[1].Wager.Type = domain.WagerTypeRollback
	for index := range reversals {
		reversals[index].Wager.ReferenceExternalID = "reversal-bet"
	}
	reversalResults := make([]application.WagerResult, len(reversals))
	reversalErrors := make([]error, len(reversals))
	startReversals := make(chan struct{})
	for index := range reversals {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-startReversals
			reversalResults[index], reversalErrors[index] = reversalServices[index].Process(ctx, reversals[index])
		}(index)
	}
	close(startReversals)
	wait.Wait()
	processedReversals, rejectedReversals := 0, 0
	for index, result := range reversalResults {
		if reversalErrors[index] != nil {
			t.Fatalf("concurrent reversal %d: %v", index, reversalErrors[index])
		}
		switch result.Status {
		case domain.WagerStatusProcessed:
			processedReversals++
		case domain.WagerStatusRejected:
			if result.FailureCode != "REFERENCE_ALREADY_REVERSED" {
				t.Fatalf("concurrent reversal failure code = %q", result.FailureCode)
			}
			rejectedReversals++
		}
	}
	if processedReversals != 1 || rejectedReversals != 1 || reversalResults[0].Balance.MinorUnits()+reversalResults[1].Balance.MinorUnits() != 20000 {
		t.Fatalf("concurrent reversal outcomes = %#v", reversalResults)
	}

	referenceWallet, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{PlayerID: runID + "-reference-player", Balance: initialBalance})
	if err != nil {
		t.Fatalf("open reference wallet: %v", err)
	}
	referenceService := application.NewWagerService(store, integrationIDs(runID+"-reference-a"))
	refundCommand := integrationWager(runID, "refund-before-bet", "refund-key", referenceWallet.ID(), referenceWallet.PlayerID(), mustIntegrationMoney(t, "10.00"))
	refundCommand.Wager.Type = domain.WagerTypeRefund
	refundCommand.Wager.ReferenceExternalID = "late-reference-bet"
	pending, err := referenceService.Process(ctx, refundCommand)
	if err != nil || pending.Status != domain.WagerStatusPendingReference {
		t.Fatalf("refund before reference = %#v, error %v", pending, err)
	}
	lateBet := integrationWager(runID, "late-reference-bet", "late-bet-key", referenceWallet.ID(), referenceWallet.PlayerID(), mustIntegrationMoney(t, "10.00"))
	if _, err := referenceService.Process(ctx, lateBet); err != nil {
		t.Fatalf("process late reference bet: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_reference_attempt_at = now() - interval '1 second' WHERE id = $1`, pending.TransactionID); err != nil {
		t.Fatalf("make pending reference due: %v", err)
	}
	restartedService := application.NewWagerService(store, integrationIDs(runID+"-reference-b"))
	if err := restartedService.RetryPendingReferences(ctx, 10); err != nil {
		t.Fatalf("resume pending reference using a new service instance: %v", err)
	}
	var resolvedStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, pending.TransactionID).Scan(&resolvedStatus); err != nil {
		t.Fatalf("read resumed refund status: %v", err)
	}
	if resolvedStatus != string(domain.WagerStatusProcessed) {
		t.Fatalf("resumed refund status = %s, want PROCESSED", resolvedStatus)
	}
}

func TestPostgresIndependentProcessesPreserveFinancialInvariants(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	runID := fmt.Sprintf("distributed-%d", time.Now().UnixNano())
	store := NewPostgresRepository(pool)
	wallets := application.NewWalletService(store, integrationIDs(runID+"-wallet"))
	initialBalance := mustIntegrationMoney(t, "100.00")

	duplicateWallet, err := wallets.OpenWallet(ctx, application.OpenWalletCommand{
		PlayerID: runID + "-duplicate-player", Balance: initialBalance,
	})
	if err != nil {
		t.Fatalf("open duplicate-test wallet: %v", err)
	}
	duplicateCommands := make([]processWagerWorker, 3)
	for index, repetitions := range []int{17, 17, 16} {
		duplicateCommands[index] = processWagerWorker{
			Command: processWagerPayload{
				RunID: runID, ExternalID: "same-bet", IdempotencyKey: "same-key",
				WalletID: duplicateWallet.ID(), PlayerID: duplicateWallet.PlayerID(),
				Amount: "25.00", Kind: string(domain.WagerTypeBet),
			}, Repetitions: repetitions,
		}
	}
	duplicateResults := runPostgresProcessWorkers(t, duplicateCommands)
	processedCount, replayCount := countProcessWagerResults(t, duplicateResults)
	if processedCount != 1 || replayCount != 49 {
		t.Fatalf("50 distributed submissions produced %d original results and %d replays, want 1 and 49", processedCount, replayCount)
	}
	assertPostgresWalletState(t, ctx, pool, duplicateWallet.ID(), 7500, 1)
	var duplicateTransactions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, runID+"-provider", "same-bet").Scan(&duplicateTransactions); err != nil {
		t.Fatalf("count duplicate wager records: %v", err)
	}
	if duplicateTransactions != 1 {
		t.Fatalf("duplicate wager rows = %d, want 1", duplicateTransactions)
	}

	competingWallet, err := wallets.OpenWallet(ctx, application.OpenWalletCommand{
		PlayerID: runID + "-competing-player", Balance: initialBalance,
	})
	if err != nil {
		t.Fatalf("open competing-test wallet: %v", err)
	}
	competingCommands := []processWagerWorker{
		{Command: processWagerPayload{RunID: runID, ExternalID: "bet-a", IdempotencyKey: "key-a", WalletID: competingWallet.ID(), PlayerID: competingWallet.PlayerID(), Amount: "80.00", Kind: string(domain.WagerTypeBet)}, Repetitions: 1},
		{Command: processWagerPayload{RunID: runID, ExternalID: "bet-b", IdempotencyKey: "key-b", WalletID: competingWallet.ID(), PlayerID: competingWallet.PlayerID(), Amount: "80.00", Kind: string(domain.WagerTypeBet)}, Repetitions: 1},
	}
	competingResults := runPostgresProcessWorkers(t, competingCommands)
	processedCount, rejectedCount := countProcessWagerStatuses(t, competingResults, domain.WagerStatusProcessed, domain.WagerStatusRejected)
	if processedCount != 1 || rejectedCount != 1 {
		t.Fatalf("competing distributed bets produced %d processed and %d rejected, want 1 each", processedCount, rejectedCount)
	}
	for _, result := range flattenProcessWagerResults(competingResults) {
		if result.Status == string(domain.WagerStatusRejected) && result.FailureCode != "INSUFFICIENT_BALANCE" {
			t.Fatalf("competing wager rejection code = %q, want INSUFFICIENT_BALANCE", result.FailureCode)
		}
	}
	assertPostgresWalletState(t, ctx, pool, competingWallet.ID(), 2000, 1)

	independentCommands := make([]processWagerWorker, 3)
	independentWalletIDs := make([]string, len(independentCommands))
	for index := range independentCommands {
		wallet, err := wallets.OpenWallet(ctx, application.OpenWalletCommand{
			PlayerID: fmt.Sprintf("%s-independent-player-%d", runID, index), Balance: mustIntegrationMoney(t, "10.00"),
		})
		if err != nil {
			t.Fatalf("open independent wallet %d: %v", index, err)
		}
		independentWalletIDs[index] = wallet.ID()
		independentCommands[index] = processWagerWorker{
			Command: processWagerPayload{
				RunID: runID, ExternalID: fmt.Sprintf("independent-bet-%d", index),
				IdempotencyKey: fmt.Sprintf("independent-key-%d", index), WalletID: wallet.ID(),
				PlayerID: wallet.PlayerID(), Amount: "1.00", Kind: string(domain.WagerTypeBet),
			}, Repetitions: 1,
		}
	}
	independentResults := runPostgresProcessWorkers(t, independentCommands)
	for index, result := range flattenProcessWagerResults(independentResults) {
		if result.Status != string(domain.WagerStatusProcessed) || result.Replay {
			t.Fatalf("independent wallet worker %d result = %#v, want a new processed wager", index, result)
		}
	}
	for _, walletID := range independentWalletIDs {
		assertPostgresWalletState(t, ctx, pool, walletID, 900, 1)
	}
}

func TestPostgresIndependentProcessWorker(t *testing.T) {
	if os.Getenv("WAGERING_PROCESS_WORKER") != "1" {
		return
	}
	var worker processWagerWorker
	if err := json.Unmarshal([]byte(os.Getenv("WAGERING_PROCESS_COMMAND")), &worker); err != nil {
		t.Fatalf("decode process worker command: %v", err)
	}
	if worker.Repetitions < 1 {
		t.Fatalf("process worker repetitions = %d, want positive", worker.Repetitions)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		t.Fatalf("wait for process worker start signal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("create independent process pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping independent process pool: %v", err)
	}
	amount, err := domain.ParseMoney(worker.Command.Amount, "BRL")
	if err != nil {
		t.Fatalf("parse process worker money: %v", err)
	}
	command := integrationWager(worker.Command.RunID, worker.Command.ExternalID, worker.Command.IdempotencyKey, worker.Command.WalletID, worker.Command.PlayerID, amount)
	command.Wager.Type = domain.WagerType(worker.Command.Kind)
	command.Wager.ReferenceExternalID = worker.Command.ReferenceExternalID
	service := application.NewWagerService(NewPostgresRepository(pool), integrationIDs(fmt.Sprintf("%s-process-%d", worker.Command.RunID, os.Getpid())))
	results := make([]processWagerResult, worker.Repetitions)
	errorsByRequest := make([]error, worker.Repetitions)
	var wait sync.WaitGroup
	start := make(chan struct{})
	for index := range results {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			result, err := service.Process(ctx, command)
			if err != nil {
				errorsByRequest[index] = err
				return
			}
			results[index] = processWagerResult{
				Status: string(result.Status), FailureCode: result.FailureCode,
				Replay: result.IdempotentReplay, Balance: result.Balance.Decimal(),
			}
		}(index)
	}
	close(start)
	wait.Wait()
	for index, err := range errorsByRequest {
		if err != nil {
			t.Fatalf("process wager request %d: %v", index, err)
		}
	}
	encoded, err := json.Marshal(results)
	if err != nil {
		t.Fatalf("encode process worker results: %v", err)
	}
	fmt.Printf("PROCESS_RESULT:%s\n", encoded)
}

type processWagerPayload struct {
	RunID               string `json:"runId"`
	ExternalID          string `json:"externalId"`
	IdempotencyKey      string `json:"idempotencyKey"`
	WalletID            string `json:"walletId"`
	PlayerID            string `json:"playerId"`
	Amount              string `json:"amount"`
	Kind                string `json:"kind"`
	ReferenceExternalID string `json:"referenceExternalId,omitempty"`
}

type processWagerWorker struct {
	Command     processWagerPayload `json:"command"`
	Repetitions int                 `json:"repetitions"`
}

type processWagerResult struct {
	Status      string `json:"status"`
	FailureCode string `json:"failureCode,omitempty"`
	Replay      bool   `json:"replay"`
	Balance     string `json:"balance"`
}

func runPostgresProcessWorkers(t *testing.T, workers []processWagerWorker) [][]processWagerResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test executable: %v", err)
	}
	type runningWorker struct {
		command *exec.Cmd
		input   io.WriteCloser
		output  bytes.Buffer
		error   bytes.Buffer
	}
	running := make([]*runningWorker, len(workers))
	for index, worker := range workers {
		encoded, err := json.Marshal(worker)
		if err != nil {
			t.Fatalf("encode process worker %d: %v", index, err)
		}
		command := exec.CommandContext(ctx, executable, "-test.v", "-test.run=^TestPostgresIndependentProcessWorker$")
		command.Env = append(os.Environ(), "WAGERING_PROCESS_WORKER=1", "WAGERING_PROCESS_COMMAND="+string(encoded))
		input, err := command.StdinPipe()
		if err != nil {
			t.Fatalf("open process worker %d input: %v", index, err)
		}
		workerProcess := &runningWorker{command: command, input: input}
		command.Stdout = &workerProcess.output
		command.Stderr = &workerProcess.error
		if err := command.Start(); err != nil {
			t.Fatalf("start independent process %d: %v", index, err)
		}
		running[index] = workerProcess
	}
	for index, worker := range running {
		if _, err := worker.input.Write([]byte("start\n")); err != nil {
			t.Fatalf("release process worker %d: %v", index, err)
		}
		if err := worker.input.Close(); err != nil {
			t.Fatalf("close process worker %d input: %v", index, err)
		}
	}
	results := make([][]processWagerResult, len(workers))
	for index, worker := range running {
		if err := worker.command.Wait(); err != nil {
			t.Fatalf("independent process %d failed: %v\nstdout: %s\nstderr: %s", index, err, worker.output.String(), worker.error.String())
		}
		marker := "PROCESS_RESULT:"
		output := worker.output.String()
		markerIndex := strings.LastIndex(output, marker)
		if markerIndex < 0 {
			t.Fatalf("independent process %d omitted result: %s", index, output)
		}
		resultLine := strings.SplitN(strings.TrimSpace(output[markerIndex+len(marker):]), "\n", 2)[0]
		if err := json.Unmarshal([]byte(resultLine), &results[index]); err != nil {
			t.Fatalf("decode independent process %d result: %v; output: %s", index, err, output)
		}
		if len(results[index]) != workers[index].Repetitions {
			t.Fatalf("independent process %d returned %d results, want %d", index, len(results[index]), workers[index].Repetitions)
		}
	}
	return results
}

func countProcessWagerResults(t *testing.T, results [][]processWagerResult) (processed, replays int) {
	t.Helper()
	for _, result := range flattenProcessWagerResults(results) {
		if result.Status != string(domain.WagerStatusProcessed) {
			t.Fatalf("distributed duplicate status = %s, want PROCESSED", result.Status)
		}
		if result.Replay {
			replays++
		} else {
			processed++
		}
	}
	return processed, replays
}

func countProcessWagerStatuses(t *testing.T, results [][]processWagerResult, first, second domain.WagerStatus) (firstCount, secondCount int) {
	t.Helper()
	for _, result := range flattenProcessWagerResults(results) {
		switch result.Status {
		case string(first):
			firstCount++
		case string(second):
			secondCount++
		default:
			t.Fatalf("distributed wager status = %s, want %s or %s", result.Status, first, second)
		}
	}
	return firstCount, secondCount
}

func flattenProcessWagerResults(results [][]processWagerResult) []processWagerResult {
	flattened := make([]processWagerResult, 0)
	for _, workerResults := range results {
		flattened = append(flattened, workerResults...)
	}
	return flattened
}

func assertPostgresWalletState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, walletID string, balanceMinorUnits int64, debitCount int) {
	t.Helper()
	var balance int64
	var actualDebits int
	if err := pool.QueryRow(ctx, `SELECT balance_minor_units FROM wallets WHERE id = $1`, walletID).Scan(&balance); err != nil {
		t.Fatalf("read wallet %s balance: %v", walletID, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID).Scan(&actualDebits); err != nil {
		t.Fatalf("count wallet %s debits: %v", walletID, err)
	}
	if balance != balanceMinorUnits || actualDebits != debitCount {
		t.Fatalf("wallet %s balance/debits = %d/%d, want %d/%d", walletID, balance, actualDebits, balanceMinorUnits, debitCount)
	}
}

func TestPostgresLedgerKeysetPaginationIsStable(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	runID := fmt.Sprintf("ledger-page-%d", time.Now().UnixNano())
	store := NewPostgresRepository(pool)
	walletService := application.NewWalletService(store, integrationIDs(runID+"-wallet"))
	wallet, err := walletService.OpenWallet(ctx, application.OpenWalletCommand{
		PlayerID: runID + "-player", Balance: mustIntegrationMoney(t, "10.00"),
	})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	wagerService := application.NewWagerService(store, integrationIDs(runID+"-wager"))
	for index, amount := range []string{"1.00", "2.00"} {
		command := integrationWager(runID, fmt.Sprintf("win-%d", index), fmt.Sprintf("key-%d", index), wallet.ID(), wallet.PlayerID(), mustIntegrationMoney(t, amount))
		command.Wager.Type = domain.WagerTypeWin
		if _, err := wagerService.Process(ctx, command); err != nil {
			t.Fatalf("process credit %d: %v", index, err)
		}
	}
	firstPage, err := walletService.Ledger(ctx, wallet.ID(), nil, 2)
	if err != nil {
		t.Fatalf("read first ledger page: %v", err)
	}
	if len(firstPage) != 2 || firstPage[0].ID == firstPage[1].ID || firstPage[0].CreatedAt.Before(firstPage[1].CreatedAt) {
		t.Fatalf("first page is not stable descending order: %#v", firstPage)
	}
	cursor := &application.LedgerCursor{CreatedAt: firstPage[1].CreatedAt, ID: firstPage[1].ID}
	secondPage, err := walletService.Ledger(ctx, wallet.ID(), cursor, 2)
	if err != nil {
		t.Fatalf("read second ledger page: %v", err)
	}
	if len(secondPage) != 1 || secondPage[0].ID == firstPage[0].ID || secondPage[0].ID == firstPage[1].ID {
		t.Fatalf("second page repeated or omitted cursor boundary: %#v", secondPage)
	}
}

func TestPostgresInboxCommitsWithFinancialEffectsAndOutboxClaimsRecover(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	runID := fmt.Sprintf("inbox-outbox-%d", time.Now().UnixNano())
	store := NewPostgresRepository(pool)
	wallet, err := application.NewWalletService(store, integrationIDs(runID+"-wallet")).OpenWallet(ctx, application.OpenWalletCommand{
		PlayerID: runID + "-player", Balance: mustIntegrationMoney(t, "100.00"),
	})
	if err != nil {
		t.Fatalf("open integration wallet: %v", err)
	}
	command := integrationWager(runID, "external-1", "key-1", wallet.ID(), wallet.PlayerID(), mustIntegrationMoney(t, "25.00"))
	service := application.NewWagerService(store, integrationIDs(runID+"-wager"))
	inbox := application.InboxMessage{ConsumerName: "wager-transactions", MessageID: runID + "-message", MessageHash: "sha256-message-hash", Payload: []byte(`{"messageId":"integration"}`)}
	first, err := service.ProcessMessage(ctx, command, inbox)
	if err != nil || first.Status != domain.WagerStatusProcessed {
		t.Fatalf("process message: result %#v, error %v", first, err)
	}
	var inboxHash string
	var completedAt pgtype.Timestamptz
	if err := pool.QueryRow(ctx, `SELECT message_hash, completed_at FROM wager_inbox WHERE consumer_name = $1 AND message_id = $2`, inbox.ConsumerName, inbox.MessageID).Scan(&inboxHash, &completedAt); err != nil {
		t.Fatalf("read completed inbox record: %v", err)
	}
	if inboxHash != inbox.MessageHash || !completedAt.Valid {
		t.Fatalf("inbox state = hash %q, completed %v", inboxHash, completedAt.Valid)
	}
	second, err := service.ProcessMessage(ctx, command, inbox)
	if err != nil || !second.IdempotentReplay {
		t.Fatalf("replay message: result %#v, error %v", second, err)
	}
	var debitCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wallet.ID()).Scan(&debitCount); err != nil {
		t.Fatalf("count committed debit: %v", err)
	}
	if debitCount != 1 {
		t.Fatalf("debit count = %d, want 1", debitCount)
	}
	inbox.MessageHash = "different-hash"
	if _, err := service.ProcessMessage(ctx, command, inbox); !errors.Is(err, application.ErrInboxConflict) {
		t.Fatalf("conflicting inbox hash error = %v", err)
	}

	eventID := runID + "-claimed-event"
	schemaName := "outbox_test_" + strings.ReplaceAll(runID, "-", "_")
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create isolated outbox test schema: %v", err)
	}
	claimPoolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database configuration for outbox schema: %v", err)
	}
	claimPoolConfig.ConnConfig.RuntimeParams["search_path"] = schemaName
	claimPool, err := pgxpool.NewWithConfig(ctx, claimPoolConfig)
	if err != nil {
		t.Fatalf("create isolated outbox pool: %v", err)
	}
	defer func() {
		claimPool.Close()
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	}()
	if _, err := claimPool.Exec(ctx, `CREATE TABLE wager_outbox (
		event_id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
		event_version INTEGER NOT NULL, payload JSONB NOT NULL, occurred_at TIMESTAMPTZ NOT NULL,
		attempts INTEGER NOT NULL, next_attempt_at TIMESTAMPTZ NOT NULL, claimed_at TIMESTAMPTZ,
		published_at TIMESTAMPTZ, last_error TEXT, created_at TIMESTAMPTZ NOT NULL
	)`); err != nil {
		t.Fatalf("create isolated outbox table: %v", err)
	}
	if _, err := claimPool.Exec(ctx, `INSERT INTO wager_outbox (event_id, aggregate_id, event_type, event_version, payload, occurred_at, attempts, next_attempt_at, created_at) VALUES ($1, $2, $3, 1, $4::jsonb, now(), 0, now(), now())`, eventID, wallet.ID(), string(domain.EventTypeWagerTransactionProcessed), `{"transactionId":"`+runID+`-event-tx"}`); err != nil {
		t.Fatalf("insert outbox event for claim test: %v", err)
	}
	claimRepository := NewPostgresRepository(claimPool)
	claimNow := time.Now().UTC()
	claimed := make([][]application.OutboxRecord, 2)
	claimErrors := make([]error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range claimed {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			claimed[index], claimErrors[index] = claimRepository.ClaimOutbox(ctx, claimNow, claimNow.Add(-time.Minute), 10)
		}(index)
	}
	close(start)
	wait.Wait()
	claimants := 0
	for index := range claimed {
		if claimErrors[index] != nil {
			t.Fatalf("claim outbox batch %d: %v", index, claimErrors[index])
		}
		for _, event := range claimed[index] {
			if event.ID == eventID {
				claimants++
			}
		}
	}
	if claimants != 1 {
		t.Fatalf("outbox event claimed by %d concurrent publishers, want exactly one", claimants)
	}
	if _, err := claimPool.Exec(ctx, `UPDATE wager_outbox SET claimed_at = now() - interval '2 minutes' WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("age outbox claim: %v", err)
	}
	recovered, err := claimRepository.ClaimOutbox(ctx, claimNow, claimNow.Add(-time.Minute), 10)
	if err != nil {
		t.Fatalf("recover abandoned outbox claim: %v", err)
	}
	recoveredClaim := false
	for _, event := range recovered {
		recoveredClaim = recoveredClaim || event.ID == eventID
	}
	if !recoveredClaim {
		t.Fatal("publisher did not recover the abandoned outbox claim")
	}
}

func integrationIDs(prefix string) application.IDGenerator {
	var counter atomic.Uint64
	return application.IDGeneratorFunc(func() (string, error) {
		return fmt.Sprintf("%s-%d", prefix, counter.Add(1)), nil
	})
}

func integrationWager(runID, externalID, key, walletID, playerID string, amount domain.Money) application.ProcessWagerCommand {
	return application.ProcessWagerCommand{Wager: domain.ExternalWagerInput{
		ExternalID: externalID, ProviderID: runID + "-provider", IdempotencyKey: key,
		WalletID: walletID, PlayerID: playerID, RoundID: runID + "-round", GameID: "integration-game",
		Type: domain.WagerTypeBet, Amount: amount,
	}}
}

func mustIntegrationMoney(t *testing.T, amount string) domain.Money {
	t.Helper()
	money, err := domain.ParseMoney(amount, "BRL")
	if err != nil {
		t.Fatalf("parse integration money %q: %v", amount, err)
	}
	return money
}
