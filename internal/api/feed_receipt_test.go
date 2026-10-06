package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/store"
)

func doRequest(t *testing.T, server *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, reader))
	return recorder
}

func requestPermit(t *testing.T, server *Server) repair.Permit {
	t.Helper()
	response := doRequest(t, server, http.MethodPost, "/api/landings/east/feed-isolations", IsolationRequest{CircuitID: "B"})
	if response.Code != http.StatusAccepted {
		t.Fatalf("isolation status=%d body=%s", response.Code, response.Body.String())
	}
	var permit repair.Permit
	if err := json.Unmarshal(response.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}
	return permit
}

func receiptCount(t *testing.T, journal *store.Journal) int {
	t.Helper()
	records, err := journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range records {
		if record.Kind == "feed-receipt" {
			count++
		}
	}
	return count
}

// A persistence failure must surface as a 5xx without making the receipt
// observable: permit evaluation keeps failing and the receipt stays unlisted.
// After storage recovers, the retried submission succeeds, replays are
// idempotent, and evaluation then reaches the groundable state.
func TestFeedReceiptFailureCompensationAndIdempotentRetry(t *testing.T) {
	server, services := testServerWithServices(t)
	permit := requestPermit(t, server)

	// A regular file standing where the journal directory should be makes every
	// append fail at MkdirAll.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	services.Journal = store.NewJournal(filepath.Join(blocker, "events.jsonl"))

	failed := doRequest(t, server, http.MethodPost,
		"/api/landings/east/feed-receipts",
		ReceiptRequest{CircuitID: "B", OperationID: permit.OperationID, Isolated: true})
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("receipt status=%d body=%s, want 500", failed.Code, failed.Body.String())
	}

	// The error response must not change the evaluable state: no receipt is
	// visible and the permit stays in the isolating state.
	listed := doRequest(t, server, http.MethodGet, "/api/landings/east/feed-receipts", nil)
	if listed.Code != http.StatusOK {
		t.Fatalf("list receipts status=%d body=%s", listed.Code, listed.Body.String())
	}
	var listedBody struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &listedBody); err != nil {
		t.Fatal(err)
	}
	if len(listedBody.Items) != 0 {
		t.Fatalf("receipts should be empty after failed submission, got %s", listed.Body.String())
	}
	evaluate := doRequest(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluate.Code != http.StatusConflict {
		t.Fatalf("evaluate status=%d body=%s, want 409 while receipt is unpersisted", evaluate.Code, evaluate.Body.String())
	}
	fetched := doRequest(t, server, http.MethodGet, "/api/permits/"+permit.ID, nil)
	var current repair.Permit
	if err := json.Unmarshal(fetched.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.State != repair.PermitIsolating {
		t.Fatalf("permit state=%s, want isolating after failed receipt", current.State)
	}

	// Storage recovers; the same submission is retried and now commits.
	services.Journal = store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	retried := doRequest(t, server, http.MethodPost,
		"/api/landings/east/feed-receipts",
		ReceiptRequest{CircuitID: "B", OperationID: permit.OperationID, Isolated: true})
	if retried.Code != http.StatusCreated {
		t.Fatalf("retried receipt status=%d body=%s", retried.Code, retried.Body.String())
	}
	firstBody := retried.Body.String()

	// Idempotent replay: same identity, same response payload, no extra journal
	// record.
	replay := doRequest(t, server, http.MethodPost,
		"/api/landings/east/feed-receipts",
		ReceiptRequest{CircuitID: "B", OperationID: permit.OperationID, Isolated: true})
	if replay.Code != http.StatusCreated || replay.Body.String() != firstBody {
		t.Fatalf("replay status=%d body=%s, want 201 with %q", replay.Code, replay.Body.String(), firstBody)
	}
	if count := receiptCount(t, services.Journal); count != 1 {
		t.Fatalf("feed-receipt journal records=%d, want 1", count)
	}

	// The committed receipt now makes the permit groundable.
	evaluated := doRequest(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluated.Code != http.StatusOK {
		t.Fatalf("evaluate status=%d body=%s, want 200 after committed receipt", evaluated.Code, evaluated.Body.String())
	}
	if err := json.Unmarshal(evaluated.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.State != repair.PermitGroundable {
		t.Fatalf("permit state=%s, want groundable", current.State)
	}
}

// A receipt that is committed with isolated=false must remain visible on retry
// (the first-committed payload wins) and must not flip the permit to
// groundable.
func TestFeedReceiptReplayPreservesFirstCommittedPayload(t *testing.T) {
	server, services := testServerWithServices(t)
	permit := requestPermit(t, server)

	payload := ReceiptRequest{CircuitID: "B", OperationID: permit.OperationID, Isolated: true}
	first := doRequest(t, server, http.MethodPost, "/api/landings/east/feed-receipts", payload)
	if first.Code != http.StatusCreated {
		t.Fatalf("receipt status=%d body=%s", first.Code, first.Body.String())
	}

	// A contradictory retry cannot overwrite the committed receipt.
	contradictory := payload
	contradictory.Isolated = false
	replay := doRequest(t, server, http.MethodPost, "/api/landings/east/feed-receipts", contradictory)
	if replay.Code != http.StatusCreated || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay body=%s, want unchanged %q", replay.Body.String(), first.Body.String())
	}
	if count := receiptCount(t, services.Journal); count != 1 {
		t.Fatalf("feed-receipt journal records=%d, want 1", count)
	}
}
