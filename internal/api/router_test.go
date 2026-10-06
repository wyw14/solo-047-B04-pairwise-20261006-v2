package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	return testServerAt(t, t.TempDir())
}

func testServerAt(t *testing.T, dataDir string) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(dataDir, "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func doJSON(t *testing.T, server *Server, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, target, reader))
	return recorder
}

func startIsolation(t *testing.T, server *Server) repair.Permit {
	t.Helper()
	recorder := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-isolations", IsolationRequest{CircuitID: "B"})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("start isolation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var permit repair.Permit
	if err := json.Unmarshal(recorder.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}
	return permit
}

// A journal whose directory is blocked by a regular file fails every append
// before writing anything.
func brokenJournal(t *testing.T) *store.Journal {
	t.Helper()
	blocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return store.NewJournal(filepath.Join(blocker, "events.jsonl"))
}

func TestRecordReceiptPersistFailureLeavesPermitNotEvaluable(t *testing.T) {
	dataDir := t.TempDir()
	server := testServerAt(t, dataDir)
	permit := startIsolation(t, server)
	healthyJournal := server.services.Journal

	// Storage fails while the receipt is being committed.
	server.services.Journal = brokenJournal(t)
	failed := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", ReceiptRequest{
		CircuitID: permit.CircuitID, OperationID: permit.OperationID, Isolated: true,
	})
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("failed receipt status=%d body=%s, want 500", failed.Code, failed.Body.String())
	}

	// The failed receipt must not be observable anywhere.
	if _, matched := server.services.Receipts.Match("east", permit.CircuitID, permit.OperationID); matched {
		t.Fatal("unpersisted receipt is matchable after persistence failure")
	}
	listed := doJSON(t, server, http.MethodGet, "/api/landings/east/feed-receipts", nil)
	var items struct {
		Items []landing.IsolationReceipt `json:"items"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items.Items) != 0 {
		t.Fatalf("unpersisted receipt leaked into listing: %+v", items.Items)
	}
	evaluation := doJSON(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluation.Code != http.StatusConflict {
		t.Fatalf("evaluation status=%d body=%s, want 409 without a durable receipt", evaluation.Code, evaluation.Body.String())
	}
	stored, exists := server.services.Permits.Get(permit.ID)
	if !exists || stored.State != repair.PermitIsolating {
		t.Fatalf("permit state changed to %+v, want %s", stored, repair.PermitIsolating)
	}

	// Storage recovers: the identical retry commits and makes the permit
	// evaluable exactly once.
	server.services.Journal = healthyJournal
	retry := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", ReceiptRequest{
		CircuitID: permit.CircuitID, OperationID: permit.OperationID, Isolated: true,
	})
	if retry.Code != http.StatusCreated {
		t.Fatalf("retried receipt status=%d body=%s, want 201", retry.Code, retry.Body.String())
	}
	evaluation = doJSON(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluation.Code != http.StatusOK {
		t.Fatalf("evaluation after retry status=%d body=%s", evaluation.Code, evaluation.Body.String())
	}
	var evaluated repair.Permit
	if err := json.Unmarshal(evaluation.Body.Bytes(), &evaluated); err != nil || evaluated.State != repair.PermitGroundable {
		t.Fatalf("permit not groundable after retry: %s", evaluation.Body.String())
	}
}

func TestRecordReceiptRetryIsIdempotent(t *testing.T) {
	server := testServer(t)
	permit := startIsolation(t, server)
	request := ReceiptRequest{CircuitID: permit.CircuitID, OperationID: permit.OperationID, Isolated: true}

	first := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", request)
	if first.Code != http.StatusCreated {
		t.Fatalf("first receipt status=%d body=%s", first.Code, first.Body.String())
	}
	var firstReceipt landing.IsolationReceipt
	if err := json.Unmarshal(first.Body.Bytes(), &firstReceipt); err != nil {
		t.Fatal(err)
	}

	second := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", request)
	if second.Code != http.StatusOK {
		t.Fatalf("retried receipt status=%d body=%s, want 200", second.Code, second.Body.String())
	}
	var secondReceipt landing.IsolationReceipt
	if err := json.Unmarshal(second.Body.Bytes(), &secondReceipt); err != nil {
		t.Fatal(err)
	}
	if !secondReceipt.SameAs(firstReceipt) || !secondReceipt.ReceivedAt.Equal(firstReceipt.ReceivedAt) {
		t.Fatalf("retry returned a different receipt: first=%+v second=%+v", firstReceipt, secondReceipt)
	}

	records, err := server.services.Journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range records {
		if record.Kind == "feed-receipt" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("journal contains %d feed-receipt records, want exactly 1", count)
	}

	evaluation := doJSON(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluation.Code != http.StatusOK {
		t.Fatalf("evaluation status=%d body=%s", evaluation.Code, evaluation.Body.String())
	}
}

func TestRecordReceiptConflictingReplayRejectedButCommittedReceiptStaysEvaluable(t *testing.T) {
	server := testServer(t)
	permit := startIsolation(t, server)

	committed := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", ReceiptRequest{
		CircuitID: permit.CircuitID, OperationID: permit.OperationID, Isolated: true,
	})
	if committed.Code != http.StatusCreated {
		t.Fatalf("receipt status=%d body=%s", committed.Code, committed.Body.String())
	}
	conflict := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts", ReceiptRequest{
		CircuitID: permit.CircuitID, OperationID: permit.OperationID, Isolated: false,
	})
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting replay status=%d body=%s, want 409", conflict.Code, conflict.Body.String())
	}

	// The conflicting replay must not overwrite the committed isolated receipt.
	if _, matched := server.services.Receipts.Match("east", permit.CircuitID, permit.OperationID); !matched {
		t.Fatal("committed isolated receipt lost after conflicting replay")
	}
	evaluation := doJSON(t, server, http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil)
	if evaluation.Code != http.StatusOK {
		t.Fatalf("evaluation status=%d body=%s, want permit still evaluable", evaluation.Code, evaluation.Body.String())
	}
	records, err := server.services.Journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range records {
		if record.Kind == "feed-receipt" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("conflicting replay wrote %d feed-receipt records, want 1", count)
	}
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}
