package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// ErrReceiptConflict indicates a retried receipt carries a different isolation
// outcome than the receipt already committed for the same operation.
var ErrReceiptConflict = errors.New("feed receipt conflicts with the committed receipt for this operation")

type Services struct {
	Clock               platform.Clock
	IDs                 platform.IDGenerator
	Journal             *store.Journal
	Spans               map[string]*span.Span
	Routes              map[string]*route.Route
	Repeaters           map[string]*repeater.Repeater
	Topology            *topology.Registry
	OTDR                *otdr.SessionService
	Switchovers         *switchover.SessionService
	Permits             *repair.PermitService
	Receipts            *landing.ReceiptRegistry
	Acceptance          *repair.AcceptanceService
	Telemetry           *telemetry.Ingestor
	TelemetryRepository *telemetry.Repository
	Admission           *incident.AdmissionService
	Classifier          *incident.Classifier
	Correlator          *otdr.Correlator
	Health              *landing.HealthService
	Station             *landing.Station
	WetPlantTests       *repair.TestService
	Ramps               *repeater.RampController
	Assets              *repair.Assets
	RepeaterHealth      *repeater.HealthProjector
	Actuator            *feed.Actuator
	Tests               map[string]*repair.TestSession
	Incidents           map[string]*incident.Incident
	mu                  sync.RWMutex
}

func (s *Services) Span(id string) (*span.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Spans[id]
	if target == nil {
		return nil, fmt.Errorf("span %s not found", id)
	}
	return target, nil
}

func (s *Services) Route(id string) (*route.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Routes[id]
	if target == nil {
		return nil, fmt.Errorf("route %s not found", id)
	}
	return target, nil
}

func (s *Services) Repeater(id string) (*repeater.Repeater, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Repeaters[id]
	if target == nil {
		return nil, fmt.Errorf("repeater %s not found", id)
	}
	return target, nil
}

func (s *Services) Persist(kind, entityID string, value any) error {
	return s.Journal.Append(kind, entityID, value, s.Clock.Now())
}

// SubmitReceipt commits a feed isolation receipt durably before making it
// visible to permit evaluation. It is safe to retry:
//
//   - On the first attempt the receipt is appended to the journal, then
//     published to the in-memory registry, and committed=true is returned.
//   - If the journal write fails the registry is never touched, so the error
//     response cannot change the evaluable state of any permit; the caller may
//     retry with the same body.
//   - On a replay (a committed receipt already exists for the operation) no
//     duplicate journal line is written and the canonical committed receipt is
//     returned with committed=false. A replay whose isolation outcome differs
//     from the committed receipt fails with ErrReceiptConflict; the registry is
//     reconciled against the committed ledger before the error is returned.
func (s *Services) SubmitReceipt(receipt landing.IsolationReceipt) (landing.IsolationReceipt, bool, error) {
	record, appended, err := s.Journal.CommitOnce("feed-receipt", receipt.OperationID, receipt, s.Clock.Now().UTC())
	if err != nil {
		return landing.IsolationReceipt{}, false, err
	}
	var committed landing.IsolationReceipt
	if err := json.Unmarshal(record.Payload, &committed); err != nil {
		return landing.IsolationReceipt{}, false, fmt.Errorf("decode committed feed receipt: %w", err)
	}
	if !appended && !committed.SameAs(receipt) {
		// Keep the in-memory view a faithful mirror of what was durably
		// committed; the conflicting retry never becomes evaluable.
		s.Receipts.Publish(committed)
		return committed, false, ErrReceiptConflict
	}
	s.Receipts.Publish(committed)
	return committed, appended, nil
}

func (s *Services) StartWetPlantTest(ctx context.Context, spanID string) repair.TestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.WetPlantTests.Start(ctx, spanID)
	snapshot := s.WetPlantTests.Snapshot(session)
	s.Tests[snapshot.ID] = session
	return snapshot
}

func (s *Services) FinishWetPlantTest(id, action string) (repair.TestSnapshot, error) {
	s.mu.Lock()
	session := s.Tests[id]
	delete(s.Tests, id)
	s.mu.Unlock()
	if session == nil {
		return repair.TestSnapshot{}, fmt.Errorf("wet-plant test %s not found", id)
	}
	if action == "complete" {
		return s.WetPlantTests.Complete(session), nil
	}
	return s.WetPlantTests.Cancel(session), nil
}

func (s *Services) Incident(id, spanID string) *incident.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.Incidents[id]
	if current == nil {
		current = incident.New(id, spanID, s.Clock.Now().Add(10*time.Second), s.Clock.Now())
		s.Incidents[id] = current
	}
	return current
}

func (s *Services) TelemetryHistory(assetID string) []telemetry.Sample {
	items := s.TelemetryRepository.History(assetID)
	if latest, ok := s.TelemetryRepository.Latest(assetID, telemetry.KindTemperature); ok {
		_ = latest
	}
	return items
}
