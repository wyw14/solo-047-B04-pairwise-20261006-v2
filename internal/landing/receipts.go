package landing

import (
	"errors"
	"sync"
	"time"
)

type IsolationReceipt struct {
	StationID   string    `json:"station_id"`
	CircuitID   string    `json:"circuit_id"`
	OperationID string    `json:"operation_id"`
	Isolated    bool      `json:"isolated"`
	ReceivedAt  time.Time `json:"received_at"`
}

// ReceiptStatus marks how far a receipt submission has progressed.
type ReceiptStatus string

const (
	// receiptPending is visible only to the submitting request: it has been
	// reserved in memory but persistence has not succeeded yet, so it must not
	// influence permit evaluation.
	receiptPending ReceiptStatus = "pending"
	// ReceiptPersisted is the committed state reached after the journal append
	// succeeds. Only committed receipts are visible to Match/Lookup/ForStation.
	ReceiptPersisted ReceiptStatus = "persisted"
)

// ErrReceiptAlreadyPending is returned when an in-flight submission for the
// same receipt identity is already being persisted. The caller should answer
// 409 Conflict and let the client retry later.
var ErrReceiptAlreadyPending = errors.New("receipt submission already in progress")

type entry struct {
	receipt IsolationReceipt
	status  ReceiptStatus
}

type ReceiptRegistry struct {
	mu      sync.RWMutex
	entries map[string]entry
}

func NewReceiptRegistry() *ReceiptRegistry {
	return &ReceiptRegistry{entries: make(map[string]entry)}
}

func receiptKey(stationID, circuitID, operationID string) string {
	return stationID + "\x00" + circuitID + "\x00" + operationID
}

// Reserve holds a receipt as a pending reservation. The reservation is
// invisible to Match and friends until Publish is called.
//
// Idempotency:
//   - If the same receipt was already committed, Reserve returns the original
//     receipt and committed=true without installing a new reservation, so a
//     retried request can be short-circuited without touching the journal.
//   - If an identical reservation is already pending, the existing pending
//     receipt is returned together with ErrReceiptAlreadyPending so the caller
//     does not overwrite an in-flight submission.
func (r *ReceiptRegistry) Reserve(receipt IsolationReceipt) (existing IsolationReceipt, committed bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := receiptKey(receipt.StationID, receipt.CircuitID, receipt.OperationID)
	if current, ok := r.entries[key]; ok {
		if current.status == receiptPending {
			return current.receipt, false, ErrReceiptAlreadyPending
		}
		return current.receipt, true, nil
	}
	r.entries[key] = entry{receipt: receipt, status: receiptPending}
	return IsolationReceipt{}, false, nil
}

// Publish moves a pending reservation to the committed state. It is safe to
// call for a receipt that is already committed (idempotent replay): the
// first-committed receipt wins and its payload, including ReceivedAt, is
// preserved. It is a no-op when the reservation has been rolled back or when
// no reservation exists.
func (r *ReceiptRegistry) Publish(stationID, circuitID, operationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := receiptKey(stationID, circuitID, operationID)
	current, ok := r.entries[key]
	if !ok || current.status != receiptPending {
		return
	}
	current.status = ReceiptPersisted
	r.entries[key] = current
}

// Rollback removes a pending reservation created by Reserve when persistence
// fails, leaving the registry exactly as if Reserve had never been called.
// Committed receipts, and reservations whose pending payload differs from the
// one being abandoned (e.g. a concurrent resubmission that won), are left
// untouched.
func (r *ReceiptRegistry) Rollback(receipt IsolationReceipt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := receiptKey(receipt.StationID, receipt.CircuitID, receipt.OperationID)
	current, ok := r.entries[key]
	if !ok || current.status != receiptPending {
		return
	}
	if !receiptEqual(current.receipt, receipt) {
		return
	}
	delete(r.entries, key)
}

// Match answers whether a committed isolated receipt exists for the given
// scope. Pending (unpersisted) reservations never match.
func (r *ReceiptRegistry) Match(stationID, circuitID, operationID string) (IsolationReceipt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, exists := r.entries[receiptKey(stationID, circuitID, operationID)]
	if !exists || current.status != ReceiptPersisted {
		return IsolationReceipt{}, false
	}
	return current.receipt, current.receipt.Isolated
}

// Lookup returns the committed receipt for a scope if one exists.
func (r *ReceiptRegistry) Lookup(stationID, circuitID, operationID string) (IsolationReceipt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, exists := r.entries[receiptKey(stationID, circuitID, operationID)]
	if !exists || current.status != ReceiptPersisted {
		return IsolationReceipt{}, false
	}
	return current.receipt, true
}

// ForStation returns committed receipts recorded for a station. Pending
// reservations are excluded.
func (r *ReceiptRegistry) ForStation(stationID string) []IsolationReceipt {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []IsolationReceipt
	for _, current := range r.entries {
		if current.status == ReceiptPersisted && current.receipt.StationID == stationID {
			result = append(result, current.receipt)
		}
	}
	return result
}

func receiptEqual(a, b IsolationReceipt) bool {
	return a.StationID == b.StationID &&
		a.CircuitID == b.CircuitID &&
		a.OperationID == b.OperationID &&
		a.Isolated == b.Isolated &&
		a.ReceivedAt.Equal(b.ReceivedAt)
}
