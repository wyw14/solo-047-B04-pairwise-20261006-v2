package landing

import (
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

// SameAs reports whether two receipts describe the same isolation outcome for
// the same operation. ReceivedAt is intentionally ignored so retries that
// regenerate their timestamp are still recognized as the same receipt.
func (r IsolationReceipt) SameAs(other IsolationReceipt) bool {
	return r.StationID == other.StationID &&
		r.CircuitID == other.CircuitID &&
		r.OperationID == other.OperationID &&
		r.Isolated == other.Isolated
}

type ReceiptRegistry struct {
	mu       sync.RWMutex
	receipts map[string]IsolationReceipt
}

func NewReceiptRegistry() *ReceiptRegistry {
	return &ReceiptRegistry{receipts: make(map[string]IsolationReceipt)}
}

func receiptKey(stationID, circuitID, operationID string) string {
	return stationID + "\x00" + circuitID + "\x00" + operationID
}

// Publish makes a receipt visible in the registry. It must only be called once
// the receipt has been durably committed; a failed persistence must never reach
// the registry, so no rollback path exists here. Publishing the same key more
// than once is idempotent and keeps the first committed receipt untouched.
func (r *ReceiptRegistry) Publish(receipt IsolationReceipt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := receiptKey(receipt.StationID, receipt.CircuitID, receipt.OperationID)
	if _, exists := r.receipts[key]; exists {
		return
	}
	r.receipts[key] = receipt
}

func (r *ReceiptRegistry) Match(stationID, circuitID, operationID string) (IsolationReceipt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	receipt, exists := r.receipts[receiptKey(stationID, circuitID, operationID)]
	return receipt, exists && receipt.Isolated
}

func (r *ReceiptRegistry) ForStation(stationID string) []IsolationReceipt {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []IsolationReceipt
	for _, receipt := range r.receipts {
		if receipt.StationID == stationID {
			result = append(result, receipt)
		}
	}
	return result
}
