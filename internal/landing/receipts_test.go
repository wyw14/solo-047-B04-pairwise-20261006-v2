package landing

import (
	"errors"
	"testing"
	"time"
)

func sampleReceipt(at time.Time) IsolationReceipt {
	return IsolationReceipt{
		StationID:   "east",
		CircuitID:   "B",
		OperationID: "op-1",
		Isolated:    true,
		ReceivedAt:  at.UTC(),
	}
}

// A reservation must not influence permit evaluation until it is published
// after a successful persistence.
func TestReserveIsInvisibleUntilPublished(t *testing.T) {
	registry := NewReceiptRegistry()
	receipt := sampleReceipt(time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC))

	if _, committed, err := registry.Reserve(receipt); err != nil || committed {
		t.Fatalf("Reserve: committed=%v err=%v", committed, err)
	}
	if _, matched := registry.Match("east", "B", "op-1"); matched {
		t.Fatal("pending reservation must not match permit evaluation")
	}
	if _, exists := registry.Lookup("east", "B", "op-1"); exists {
		t.Fatal("pending reservation must not be visible to Lookup")
	}
	if items := registry.ForStation("east"); len(items) != 0 {
		t.Fatalf("pending reservation must not be listed, got %+v", items)
	}

	registry.Publish("east", "B", "op-1")
	got, matched := registry.Match("east", "B", "op-1")
	if !matched || !got.Isolated {
		t.Fatalf("published receipt should match, got %+v matched=%v", got, matched)
	}
	if items := registry.ForStation("east"); len(items) != 1 {
		t.Fatalf("published receipt should be listed once, got %+v", items)
	}
}

// A failed persistence is compensated by Rollback: the registry returns to its
// original state and a fresh retry can reserve the same receipt.
func TestRollbackAfterPersistenceFailure(t *testing.T) {
	registry := NewReceiptRegistry()
	receipt := sampleReceipt(time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC))

	if _, _, err := registry.Reserve(receipt); err != nil {
		t.Fatal(err)
	}
	registry.Rollback(receipt)

	if _, matched := registry.Match("east", "B", "op-1"); matched {
		t.Fatal("rolled-back reservation must not match")
	}
	if _, _, err := registry.Reserve(receipt); err != nil {
		t.Fatalf("retry after rollback should reserve cleanly, got %v", err)
	}
}

// Two concurrent submissions for the same receipt must not overwrite each
// other; the second one is told an in-flight submission exists.
func TestReserveRejectsConcurrentPendingSubmission(t *testing.T) {
	registry := NewReceiptRegistry()
	receipt := sampleReceipt(time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC))
	if _, _, err := registry.Reserve(receipt); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Reserve(receipt); !errors.Is(err, ErrReceiptAlreadyPending) {
		t.Fatalf("expected ErrReceiptAlreadyPending, got %v", err)
	}

	// The loser rolling back must not remove the winner's in-flight reservation
	// when the payloads differ.
	different := receipt
	different.ReceivedAt = receipt.ReceivedAt.Add(time.Second)
	registry.Rollback(different)
	if _, _, err := registry.Reserve(receipt); !errors.Is(err, ErrReceiptAlreadyPending) {
		t.Fatalf("winner reservation should survive foreign rollback, got %v", err)
	}
}

// Replaying an already-committed receipt must be idempotent: the first
// committed payload wins (including ReceivedAt) and no new entry is created.
func TestReserveAfterCommitIsIdempotent(t *testing.T) {
	registry := NewReceiptRegistry()
	first := sampleReceipt(time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC))
	if _, _, err := registry.Reserve(first); err != nil {
		t.Fatal(err)
	}
	registry.Publish("east", "B", "op-1")

	replayed := first
	replayed.ReceivedAt = first.ReceivedAt.Add(5 * time.Second)
	replayed.Isolated = false
	existing, committed, err := registry.Reserve(replayed)
	if err != nil || !committed {
		t.Fatalf("replay should report committed without error, got committed=%v err=%v", committed, err)
	}
	if !existing.ReceivedAt.Equal(first.ReceivedAt) || !existing.Isolated {
		t.Fatalf("first-committed receipt must win, got %+v", existing)
	}
	// Publish on the replay path must not clobber the committed entry either.
	registry.Publish("east", "B", "op-1")
	got, matched := registry.Match("east", "B", "op-1")
	if !matched || !got.ReceivedAt.Equal(first.ReceivedAt) {
		t.Fatalf("committed receipt changed after replay: %+v", got)
	}
	if items := registry.ForStation("east"); len(items) != 1 {
		t.Fatalf("expected exactly one committed receipt, got %d", len(items))
	}
}

// Rolling back a committed receipt must never remove it.
func TestRollbackDoesNotTouchCommittedReceipt(t *testing.T) {
	registry := NewReceiptRegistry()
	receipt := sampleReceipt(time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC))
	if _, _, err := registry.Reserve(receipt); err != nil {
		t.Fatal(err)
	}
	registry.Publish("east", "B", "op-1")
	registry.Rollback(receipt)
	if _, exists := registry.Lookup("east", "B", "op-1"); !exists {
		t.Fatal("committed receipt must survive rollback")
	}
}
