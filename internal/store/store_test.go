package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotAndJournalRoundTrip(t *testing.T) {
	root := t.TempDir()
	snapshots := NewSnapshotStore(filepath.Join(root, "snapshots"))
	want := RampCheckpoint{RepeaterID: "R07", StableStep: 2, UpdatedAt: time.Now().UTC().Truncate(time.Second)}
	if err := snapshots.Save("ramp-R07", want); err != nil {
		t.Fatal(err)
	}
	var got RampCheckpoint
	found, err := snapshots.Load("ramp-R07", &got)
	if err != nil || !found {
		t.Fatalf("load snapshot: found=%v err=%v", found, err)
	}
	if got.RepeaterID != want.RepeaterID || got.StableStep != want.StableStep {
		t.Fatalf("snapshot mismatch: got=%+v want=%+v", got, want)
	}

	journal := NewJournal(filepath.Join(root, "events.jsonl"))
	if err := journal.Append("ramp", "R07", got, want.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	records, err := journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Kind != "ramp" || records[0].EntityID != "R07" {
		t.Fatalf("unexpected journal records: %+v", records)
	}
}

func TestCommitOnceIsIdempotentWithinAndAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	first := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)
	later := first.Add(time.Minute)
	payload := map[string]string{"status": "isolated"}

	journal := NewJournal(path)
	record, appended, err := journal.CommitOnce("feed-receipt", "op-1", payload, first)
	if err != nil || !appended {
		t.Fatalf("first commit: appended=%v err=%v", appended, err)
	}
	if !record.Timestamp.Equal(first) {
		t.Fatalf("timestamp = %s, want %s", record.Timestamp, first)
	}

	// Retry with a fresh timestamp must not append, and must return the
	// canonical record from the first commit.
	retry, appended, err := journal.CommitOnce("feed-receipt", "op-1", payload, later)
	if err != nil {
		t.Fatal(err)
	}
	if appended {
		t.Fatal("retry appended a duplicate record")
	}
	if !retry.Timestamp.Equal(first) {
		t.Fatalf("retry returned timestamp %s, want canonical %s", retry.Timestamp, first)
	}

	// A different entity ID is still a distinct commit.
	if _, appended, err := journal.CommitOnce("feed-receipt", "op-2", payload, later); err != nil || !appended {
		t.Fatalf("second entity: appended=%v err=%v", appended, err)
	}

	records, err := journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("journal has %d records, want 2: %+v", len(records), records)
	}

	// A new journal instance rebuilds its index from the file: a retry after
	// restart must remain idempotent.
	reopened := NewJournal(path)
	replayed, appended, err := reopened.CommitOnce("feed-receipt", "op-1", payload, later)
	if err != nil || appended {
		t.Fatalf("replay after reopen: appended=%v err=%v", appended, err)
	}
	if !replayed.Timestamp.Equal(first) {
		t.Fatalf("replayed timestamp %s, want canonical %s", replayed.Timestamp, first)
	}
	records, err = reopened.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("journal has %d records after reopen replay, want 2", len(records))
	}
}

func TestCommitOnceFailsBeforeAppendingWhenUnwritable(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal := NewJournal(filepath.Join(blocker, "events.jsonl"))
	if _, _, err := journal.CommitOnce("feed-receipt", "op-1", map[string]string{"status": "x"}, time.Now()); err == nil {
		t.Fatal("CommitOnce succeeded with an unwritable journal path")
	}
	// The failed commit must not have populated the in-memory commit index.
	if len(journal.committed) != 0 {
		t.Fatalf("failed commit left index entries behind: %+v", journal.committed)
	}
	// Once the path is writable, retrying the same key is still the first
	// commit rather than a rejected replay.
	journal.path = filepath.Join(t.TempDir(), "events.jsonl")
	if _, appended, err := journal.CommitOnce("feed-receipt", "op-1", map[string]string{"status": "x"}, time.Now()); err != nil || !appended {
		t.Fatalf("retry after failed commit: appended=%v err=%v, want first-time append", appended, err)
	}
}
