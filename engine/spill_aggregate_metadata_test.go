package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"os"
	"path/filepath"
	"testing"
)

func aggregateMetadataStream(t *testing.T, limit int64, rows, groups int) (*spillableAggregateStream, *memory.QueryBudget, string) {
	t.Helper()
	opts := streamTestOptions()
	opts.MemoryLimitBytes = limit
	opts.ExecutionPolicy = ExecutionBounded
	dir := t.TempDir()
	opts.Spill = SpillOptions{Directory: dir, MaxBytes: 64 << 20}
	e := New(opts)
	q := streamTestQuery()
	q.Hints.ForceStrategy = ""
	p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: rows, groups: groups}, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(); b.Close(); e.Close() })
	stream, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), b), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*spillableAggregateStream)
	t.Cleanup(func() { s.Close() })
	return s, b, dir
}

func assertAggregateMetadataClean(t *testing.T, b *memory.QueryBudget, dir string) {
	t.Helper()
	if b.Used() != 0 {
		t.Fatalf("retained bytes after cleanup: %d", b.Used())
	}
	if b.Snapshot().PeakBytes > b.Limit() {
		t.Fatalf("peak exceeds hard limit: %+v", b.Snapshot())
	}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return fmt.Errorf("spill file remains: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAggregateMetadataManyRunsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int64
		fail  bool
	}{{"tight", 128 << 10, true}, {"generous", 16 << 20, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, b, dir := aggregateMetadataStream(t, tc.limit, 4096, 4096)
			out := memory.NewBatch()
			groups := 0
			var finalErr error
			for {
				ok, err := s.Next(context.Background(), out)
				if err != nil {
					finalErr = err
					break
				}
				if !ok {
					break
				}
				groups += out.Rows
			}
			if tc.fail {
				if !errors.Is(finalErr, memory.ErrBudgetExceeded) {
					t.Fatalf("want budget rejection, got %v", finalErr)
				}
			} else if finalErr != nil || groups != 4096 {
				t.Fatalf("groups=%d error=%v", groups, finalErr)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			assertAggregateMetadataClean(t, b, dir)
		})
	}
}

func TestAggregateMetadataRecursiveCancellationCleanup(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 64, 64)
	ctx := memory.WithQueryBudget(context.Background(), b)
	if err := s.consumeInput(ctx); err != nil {
		t.Fatal(err)
	}
	// Repartitioning requires persisted input; EOF now leaves fitting state resident.
	if _, err := s.state.RequestHardSpill(ctx, s.SpillableBytes()); err != nil {
		t.Fatal(err)
	}
	// Children are intentionally not attached to the stream's top-level table:
	// the private registry must still find them when a deeper operation fails.
	var index int
	for i := range s.partitions {
		if len(s.partitions[i].Values) > 0 {
			index = i
			break
		}
	}
	children, err := s.repartitionRuns(ctx, &s.partitions[index], 1)
	if err != nil {
		t.Fatal(err)
	}
	var nested *aggregateRunPartitions
	for i := range children.runs {
		if len(children.runs[i].Values) > 0 {
			nested, err = s.repartitionRuns(ctx, &children.runs[i], 2)
			break
		}
	}
	if err != nil {
		children.release()
		t.Fatal(err)
	}
	if nested == nil {
		children.release()
		t.Fatal("no nested children")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for i := range nested.runs {
		if len(nested.runs[i].Values) > 0 {
			_, err = s.repartitionRuns(cancelled, &nested.runs[i], 3)
			break
		}
	}
	nested.release()
	children.release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation, got %v", err)
	}
	s.abort(err)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestAggregateMetadataRetainedUntilEOF(t *testing.T) {
	s, b, dir := aggregateMetadataStream(t, 1<<20, 64, 16)
	if b.Used() == 0 {
		t.Fatal("constructor metadata not admitted")
	}
	for {
		ok, err := s.Next(context.Background(), memory.NewBatch())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	if s.outputRefs.Values != nil || s.metadataLease != nil {
		t.Fatal("metadata owner retained at EOF")
	}
	assertAggregateMetadataClean(t, b, dir)
}

func TestAggregateMetadataConstructorAdmission(t *testing.T) {
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 1024
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
	e := New(opts)
	defer e.Close()
	q := streamTestQuery()
	q.Hints.ForceStrategy = ""
	p, err := e.Prepare(context.Background(), &generatedStreamSource{rows: 1, groups: 1}, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	b, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	s, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), b), p.resultSchema(), 1)
	if s != nil {
		s.Close()
		t.Fatal("tiny budget admitted fixed metadata")
	}
	if !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("want metadata budget rejection, got %v", err)
	}
	if b.Used() != 0 {
		t.Fatalf("constructor leaked %d bytes", b.Used())
	}
}
