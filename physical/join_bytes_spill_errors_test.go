package physical

import (
	"context"
	"errors"
	"gocolumn/internal/spillmeta"
	"gocolumn/join"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type byteSpillDeleteOnce struct {
	memory.SpillManager
	err    error
	failed bool
}

func (s *byteSpillDeleteOnce) SpillWithMetadata(ctx context.Context, o memory.SpillObject, m memory.Manager) (memory.SpillHandle, error) {
	return s.SpillManager.(memory.MetadataSpillManager).SpillWithMetadata(ctx, o, m)
}
func (s *byteSpillDeleteOnce) Delete(ctx context.Context, h memory.SpillHandle) error {
	if !s.failed {
		s.failed = true
		return s.err
	}
	return s.SpillManager.Delete(ctx, h)
}
func TestGenericSpillDeleteFailureStickyCleanup(t *testing.T) {
	ctx, o, s, q, dir := openSmallByteSpill(t)
	base, err := q.SpillManager()
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("injected spill deletion failure")
	s.store.Close()
	s.store = spillmeta.NewStore(&byteSpillDeleteOnce{SpillManager: base, err: sentinel}, s.metadata)
	_, err = s.Next(ctx, memory.NewBatch())
	if !errors.Is(err, sentinel) {
		t.Fatalf("lost injected delete error: %v", err)
	}
	_, retry := s.Next(ctx, memory.NewBatch())
	if err != retry {
		t.Fatalf("different terminal error: %v / %v", err, retry)
	}
	s.Close()
	assertByteSpillClean(t, o, q, dir)
}
func TestGenericSpillCorruptRunCleanup(t *testing.T) {
	ctx, o, s, q, dir := openSmallByteSpill(t)
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	modified := false
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !modified {
			modified = true
			return os.WriteFile(p, []byte("corrupt"), 0600)
		}
		return nil
	})
	if err != nil || !modified {
		t.Fatal(err, modified)
	}
	_, err = s.Next(ctx, memory.NewBatch())
	if !errors.Is(err, memory.ErrSpillCorrupt) {
		t.Fatalf("corruption not detected: %v", err)
	}
	s.Close()
	assertByteSpillClean(t, o, q, dir)
}
func TestGenericSpillOutputQuotaCleanup(t *testing.T) {
	left := make([]genericJoinRow, 5)
	right := make([]genericJoinRow, 150)
	for i := range left {
		left[i] = genericJoinRow{s: "same"}
	}
	for i := range right {
		right[i] = genericJoinRow{s: "same", x: strings.Repeat("x", 100)}
	}
	ctx, o, s, q, dir := openByteSpill(t, genericJoinSource(t, left, false, 5), genericJoinSource(t, right, false, 30), join.Full, []JoinKey{{"s", "s"}}, 256<<10, 64<<10)
	if err := s.prepare(ctx); err != nil {
		t.Fatalf("quota must survive input partition: %v", err)
	}
	_, err := drainByteSpill(ctx, s)
	if !errors.Is(err, memory.ErrSpillQuotaExceeded) {
		t.Fatalf("expected output quota failure: %v", err)
	}
	if s.outputCount == 0 {
		t.Fatal("did not create output before quota failure")
	}
	s.Close()
	assertByteSpillClean(t, o, q, dir)
}
func TestGenericSpillChildFailureJoinsClose(t *testing.T) {
	ctx, o, s, q, dir := openSmallByteSpill(t)
	sentinel := errors.New("injected child close")
	bad := &filterProbeSource{schema: o.left.Schema(), next: func(context.Context, *memory.Batch) (bool, error) { return false, source.ErrBatchStreamClosed }, close: func() error { return sentinel }}
	o.left = bad
	_, err := s.Next(ctx, memory.NewBatch())
	if !errors.Is(err, sentinel) || !errors.Is(err, source.ErrBatchStreamClosed) || bad.closes != 1 {
		t.Fatalf("lost child failure/close: %v closes=%d", err, bad.closes)
	}
	_, retry := s.Next(ctx, memory.NewBatch())
	if retry != err {
		t.Fatal("not sticky")
	}
	s.Close()
	assertByteSpillClean(t, o, q, dir)
}

func openSmallByteSpill(t *testing.T) (context.Context, *JoinSource, *spillableJoinStream, *memory.QueryBudget, string) {
	t.Helper()
	rows := []genericJoinRow{{s: "a"}, {s: "b"}}
	return openByteSpill(t, genericJoinSource(t, rows, false, 1), genericJoinSource(t, rows, false, 1), join.Full, []JoinKey{{"s", "s"}}, 1<<20, 64<<20)
}
