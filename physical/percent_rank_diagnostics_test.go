package physical

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"sync"
	"testing"
	"unsafe"
)

type percentRankDiagnosticTestSource struct {
	manager   memory.Manager
	stream    *percentRankDiagnosticTestStream
	values    map[string]string
	openError error
	nextError error
	opens     int
}

func (s *percentRankDiagnosticTestSource) Schema() memory.Schema {
	return memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt64})
}
func (s *percentRankDiagnosticTestSource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{}, nil
}
func (s *percentRankDiagnosticTestSource) Partitions(context.Context) ([]source.Partition, error) {
	return nil, nil
}
func (s *percentRankDiagnosticTestSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: "k"}}
}
func (s *percentRankDiagnosticTestSource) NewBatchStream(ctx context.Context, _ source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.openError != nil {
		return nil, s.openError
	}
	lease, err := s.manager.Reserve(ctx, 64)
	if err != nil {
		return nil, err
	}
	s.stream = &percentRankDiagnosticTestStream{values: s.values, nextError: s.nextError, lease: lease}
	return s.stream, nil
}

type percentRankDiagnosticTestStream struct {
	values    map[string]string
	nextError error
	lease     memory.Reservation
	closed    bool
	closes    int
}

func (s *percentRankDiagnosticTestStream) Next(context.Context, *memory.Batch) (bool, error) {
	return false, s.nextError
}
func (s *percentRankDiagnosticTestStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.closes++
	clear(s.values) // A post-close-only observer would lose the real counters.
	s.lease.Release()
	s.lease = nil
	return nil
}
func (s *percentRankDiagnosticTestStream) StreamDiagnostics() map[string]string {
	out := make(map[string]string, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out
}
func TestPercentRankDiagnosticsSnapshotAndClose(t *testing.T) {
	ctx := context.Background()
	m := memory.NewBudgetManager(1 << 20)
	d, err := newPercentRankDiagnostics(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := m.Used(), int64(unsafe.Sizeof(percentRankDiagnostics{})); got != want {
		t.Fatal(got, want)
	}
	specs := []struct {
		stage  string
		values map[string]string
	}{
		{"front_sort", map[string]string{"spill_runs": "2", "spill_bytes": "101"}},
		{"partition_spool", map[string]string{"partition_spool_pages": "3", "partition_spool_bytes": "202", "partition_spool_partitions": "4"}},
		{"restore_sort", map[string]string{"spill_runs": "5", "spill_bytes": "303"}},
	}
	var inputs []*percentRankDiagnosticTestSource
	opts := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)}
	for _, spec := range specs {
		input := &percentRankDiagnosticTestSource{manager: m, values: spec.values}
		inputs = append(inputs, input)
		wrapped := d.Wrap(spec.stage, input)
		if !source.OrderedByPrefix(wrapped, []string{"k"}) || source.ReplayabilityOf(wrapped) != source.ReplayUnknown {
			t.Fatal("source contracts changed")
		}
		stream, e := source.OpenBatchStream(ctx, wrapped, opts)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = source.OpenBatchStream(ctx, wrapped, opts); e == nil {
			t.Fatal("opened twice")
		}
		if ok, e := stream.Next(ctx, memory.NewBatch()); ok || e != nil {
			t.Fatal(ok, e)
		}
	}
	// Snapshot is a copy and can run concurrently with terminal counter capture.
	got := d.Snapshot()
	got["front_sort_spill_runs"] = "999"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = d.Snapshot()
		}
	}()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	got = d.Snapshot()
	for k, want := range map[string]string{"front_sort_spill_runs": "2", "front_sort_spill_bytes": "101", "partition_spool_pages": "3", "partition_spool_bytes": "202", "partition_spool_partitions": "4", "restore_sort_spill_runs": "5", "restore_sort_spill_bytes": "303", "front_sort_observed": "true", "partition_spool_observed": "true", "restore_sort_observed": "true"} {
		if got[k] != want {
			t.Fatal(k, got[k], want)
		}
	}
	for i, input := range inputs {
		if input.opens != 1 || input.stream.closes != 1 || d.sources[i].input != nil || d.streams[i].child != nil || d.streams[i].owner != nil {
			t.Fatal("retained child or duplicate open/close", i)
		}
	}
	if m.Used() != 0 {
		t.Fatal("diagnostic leak", m.Used())
	}
}
func TestPercentRankDiagnosticsFailureCleanup(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("observer child failure")
	for _, failOpen := range []bool{true, false} {
		m := memory.NewBudgetManager(1 << 20)
		d, e := newPercentRankDiagnostics(ctx, m)
		if e != nil {
			t.Fatal(e)
		}
		input := &percentRankDiagnosticTestSource{manager: m, values: map[string]string{"spill_runs": "7", "spill_bytes": "19"}}
		if failOpen {
			input.openError = sentinel
		} else {
			input.nextError = sentinel
		}
		st, e := source.OpenBatchStream(ctx, d.Wrap("front_sort", input), source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar)})
		if failOpen {
			if !errors.Is(e, sentinel) {
				t.Fatal(e)
			}
		} else {
			if e != nil {
				t.Fatal(e)
			}
			if _, e = st.Next(ctx, memory.NewBatch()); !errors.Is(e, sentinel) {
				t.Fatal(e)
			}
		}
		if e := d.Close(); e != nil {
			t.Fatal(e)
		}
		if m.Used() != 0 {
			t.Fatal("failed execution leak", m.Used())
		}
		if !failOpen && d.Snapshot()["front_sort_spill_runs"] != "7" {
			t.Fatal("error snapshot missing")
		}
	}
	m := memory.NewBudgetManager(int64(unsafe.Sizeof(percentRankDiagnostics{})) - 1)
	if _, err := newPercentRankDiagnostics(ctx, m); !errors.Is(err, memory.ErrBudgetExceeded) || m.Used() != 0 {
		t.Fatal(err, m.Used())
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := newPercentRankDiagnostics(canceled, m); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := newPercentRankDiagnostics(nil, m); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := newPercentRankDiagnostics(ctx, nil); err == nil {
		t.Fatal("nil manager accepted")
	}
}
