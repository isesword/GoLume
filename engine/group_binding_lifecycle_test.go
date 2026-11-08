package engine

import (
	"context"
	"errors"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"testing"
	"unsafe"
)

func assertGroupBindingCleared(t *testing.T, w *aggregatePredicateWorkspace) {
	t.Helper()
	if w.keys.binding.batch != nil || w.keys.binding.fields != nil || w.keys.binding.components != nil {
		t.Fatal("retained batch binding")
	}
	for i, c := range w.keys.components {
		if !reflect.DeepEqual(c, canonicalGroupComponent{}) {
			t.Fatalf("key %d retained borrowed data", i)
		}
	}
}

func TestGroupBindingStreamLifetime(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted"} {
		for _, terminal := range []string{"eof", "input-error", "invalid", "cancel", "early-close"} {
			t.Run(strategy+"/"+terminal, func(t *testing.T) {
				schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeUint64}, memory.Field{Name: "s", Type: memory.TypeString})
				src, err := inmemory.New(schema)
				if err != nil {
					t.Fatal(err)
				}
				if strategy == "sorted" {
					src.WithOrdering(source.OrderingColumn{Name: "k"}, source.OrderingColumn{Name: "s"})
				}
				opts := streamTestOptions()
				opts.Workers = 3
				opts.MemoryLimitBytes = 4 << 20
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"k", "s"}, Aggregates: []expr.Aggregate{expr.Count("n")}, Hints: expr.Hints{ForceStrategy: strategy}}, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx, cancel := context.WithCancel(memory.WithQueryBudget(context.Background(), budget))
				defer cancel()
				var stream source.BatchStream
				if strategy == "sorted" {
					stream, err = p.NewSortedAggregateStream(ctx, p.resultSchema(), 1)
				} else {
					stream, err = p.NewResidentAggregateStream(ctx, p.resultSchema(), 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				var workspace *aggregatePredicateWorkspace
				replace := func(in source.BatchStream) {
					switch s := stream.(type) {
					case *sortedAggregateStream:
						s.input.Close()
						s.input = in
						workspace = &s.predicate
					case *residentAggregateStream:
						s.input.Close()
						s.input = in
						workspace = &s.predicate
					}
				}
				index := 0
				injected := errors.New("group binding input failure")
				numeric := &memory.Uint64Vector{}
				strings := &memory.StringViewVector{}
				input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
					assertGroupBindingCleared(t, workspace)
					b.ClearColumns()
					if index == 3 {
						switch terminal {
						case "input-error":
							return false, injected
						case "cancel":
							cancel()
							return false, context.Canceled
						case "invalid":
							b.Rows = 2
							b.Uint64s["k"] = &memory.Uint64Vector{Values: []uint64{99}}
							b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 0, 0}}
							return true, nil
						}
						return false, nil
					}
					n := []int{65, 0, 1}[index]
					index++
					numeric.Values = make([]uint64, n)
					numeric.Validity = nil
					strings.Offsets = make([]uint32, n+1)
					strings.Data = []byte("x")
					strings.Validity = nil
					for i := 0; i < n; i++ {
						numeric.Values[i] = 1
						strings.Offsets[i+1] = 1
					}
					// Recycled headers and fresh backing buffers on every batch. The first
					// string is x, later entries are empty, preserving adjacent group runs.
					if n > 0 {
						for i := 1; i < n; i++ {
							numeric.Values[i] = 2
						}
					}
					if index == 3 && n > 0 {
						numeric.Values[0] = 3
					}
					b.Rows = n
					b.Uint64s["k"] = numeric
					b.Strings["s"] = strings
					return true, nil
				}}
				replace(input)
				if terminal != "early-close" {
					output := memory.NewBatch()
					var seen uint64
					var gotErr error
					for {
						ok, err := stream.Next(ctx, output)
						if err != nil {
							gotErr = err
							break
						}
						if !ok {
							break
						}
						for _, v := range output.Uint64s["n"].Values[:output.Rows] {
							seen += v
						}
					}
					if terminal == "eof" {
						if gotErr != nil || seen != 66 {
							t.Fatalf("rows=%d err=%v", seen, gotErr)
						}
					} else if gotErr == nil {
						t.Fatal("missing terminal error")
					}
					if terminal == "input-error" && !errors.Is(gotErr, injected) {
						t.Fatalf("input error=%v", gotErr)
					}
					if terminal == "cancel" && !errors.Is(gotErr, context.Canceled) {
						t.Fatalf("cancel=%v", gotErr)
					}
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				assertGroupBindingCleared(t, workspace)
				if workspace.keys.lease != nil || workspace.keys.components != nil || budget.Used() != 0 || input.closed != 1 {
					t.Fatalf("cleanup bytes=%d closed=%d", budget.Used(), input.closed)
				}
			})
		}
	}
}

func TestGroupBindingOwnerReservationAndRebind(t *testing.T) {
	fields := []memory.Field{{Name: "k", Type: memory.TypeUint64}, {Name: "s", Type: memory.TypeString}}
	per := int64(unsafe.Sizeof(canonicalGroupComponent{}))
	manager := memory.NewBudgetManager(2 * per)
	w := aggregatePredicateWorkspace{manager: manager}
	defer w.close()
	b := memory.NewBatch()
	b.Rows = 1
	b.Uint64s["k"] = &memory.Uint64Vector{Values: []uint64{1}}
	b.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1}, Data: []byte("x")}
	binding, err := w.bindKeys(context.Background(), fields, b)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.memoryBytes(); got != 2*per || manager.Used() != got {
		t.Fatalf("bytes=%d used=%d", got, manager.Used())
	}
	old := binding
	before := w.keys.lease
	if _, err = w.bindKeys(context.Background(), append(fields, memory.Field{Name: "extra", Type: memory.TypeInt8}), b); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatalf("growth=%v", err)
	}
	if w.keys.lease != before || old != &w.keys.binding || manager.Used() != 2*per {
		t.Fatal("failed reservation mutated ownership")
	}
	w.keys.clear()
	assertGroupBindingCleared(t, &w)
	b.Uint64s["k"].Values = nil
	if _, err = w.bindKeys(context.Background(), fields, b); err == nil {
		t.Fatal("accepted malformed batch")
	}
	assertGroupBindingCleared(t, &w)
	w.close()
	w.close()
	if manager.Used() != 0 {
		t.Fatal("leaked descriptors")
	}
}
