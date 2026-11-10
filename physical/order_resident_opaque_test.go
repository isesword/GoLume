package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"strings"
	"testing"
)

func TestResidentOpaqueRowsBackingOwnership(t *testing.T) {
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	builder := array.NewLargeStringBuilder(alloc)
	builder.Append(strings.Repeat("x", 1<<18))
	builder.Append("winner")
	builder.AppendNull()
	values := builder.NewArray()
	builder.Release()
	view := array.NewSlice(values, 1, 3)
	values.Release()
	src := memory.NewBatch()
	src.Rows = 2
	src.Opaque["o"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: arrow.BinaryTypes.LargeString.String(), Value: view}
	owner, err := newResidentOpaqueRows(ctx, memory.Schema{Fields: []memory.Field{{Name: "o", Type: memory.TypeArrow}}}, src, w)
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.appendRow(ctx, src, 0); err != nil {
		t.Fatal(err)
	}
	first := owner.reservation.Bytes()
	if first < 1<<18 {
		t.Fatalf("backing not admitted: %d", first)
	}
	if err = owner.appendRow(ctx, src, 1); err != nil {
		t.Fatal(err)
	}
	if owner.reservation.Bytes()-first >= 1<<18 {
		t.Fatal("same backing charged twice")
	}
	view.Release()
	src.Opaque = nil
	dst := memory.NewBatch()
	if err = owner.bindRow(dst, 0); err != nil {
		t.Fatal(err)
	}
	if dst.Opaque["o"].Owned {
		t.Fatal("binding transfers ownership")
	}
	if got := dst.Opaque["o"].Value.(*array.LargeString).Value(0); got != "winner" {
		t.Fatal(got)
	}
	*dst.Opaque["o"] = memory.OpaqueVector{}
	if err = owner.bindRow(dst, 0); err != nil {
		t.Fatal(err)
	}
	if dst.Opaque["o"].Value == nil {
		t.Fatal("clearing output damaged retained row")
	}
	if err = owner.bindRow(dst, 1); err != nil {
		t.Fatal(err)
	}
	if memory.IsSet(dst.Opaque["o"].Validity, 0) {
		t.Fatal("null lost")
	}
	dst.Opaque = nil
	owner.close()
	owner.close()
	if w.Used() != 0 {
		t.Fatal("reservation leak", w.Used())
	}
	alloc.AssertSize(t, 0)
}
func TestResidentOpaqueRowsDeniedAndCanceled(t *testing.T) {
	ctx := context.Background()
	w, parent := externalAccumulatorTestWorkspace(t, 0)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	builder := array.NewLargeStringBuilder(alloc)
	builder.Append(strings.Repeat("a", 1<<18))
	values := builder.NewArray()
	builder.Release()
	src := memory.NewBatch()
	src.Rows = 1
	src.Opaque["o"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeArrow, Value: values}
	owner, err := newResidentOpaqueRows(ctx, memory.Schema{Fields: []memory.Field{{Name: "o", Type: memory.TypeArrow}}}, src, w)
	if err != nil {
		t.Fatal(err)
	}
	parent.deny = true
	if err = owner.appendRow(ctx, src, 0); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if len(owner.fields[0].values) != 0 || owner.fields[0].sources.count != 0 {
		t.Fatal("allocated before admission")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = owner.appendRow(canceled, src, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	owner.close()
	values.Release()
	alloc.AssertSize(t, 0)
}

func TestResidentOpaqueRowsReorderOwnership(t *testing.T) {
	ctx := context.Background()
	w, _ := externalAccumulatorTestWorkspace(t, 0)
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	b := array.NewLargeStringBuilder(alloc)
	b.Append("first")
	b.Append("last")
	a := b.NewArray()
	b.Release()
	src := memory.NewBatch()
	src.Rows = 2
	src.Opaque["o"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, Value: a}
	schema := memory.Schema{Fields: []memory.Field{{Name: "o", Type: memory.TypeArrow}}}
	owner, err := newResidentOpaqueRows(ctx, schema, src, w)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = owner.appendRow(ctx, src, i); err != nil {
			t.Fatal(err)
		}
	}
	out, err := newResidentOpaqueRows(ctx, schema, src, w)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i >= 0; i-- {
		if err = out.appendFrom(ctx, owner, i); err != nil {
			t.Fatal(err)
		}
	}
	a.Release()
	owner.close()
	dst := memory.NewBatch()
	if err = out.bindRow(dst, 0); err != nil {
		t.Fatal(err)
	}
	if got := dst.Opaque["o"].Value.(*array.LargeString).Value(0); got != "last" {
		t.Fatal(got)
	}
	dst.Opaque = nil
	out.close()
	alloc.AssertSize(t, 0)
	ignored, err := newResidentOpaqueRows(ctx, memory.Schema{}, src, w)
	if err != nil || ignored != nil {
		t.Fatal("runtime extras not ignored", err)
	}
}

type residentOpaqueReplaySource struct {
	schema memory.Schema
	closes int
}

func (s *residentOpaqueReplaySource) Schema() memory.Schema { return s.schema }
func (s *residentOpaqueReplaySource) Stats(context.Context) (source.Stats, error) {
	return source.Stats{Rows: 6}, nil
}
func (s *residentOpaqueReplaySource) Partitions(context.Context) ([]source.Partition, error) {
	return []source.Partition{{ID: 0, Rows: 6}}, nil
}
func (s *residentOpaqueReplaySource) Replayability() source.Replayability { return source.ReplayStable }
func (s *residentOpaqueReplaySource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	p := &topKOneShotSource{schema: s.schema, batch: memory.NewBatch(), count: 3}
	p.fill = func(i int, b *memory.Batch) {
		b.ReleaseOwned()
		b.Rows = 2
		ids := []int64{5 - int64(2*i), 4 - int64(2*i)}
		b.Int64s["key"] = &memory.Int64Vector{Values: ids}
		builder := array.NewLargeStringBuilder(arrowmem.DefaultAllocator)
		for _, id := range ids {
			if id == 2 {
				builder.AppendNull()
			} else {
				builder.Append(fmt.Sprintf("value-%d", id))
			}
		}
		a := builder.NewArray()
		builder.Release()
		b.Opaque["raw"] = &memory.OpaqueVector{Rows: 2, Type: memory.TypeArrow, ArrowType: arrow.BinaryTypes.LargeString.String(), Value: a, Owned: true}
	}
	return &residentOpaqueReplayStream{topKOneShotStream: topKOneShotStream{owner: p}, source: s}, nil
}

type residentOpaqueReplayStream struct {
	topKOneShotStream
	source *residentOpaqueReplaySource
}

func (s *residentOpaqueReplayStream) Close() error {
	s.source.closes++
	return s.topKOneShotStream.Close()
}

func TestResidentOpaqueSortAndTopKAcrossBatches(t *testing.T) {
	for _, topK := range []bool{false, true} {
		for _, ending := range []string{"eof", "close", "cancel"} {
			t.Run(fmt.Sprintf("topk=%v/%s", topK, ending), func(t *testing.T) {
				alloc := arrowmem.NewCheckedAllocator(arrowmem.NewGoAllocator())
				previous := arrowmem.DefaultAllocator
				arrowmem.DefaultAllocator = alloc
				defer func() { arrowmem.DefaultAllocator = previous }()
				defer alloc.AssertSize(t, 0)
				input := &residentOpaqueReplaySource{schema: memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt64}, memory.Field{Name: "raw", Type: memory.TypeArrow, ArrowType: arrow.BinaryTypes.LargeString.String()})}
				manager := memory.NewBudgetManager(0)
				var ordered source.Source
				var err error
				want := 6
				wantCloses := 1
				if topK {
					want = 4
					wantCloses = 2
					ordered, err = NewTopKSourceWithManager(input, want, []SortKey{{Column: "key"}}, manager)
				} else {
					ordered, err = NewSortSource(input, []SortKey{{Column: "key"}}, manager)
				}
				if err != nil {
					t.Fatal(err)
				}
				stream, err := source.OpenBatchStream(context.Background(), ordered, testReadOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer stream.Close()
				out := memory.NewBatch()
				var ids []int64
				for len(ids) < want {
					ok, err := stream.Next(context.Background(), out)
					if !ok || err != nil {
						t.Fatal(ok, err)
					}
					if input.closes != wantCloses {
						t.Fatalf("source not closed before output: %d", input.closes)
					}
					a := out.Opaque["raw"].Value.(*array.LargeString)
					if out.Opaque["raw"].Owned || a.Len() != out.Rows {
						t.Fatal("opaque ownership or row shape changed")
					}
					for row, id := range out.Int64s["key"].Values {
						if id == 2 {
							if !a.IsNull(row) {
								t.Fatal("null lost")
							}
						} else if a.IsNull(row) || a.Value(row) != fmt.Sprintf("value-%d", id) {
							t.Fatal("opaque payload/key mismatch")
						}
						ids = append(ids, id)
					}
					out.ClearColumns()
				}
				expected := []int64{0, 1, 2, 3, 4, 5}[:want]
				if !reflect.DeepEqual(ids, expected) {
					t.Fatalf("ids=%v want=%v", ids, expected)
				}
				if ending != "close" {
					ctx := context.Background()
					if ending == "cancel" {
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					ok, err := stream.Next(ctx, out)
					if ok || ending == "eof" && err != nil || ending == "cancel" && !errors.Is(err, context.Canceled) {
						t.Fatal(ok, err)
					}
				}
				if err = stream.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatal("budget leak", manager.Used())
				}
				alloc.AssertSize(t, 0)
			})
		}
	}
}
