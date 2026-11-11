package batchview

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
)

func reviewPackedBoolean(t *testing.T, n int) *array.Boolean {
	t.Helper()
	alloc := arrowmem.NewCheckedAllocator(arrowmem.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	b := array.NewBooleanBuilder(alloc)
	for i := 0; i < n; i++ {
		b.Append(i%3 == 0)
	}
	a := b.NewBooleanArray()
	b.Release()
	t.Cleanup(a.Release)
	return a
}

func TestReviewBooleanMixedPayloadMatchesOutput(t *testing.T) {
	a := reviewPackedBoolean(t, 200)
	sliced := array.NewSlice(a, 7, 147).(*array.Boolean)
	defer sliced.Release()
	src := memory.NewBatch()
	src.Rows = 140
	valid := memory.EnsureBitmap(nil, 140)
	for i := 0; i < 140; i++ {
		if i%5 != 0 {
			memory.Set(valid, i)
		}
	}
	src.Opaque["flag"] = &memory.OpaqueVector{Rows: 140, Type: memory.TypeBool, Value: sliced, Validity: valid}
	src.Int64s["number"] = &memory.Int64Vector{Values: make([]int64, 140)}
	// Same Arrow Boolean with a different logical type must remain opaque.
	src.Opaque["other"] = &memory.OpaqueVector{Rows: 140, Type: memory.TypeUint32, Value: sliced}
	out := memory.NewBatch()
	var v View
	defer out.ClearColumns()
	defer v.Release()
	manager := memory.NewBudgetManager(4096)
	for _, offset := range []int{0, 1, 63, 64, 65} {
		for _, rows := range []int{1, 7, 65} {
			opts := source.ReadOptions{BatchRows: rows, BatchBytes: 4096, MemoryManager: manager}
			got, err := v.Bind(context.Background(), out, src, offset, opts)
			if err != nil || got != rows {
				t.Fatalf("bind = %d, %v", got, err)
			}
			estimate, err := decodedPayloadBytes(src, offset, rows)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := memory.EstimateBatchRangeBytes(out, 0, rows)
			if err != nil || actual != estimate {
				t.Fatalf("payload actual=%d estimate=%d err=%v", actual, estimate, err)
			}
			wantScratch := int64(rows * 4)
			if offset%64 != 0 {
				wantScratch += int64(memory.WordsFor(rows)) * 8
			}
			if manager.Used() != wantScratch {
				t.Fatalf("used %d want %d", manager.Used(), wantScratch)
			}
			if _, ok := out.Opaque["other"].Value.(arrow.Array); !ok {
				t.Fatal("non-Boolean logical type changed")
			}
			for i, x := range out.Uint32s["flag"].Values {
				want := uint32(0)
				if (offset+i)%5 != 0 && (7+offset+i)%3 == 0 {
					want = 1
				}
				if x != want {
					t.Fatalf("offset=%d row=%d got=%d want=%d", offset, i, x, want)
				}
			}
		}
	}
	v.Release()
	if manager.Used() != 0 {
		t.Fatal("reservation leaked")
	}
}

func TestReviewBooleanNestedAccountingAndCanceledBind(t *testing.T) {
	a := reviewPackedBoolean(t, 20)
	child := memory.NewBatch()
	child.Rows = 20
	child.Opaque["flag"] = &memory.OpaqueVector{Rows: 20, Type: memory.TypeBool, Value: a}
	src := memory.NewBatch()
	src.Rows = 20
	src.Structs["nested"] = &memory.StructVector{Rows: 20, Values: child}
	src.Opaque["flag"] = &memory.OpaqueVector{Rows: 20, Type: memory.TypeBool, Value: a}
	manager := memory.NewBudgetManager(1024)
	out := memory.NewBatch()
	var v View
	defer out.ClearColumns()
	defer v.Release()
	opts := source.ReadOptions{BatchRows: 20, BatchBytes: 48, BatchPayloadBytes: 40, MemoryManager: manager}
	n, err := v.Bind(context.Background(), out, src, 1, opts)
	if err != nil || n != 5 || manager.Used() != 40 {
		t.Fatalf("nested bind=%d/%v used=%d", n, err, manager.Used())
	}
	if len(out.Structs["nested"].Values.Uint32s["flag"].Values) != 5 {
		t.Fatal("nested decode missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err = v.Bind(ctx, out, src, 1, opts)
	if n != 0 || !errors.Is(err, context.Canceled) || out.Rows != 0 || manager.Used() != 0 {
		t.Fatalf("canceled bind=%d/%v rows=%d used=%d", n, err, out.Rows, manager.Used())
	}
}

func TestReviewBooleanTypedNilReturnsError(t *testing.T) {
	for _, payload := range []int64{0, 64} {
		t.Run(fmt.Sprint(payload), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("typed-nil Boolean backing panicked: %v", p)
				}
			}()
			var a *array.Boolean
			src := memory.NewBatch()
			src.Rows = 1
			src.Opaque["flag"] = &memory.OpaqueVector{Rows: 1, Type: memory.TypeBool, Value: a}
			out := memory.NewBatch()
			defer out.ClearColumns()
			var v View
			defer v.Release()
			_, err := v.Bind(context.Background(), out, src, 0, source.ReadOptions{BatchRows: 1, BatchBytes: 64, BatchPayloadBytes: payload})
			if err == nil {
				t.Fatal("typed-nil Boolean backing accepted")
			}
		})
	}
}
