package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"testing"
	"unsafe"
)

func TestFutureCloneAdmissionNestedMetadataAndCapacity(t *testing.T) {
	const rows = 257
	child := memory.NewBatch()
	child.Rows = rows
	child.Int8s["small"] = &memory.Int8Vector{Values: make([]int8, rows)}
	list := &memory.StringListVector{Offsets: make([]uint32, rows+1), Values: &memory.StringViewVector{Offsets: make([]uint32, rows+1), Data: make([]byte, rows*17)}}
	for i := 0; i <= rows; i++ {
		list.Offsets[i] = uint32(i)
		list.Values.Offsets[i] = uint32(i * 17)
	}
	child.StringLists["list"] = list
	fields := []memory.Field{{Name: "small", Type: memory.TypeInt8}, {Name: "list", Type: memory.TypeList}}
	batch := memory.NewBatch()
	batch.Rows = rows
	batch.Structs["record"] = &memory.StructVector{Rows: rows, Fields: fields, Values: child, Validity: make(memory.Bitmap, memory.WordsFor(rows))}
	for i := 1; i < rows; i++ {
		memory.Set(batch.Structs["record"].Validity, i)
	}
	var estimate int64
	var estimateErr error
	if allocs := testing.AllocsPerRun(100, func() { estimate, estimateErr = futureCloneAdmissionBytes(batch) }); allocs != 0 {
		t.Fatalf("admission estimator allocated %g times", allocs)
	}
	if estimateErr != nil {
		t.Fatal(estimateErr)
	}
	clone, err := memory.CloneBatchRange(batch, 0, rows)
	if err != nil {
		t.Fatal(err)
	}
	defer clone.ReleaseOwned()
	record := clone.Structs["record"]
	small := record.Values.Int8s["small"]
	gotList := record.Values.StringLists["list"]
	retained := int64(cap(record.Fields))*int64(unsafe.Sizeof(memory.Field{})) + int64(cap(record.Validity))*8 + int64(cap(small.Values)) + int64(cap(small.Validity))*8 + int64(cap(gotList.Offsets))*4 + int64(cap(gotList.Validity))*8 + int64(cap(gotList.Values.Offsets))*4 + int64(cap(gotList.Values.Data)) + int64(cap(gotList.Values.Validity))*8
	// The envelope also covers replacement overlap, vector descriptors and maps.
	if estimate < retained*2 {
		t.Fatalf("estimate=%d retained payload/fields=%d lacks growth allowance", estimate, retained)
	}
	if cap(small.Values) <= len(small.Values) {
		t.Fatal("fixture did not exercise append spare capacity")
	}
	if gotList.Len() != rows || gotList.IsValid(0) || record.IsValid(0) {
		t.Fatal("clone null semantics changed")
	}
}

func TestFutureAdmissionRejectsCyclesAndCheckedOverflow(t *testing.T) {
	b := memory.NewBatch()
	b.Rows = 1
	b.Structs["cycle"] = &memory.StructVector{Rows: 1, Values: b}
	if _, err := futureCloneAdmissionBytes(b); err == nil {
		t.Fatal("cycle was not rejected before recursive payload sizing")
	}
	for _, tc := range [][3]int64{{math.MaxInt64, 1, 1}, {0, math.MaxInt64, 4}, {-1, 1, 1}, {0, -1, 1}} {
		if _, err := futureBudgetAdd(tc[0], tc[1], tc[2]); err == nil {
			t.Fatal("overflow accepted", tc)
		}
	}
	if n, err := futureBudgetAdd(math.MaxInt64-4, 1, 4); err != nil || n != math.MaxInt64 {
		t.Fatal(n, err)
	}
}

func TestPastOnlyWindowPreparationDoesNotAllocate(t *testing.T) {
	owner := &WindowSource{expressions: []WindowExpression{{Function: WindowRowNumber}, {Function: WindowShift, Frame: WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 1}}}}}
	if allocs := testing.AllocsPerRun(100, func() {
		out, input, hidden := prepareFutureWindow(owner, nil)
		if out != owner || input != nil || hidden != nil {
			panic("past path changed")
		}
	}); allocs != 0 {
		t.Fatalf("past-only setup allocated %g times", allocs)
	}
}

type futureOpenCounter struct {
	source.Source
	opens int
}

func (s *futureOpenCounter) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	return source.OpenBatchStream(ctx, s.Source, opts)
}

func TestFutureSetupRejectsBeforeOpeningInput(t *testing.T) {
	input := &futureOpenCounter{Source: futureTestSource(t, 2)}
	manager := memory.NewBudgetManager(80 << 10)
	ex := make([]WindowExpression, 2048)
	fields := make([]memory.Field, len(ex))
	bound := WindowBound{Kind: WindowFollowing, Offset: 1}
	for i := range ex {
		name := fmt.Sprintf("out%d", i)
		ex[i] = WindowExpression{Function: WindowShift, Column: "v", Alias: name, Frame: WindowFrame{Start: bound, End: bound}}
		fields[i] = memory.Field{Name: name, Type: memory.TypeInt64, Nullable: true}
	}
	owner, err := NewWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, ex, fields, manager, WindowSourceOptions{Mode: WindowExecutionOrdered})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.OpenBatchStream(context.Background(), owner, source.ReadOptions{Kernels: offsetTestKernels()})
	if !errors.Is(err, memory.ErrBudgetExceeded) || input.opens != 0 || manager.Used() != 0 {
		t.Fatal(err, input.opens, manager.Used())
	}
}

func TestFutureCloseDropsDerivedDescriptorsAndRings(t *testing.T) {
	manager := memory.NewBudgetManager(128 << 10)
	owner := futureTestWindow(t, futureTestSource(t, 20), manager, 3).(*WindowSource)
	for run := 0; run < 2; run++ {
		stream, err := newOrderedWindowStream(context.Background(), owner, owner.input, owner.output, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
		if err != nil {
			t.Fatal(err)
		}
		ordered := stream.(*orderedWindowStream)
		// The direct child is the private future cursor; its source is independent
		// of the reusable public owner retained above.
		_, err = ordered.Next(context.Background(), memory.NewBatch())
		if err != nil {
			t.Fatal(err)
		}
		ordered.Close()
		if ordered.owner != nil || ordered.child != nil || ordered.input != nil || ordered.states != nil || ordered.hiddenColumns != nil || len(ordered.inputSchema.Fields) != 0 || len(ordered.output.Fields) != 0 || manager.Used() != 0 {
			t.Fatal("closed stream retained execution-owned state")
		}
	}
	prepared, input, _ := prepareFutureWindow(owner, owner.input)
	_ = prepared
	future := input.(*futureWindowSource)
	stream, err := future.NewBatchStream(context.Background(), source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	cursor := stream.(*futureWindowStream)
	cursor.Close()
	if cursor.owner != nil || cursor.child != nil || cursor.scratch != nil || manager.Used() != 0 {
		t.Fatal("closed future cursor retained descriptors")
	}
}
