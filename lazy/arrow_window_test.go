package lazy

import (
	"context"
	"gocolumn/engine"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestWindowOverArrowRecordBatches(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	r1 := windowArrowRecord(t, alloc, []int32{1, 2}, []int64{20, 5})
	defer r1.Release()
	r2 := windowArrowRecord(t, alloc, []int32{1, 2}, []int64{10, 7})
	defer r2.Release()
	session := NewSession(engine.DefaultOptions())
	defer session.Close()
	frame, err := session.ScanArrow(r1, r2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := frame.WithWindows(NewWindow(PartitionBy("grp"), OrderBy(Asc("score"))), RowNumber("rn"), Rank("rank")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if got := frameInt64Column(t, result, "rn"); !equalI64(got, []int64{2, 1, 1, 2}) {
		t.Fatalf("arrow row number=%v", got)
	}
	if got := frameInt64Column(t, result, "rank"); !equalI64(got, []int64{2, 1, 1, 2}) {
		t.Fatalf("arrow rank=%v", got)
	}
}

func windowArrowRecord(t *testing.T, alloc arrowmem.Allocator, groups []int32, scores []int64) arrow.RecordBatch {
	t.Helper()
	gb := array.NewInt32Builder(alloc)
	defer gb.Release()
	gb.AppendValues(groups, nil)
	group := gb.NewArray()
	defer group.Release()
	sb := array.NewInt64Builder(alloc)
	defer sb.Release()
	sb.AppendValues(scores, nil)
	score := sb.NewArray()
	defer score.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "grp", Type: arrow.PrimitiveTypes.Int32}, {Name: "score", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{group, score}, int64(len(groups)))
}
