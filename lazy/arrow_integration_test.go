package lazy

import (
	"context"
	"gocolumn/engine"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"
)

func TestLazyArrowRecordBatchAggregatePushdown(t *testing.T) {
	alloc := arrowmem.NewGoAllocator()
	r1 := makeArrowAggRecord(t, alloc, []int64{1, 2}, []int32{1, 2}, []int64{10, 20}, []string{"cold", "cold"})
	defer r1.Release()
	r2 := makeArrowAggRecord(t, alloc, []int64{6, 7, 9}, []int32{1, 1, 2}, []int64{30, 40, 50}, []string{"hot", "hot", "hot"})
	defer r2.Release()

	s := NewSession(engine.DefaultOptions())
	defer s.Close()
	lf, err := s.ScanArrow(r1, r2)
	if err != nil {
		t.Fatal(err)
	}
	lf = lf.Filter(Col("pred").Gt(Lit(5))).Select("key", "value").GroupBy("key").Agg(Sum("value", "sum"), Count("count"))
	optimized, err := lf.ExplainOptimized()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(optimized, "unused") {
		t.Fatalf("unused column survived optimized projection:\n%s", optimized)
	}
	physical, err := lf.ExplainPhysical(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(physical, "row_groups=1/2") && !strings.Contains(physical, "partitions=1/2") {
		t.Fatalf("expected Arrow RecordBatch pruning in physical explain:\n%s", physical)
	}
	res, err := lf.CollectResult(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.SelectedRows != 3 || res.GroupCount != 2 || res.Checksum != 120 {
		t.Fatalf("result groups=%d selected=%d checksum=%d", res.GroupCount, res.SelectedRows, res.Checksum)
	}
}

func makeArrowAggRecord(t *testing.T, alloc arrowmem.Allocator, pred []int64, key []int32, value []int64, unused []string) arrow.RecordBatch {
	t.Helper()
	pb := array.NewInt64Builder(alloc)
	defer pb.Release()
	pb.AppendValues(pred, nil)
	pa := pb.NewArray()
	defer pa.Release()
	kb := array.NewInt32Builder(alloc)
	defer kb.Release()
	kb.AppendValues(key, nil)
	ka := kb.NewArray()
	defer ka.Release()
	vb := array.NewInt64Builder(alloc)
	defer vb.Release()
	vb.AppendValues(value, nil)
	va := vb.NewArray()
	defer va.Release()
	ub := array.NewStringBuilder(alloc)
	defer ub.Release()
	ub.AppendValues(unused, nil)
	ua := ub.NewArray()
	defer ua.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "pred", Type: arrow.PrimitiveTypes.Int64}, {Name: "key", Type: arrow.PrimitiveTypes.Int32}, {Name: "value", Type: arrow.PrimitiveTypes.Int64}, {Name: "unused", Type: arrow.BinaryTypes.String}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{pa, ka, va, ua}, int64(len(pred)))
}
