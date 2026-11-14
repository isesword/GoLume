package parquet

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

type recoveryBloomCancelReader struct {
	*os.File
	cancel context.CancelFunc
	armed  bool
}

func (r *recoveryBloomCancelReader) ReadAt(p []byte, off int64) (int, error) {
	n, e := r.File.ReadAt(p, off)
	if r.armed {
		r.cancel()
	}
	return n, e
}
func TestRecoveryBloomCancellationDuringRead(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2)
	pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: expr.EQ(expr.Col("x"), expr.I64(50))})
	if err != nil {
		t.Fatal(err)
	}
	for _, streamLifetime := range []bool{false, true} {
		t.Run(map[bool]string{true: "stream", false: "next"}[streamLifetime], func(t *testing.T) {
			f, e := os.Open(src.path)
			if e != nil {
				t.Fatal(e)
			}
			canceled, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &recoveryBloomCancelReader{File: f, cancel: cancel}
			r, e := file.NewParquetReader(input)
			if e != nil {
				f.Close()
				t.Fatal(e)
			}
			lifetime, call := context.Background(), canceled
			if streamLifetime {
				lifetime, call = canceled, context.Background()
			}
			s := pushed.(*Source).batchStreamFromReader(lifetime, testReadOptions().Normalize(), pushed.(*Source).parts, r)
			defer s.Close()
			input.armed = true
			if ok, e := s.Next(call, memory.NewBatch()); ok || !errors.Is(e, context.Canceled) {
				t.Fatalf("ok=%v err=%v", ok, e)
			}
		})
	}
}
func TestRecoveryBloomUnsafeProjectedBinding(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2)
	r, err := file.OpenParquetFile(src.path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := expr.EQ(expr.Col("x"), expr.I64(50))
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeUint8, memory.TypeUint16, memory.TypeString, memory.TypeDecimal, memory.TypeTimestamp} {
		bindings := append([]columnBinding(nil), src.bindings...)
		bindings[1].Type = typ
		if bloomGroupExcluded(context.Background(), r, 0, bindings, p) {
			t.Fatalf("unsafe projected type %v pruned", typ)
		}
	}
}
func TestRecoveryBloomInt32(t *testing.T) {
	path := filepath.Join(t.TempDir(), "i32.parquet")
	root, err := schema.NewGroupNode("schema", pq.Repetitions.Required, schema.FieldList{schema.NewInt32Node("x", pq.Repetitions.Required, -1)}, -1)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := file.NewParquetWriter(f, root, file.WithWriterProps(pq.NewWriterProperties(pq.WithDictionaryDefault(false), pq.WithBloomFilterEnabled(true), pq.WithBloomFilterNDV(2))))
	rg := w.AppendRowGroup()
	col, err := rg.NextColumn()
	if err != nil {
		t.Fatal(err)
	}
	values := make([]int32, 16384)
	for i := range values {
		if i%2 == 0 {
			values[i] = math.MinInt32
		} else {
			values[i] = math.MaxInt32
		}
	}
	if _, err = col.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err = col.Close(); err != nil {
		t.Fatal(err)
	}
	if err = rg.Close(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	src := openBatchStreamFixture(t, path)
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, tc := range []struct {
		value   int64
		exclude bool
	}{{math.MinInt32, false}, {math.MaxInt32, false}, {50, true}, {int64(math.MinInt32) - 1, true}, {int64(math.MaxInt32) + 1, true}} {
		if got := bloomGroupExcluded(context.Background(), r, 0, src.bindings, expr.EQ(expr.Col("x"), expr.I64(tc.value))); got != tc.exclude {
			t.Fatalf("value=%d exclude=%v want=%v", tc.value, got, tc.exclude)
		}
	}
}

func TestRecoveryBloomPreservesFalliblePredicate(t *testing.T) {
	src := recoveryBloomFixture(t, 16384, true, 2)
	miss := expr.EQ(expr.Col("x"), expr.I64(50))
	fallible := expr.GT(expr.CastTo(expr.Col("id"), memory.TypeInt8), expr.I64(0))
	for _, p := range []expr.Expr{expr.And(miss, fallible), expr.And(fallible, miss)} {
		pushed, _, err := src.ApplyPushdown(context.Background(), source.PushdownRequest{Predicate: p})
		if err != nil {
			t.Fatal(err)
		}
		s := pushed.(*Source)
		if s.pagePredicate != nil {
			t.Fatal("fallible predicate retained runtime pruning")
		}
		rows, _ := recoveryBloomDecoded(t, s, false)
		if rows != 16384 {
			t.Fatalf("fallible predicate skipped rows: %d", rows)
		}
	}
}
