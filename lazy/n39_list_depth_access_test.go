package lazy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

// Expected results come from ordinary Go values, never expression evaluators.
func n39Length(v any) any {
	if v == nil {
		return nil
	}
	return int64(len(v.([]any)))
}
func n39Get(v any, index int64) any {
	if v == nil {
		return nil
	}
	a := v.([]any)
	if index < 0 || index >= int64(len(a)) {
		return nil
	}
	return a[index]
}
func TestN39ListDepthAccessPublic(t *testing.T) {
	for _, name := range []string{"ints", "strs"} {
		for _, index := range []int64{-1, 0, 1, 99} {
			t.Run(fmt.Sprintf("%s_index%d", name, index), func(t *testing.T) {
				path, values, schema := n38Fixture(t, false)
				o := engine.DefaultOptions()
				o.Workers = 1
				o.Pipeline.Workers = 1
				o.Pipeline.MinMorselRows = 1
				o.Pipeline.MaxMorselRows = 2
				o.ExecutionPolicy = engine.ExecutionMemoryOnly
				o.MemoryLimitBytes = 32 << 20
				o.PreparedCacheBytes = 0
				s := NewSession(o)
				defer s.Close()
				q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				get := ListGet(Col(name), expr.I64(index))
				inner := ListGet(get, expr.I64(0))
				q = q.SelectExprs(ListLength(Col(name)).Alias("length"), get.Alias("selected"), ListLength(get).Alias("selected_length"), ListGet(inner, expr.I64(0)).Alias("scalar"), get.Alias("selected_again"))
				rd, err := q.CollectBatches(context.Background(), BatchOptions{})
				if err != nil {
					t.Fatalf("ACCESS BARRIER: %v", err)
				}
				defer rd.Close()
				at := 0
				for rd.Next(context.Background()) {
					v := rd.Batch()
					rec, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
					if err != nil {
						t.Fatal(err)
					}
					expected := schema.Field(schema.FieldIndices(name)[0]).Type.(*arrow.ListType).Elem()
					for _, j := range []int{1, 4} {
						if !rec.Schema().Field(j).Nullable {
							rec.Release()
							t.Fatal("ListGet output must be nullable")
						}
						n38CheckType(t, rec.Schema().Field(j).Type, expected)
					}
					for i := 0; i < int(rec.NumRows()); i++ {
						col := 1
						if name == "strs" {
							col = 2
						}
						raw := values[at][col]
						selected := n39Get(raw, index)
						want := []any{n39Length(raw), selected, n39Length(selected), n39Get(n39Get(selected, 0), 0), selected}
						for j, w := range want {
							got := n38Read(rec.Column(j), i)
							if !reflect.DeepEqual(got, w) {
								rec.Release()
								t.Fatalf("row=%d column=%d got=%#v want=%#v", at, j, got, w)
							}
						}
						at++
					}
					rec.Release()
				}
				if err := rd.Err(); err != nil {
					t.Fatal(err)
				}
				rd.Close()
				if at != len(values) {
					t.Fatalf("rows=%d want=%d", at, len(values))
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("leaked query memory")
				}
			})
		}
	}
}

// Nullable and potentially failing indices exercise the real expression mask.
// Inactive casts must not be evaluated, while a selected bad cast must fail.
func TestN39ListDepthAccessPublicGuardedIndex(t *testing.T) {
	path, values, _ := n38Fixture(t, false)
	o := engine.DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.Pipeline.MinMorselRows = 1
	o.Pipeline.MaxMorselRows = 2
	o.ExecutionPolicy = engine.ExecutionMemoryOnly
	o.MemoryLimitBytes = 32 << 20
	o.PreparedCacheBytes = 0
	s := NewSession(o)
	defer s.Close()
	q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if err != nil {
		t.Fatal(err)
	}
	bad := expr.CastTo(expr.Str("not-an-index"), memory.TypeInt64)
	safeIndex := expr.If(expr.GE(Col("k"), expr.I64(0)), expr.I64(0), bad)
	nullIndex := expr.If(expr.EQ(Col("k"), expr.I64(4)), expr.Null(memory.TypeInt64), expr.I64(0))
	qgood := q.SelectExprs(ListGet(Col("ints"), safeIndex).Alias("safe"), ListGet(Col("ints"), nullIndex).Alias("nullable"), ListLength(ListGet(Col("strs"), safeIndex)).Alias("length"))
	rd, err := qgood.CollectBatches(context.Background(), BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	at := 0
	for rd.Next(context.Background()) {
		v := rd.Batch()
		rec, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
		if err != nil {
			rd.Close()
			t.Fatal(err)
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			selected := n39Get(values[at][1], 0)
			nullable := selected
			if at == 4 {
				nullable = nil
			}
			want := []any{selected, nullable, n39Length(n39Get(values[at][2], 0))}
			for j, w := range want {
				if got := n38Read(rec.Column(j), i); !reflect.DeepEqual(got, w) {
					rec.Release()
					rd.Close()
					t.Fatalf("row %d column %d got %#v want %#v", at, j, got, w)
				}
			}
			at++
		}
		rec.Release()
	}
	err = rd.Err()
	rd.Close()
	if err != nil || at != len(values) {
		t.Fatalf("safe rows=%d err=%v", at, err)
	}
	qbad := q.SelectExprs(ListGet(Col("ints"), expr.If(expr.LT(Col("k"), expr.I64(4)), expr.I64(0), bad)).Alias("bad"))
	rb, err := qbad.CollectBatches(context.Background(), BatchOptions{})
	if err == nil {
		for rb.Next(context.Background()) {
		}
		err = rb.Err()
		rb.Close()
	}
	if err == nil {
		t.Fatal("active invalid numeric cast silently succeeded")
	}
	t.Logf("active invalid index cast: %T %v", err, err)
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("error path leaked query memory")
	}
}

func TestN39ListDepthAccessPublicDownstreamAndBudget(t *testing.T) {
	for _, limit := range []int64{32 << 20, 1024} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			path, values, _ := n38Fixture(t, false)
			o := engine.DefaultOptions()
			o.Workers = 1
			o.Pipeline.Workers = 1
			o.Pipeline.MinMorselRows = 1
			o.Pipeline.MaxMorselRows = 3
			o.ExecutionPolicy = engine.ExecutionMemoryOnly
			o.MemoryLimitBytes = limit
			o.PreparedCacheBytes = 0
			s := NewSession(o)
			defer s.Close()
			q, err := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
			if err != nil {
				t.Fatal(err)
			}
			q = q.SelectExprs(ListGet(Col("strs"), expr.I64(0)).Alias("picked"), ListLength(Col("strs")).Alias("n"))
			q = q.Filter(expr.GT(Col("n"), expr.I64(0))).SelectExprs(Col("picked").Alias("picked"), ListLength(Col("picked")).Alias("inner_length"), ListGet(ListGet(Col("picked"), expr.I64(0)), expr.I64(0)).Alias("scalar"))
			rd, err := q.CollectBatches(context.Background(), BatchOptions{})
			if limit == 1024 {
				if err == nil {
					for rd.Next(context.Background()) {
					}
					err = rd.Err()
					rd.Close()
				}
				if !errors.Is(err, memory.ErrBudgetExceeded) {
					t.Fatalf("expected actual resource failure, got %T %v", err, err)
				}
				var resource *memory.ResourceError
				if errors.As(err, &resource) {
					t.Logf("actual resource error: %+v", resource)
				} else {
					t.Logf("actual budget error without structured fields: %T %v", err, err)
				}
				if s.Engine().Memory().Used() != 0 {
					t.Fatal("budget failure leaked memory")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer rd.Close()
			var expected [][]any
			for _, v := range values {
				if v[2] == nil || len(v[2].([]any)) == 0 {
					continue
				}
				picked := n39Get(v[2], 0)
				expected = append(expected, []any{picked, n39Length(picked), n39Get(n39Get(picked, 0), 0)})
			}
			at := 0
			for rd.Next(context.Background()) {
				v := rd.Batch()
				rec, err := memory.ArrowRecord(am.DefaultAllocator, v.Schema(), v.state.batches[0])
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < int(rec.NumRows()); i++ {
					if at >= len(expected) {
						rec.Release()
						t.Fatal("extra downstream row")
					}
					for j, w := range expected[at] {
						if got := n38Read(rec.Column(j), i); !reflect.DeepEqual(got, w) {
							rec.Release()
							t.Fatalf("downstream row %d col %d got %#v want %#v", at, j, got, w)
						}
					}
					at++
				}
				rec.Release()
			}
			err = rd.Err()
			rd.Close()
			if err != nil || at != len(expected) {
				t.Fatalf("downstream rows %d want %d err=%v", at, len(expected), err)
			}
			if s.Engine().Memory().Used() != 0 {
				t.Fatal("downstream leaked query memory")
			}
		})
	}
}
