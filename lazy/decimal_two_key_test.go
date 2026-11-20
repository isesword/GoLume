package lazy

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/memory"
)

func TestN23DecimalOrderedTwoKeyLazy(t *testing.T) {
	ctx := context.Background()
	s := n18Session(t, engine.ExecutionMemoryOnly)
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 1}}}, nil)
	b := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range []struct {
		x, y, d int64
		xn, yn  bool
	}{{0, 7, 40, true, false}, {1, 3, 20, false, false}, {1, 0, 11, false, true}, {2, 5, -25, false, false}, {1, 3, 30, false, false}} {
		x, y := b.Field(0).(*array.Int64Builder), b.Field(1).(*array.Int64Builder)
		if r.xn {
			x.AppendNull()
		} else {
			x.Append(r.x)
		}
		if r.yn {
			y.AppendNull()
		} else {
			y.Append(r.y)
		}
		b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(r.d))
	}
	r := b.NewRecordBatch()
	defer r.Release()
	src, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	group := func(f LazyFrame) LazyFrame {
		return f.GroupBy("x", "y").AggConfigured(AvgDecimal128("d", 4, 0, expr.DecimalRoundHalfEven, "mean"), Sum("d", "sum"), Count("n"))
	}
	x, y := Asc("x"), DescNullsFirst("y")
	x.NullsLast = true
	// An incomplete ordering is now acceptable only to MemoryOnly resident
	// grouping. Bounded retains the original rejection probes.
	boundedOptions := engine.DefaultOptions()
	boundedOptions.ExecutionPolicy = engine.ExecutionBounded
	boundedOptions.MemoryLimitBytes = 64 << 20
	boundedOptions.Spill = memory.SpillOptions{Directory: t.TempDir(), MaxBytes: 64 << 20}
	bounded := NewSession(boundedOptions)
	defer bounded.Close()
	negative, e := bounded.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range []LazyFrame{negative, negative.Sort(x), negative.Sort(y, x)} {
		v, e := group(f).Collect(ctx)
		if e == nil {
			v.Close()
			t.Fatal("unsupported sort proof accepted")
		}
		if !strings.Contains(e.Error(), "unordered Decimal grouping") {
			t.Fatal("did not reach grouping policy", e)
		}
	}
	q, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	df, e := group(src.Sort(x, y)).Collect(memory.WithQueryBudget(ctx, q))
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	type expected struct {
		x, y, mean, sum int64
		xn, yn          bool
	}
	want := []expected{{1, 0, 1, 11, false, true}, {1, 3, 2, 50, false, false}, {2, 5, -2, -25, false, false}, {0, 7, 4, 40, true, false}}
	for _, name := range []string{"x", "y", "mean", "sum", "n"} {
		col, e := df.Column(name)
		if e != nil {
			t.Fatal(e)
		}
		pos := 0
		switch name {
		case "x", "y":
			cs, e := col.Int64Chunks()
			if e != nil {
				t.Fatal(e)
			}
			for _, c := range cs {
				for i := 0; i < c.Len(); i++ {
					v, ok, e := c.Value(i)
					if e != nil || pos >= len(want) {
						t.Fatal(name, pos, e)
					}
					w := want[pos]
					exp, null := w.x, w.xn
					if name == "y" {
						exp, null = w.y, w.yn
					}
					if ok == null || ok && v != exp {
						t.Fatal(name, pos, v, ok, w)
					}
					pos++
				}
			}
		case "mean", "sum":
			cs, e := col.Decimal128Chunks()
			if e != nil {
				t.Fatal(e)
			}
			for _, c := range cs {
				p, sc := int32(4), int32(0)
				if name == "sum" {
					p, sc = 38, 1
				}
				if c.Precision() != p || c.Scale() != sc {
					t.Fatal(name, c.Precision(), c.Scale())
				}
				for i := 0; i < c.Len(); i++ {
					v, ok, e := c.Value(i)
					if e != nil || !ok || pos >= len(want) {
						t.Fatal(name, pos, e)
					}
					w := want[pos]
					exp := w.mean
					if name == "sum" {
						exp = w.sum
					}
					if v != decimal128.FromI64(exp) {
						t.Fatal(name, pos, v, w)
					}
					pos++
				}
			}
		case "n":
			cs, e := col.Uint64Chunks()
			if e != nil {
				t.Fatal(e)
			}
			for _, c := range cs {
				for i := 0; i < c.Len(); i++ {
					v, ok, e := c.Value(i)
					exp := uint64(1)
					if pos == 1 {
						exp = 2
					}
					if e != nil || !ok || v != exp {
						t.Fatal(name, pos, v, e)
					}
					pos++
				}
			}
		}
		if pos != len(want) {
			t.Fatal(name, pos, len(want))
		}
	}
	df.Close()
	if q.Used() != 0 {
		t.Fatal("query retained bytes", q.Used())
	}
}

func TestN24DecimalResidentTwoKeyLazy(t *testing.T) {
	ctx := context.Background()
	s := n18Session(t, engine.ExecutionMemoryOnly)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "d", Type: &arrow.Decimal128Type{Precision: 38, Scale: 1}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(am.DefaultAllocator, schema)
	defer b.Release()
	x, y, d := b.Field(0).(*array.Int64Builder), b.Field(1).(*array.Int64Builder), b.Field(2).(*array.Decimal128Builder)
	for _, v := range []struct {
		y, d int64
		null bool
	}{{2, 15, false}, {3, 25, false}, {0, 0, true}, {2, 25, false}} {
		x.Append(1)
		if v.null {
			y.AppendNull()
			d.AppendNull()
		} else {
			y.Append(v.y)
			d.Append(decimal128.FromI64(v.d))
		}
	}
	r := b.NewRecordBatch()
	defer r.Release()
	src, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	q, e := s.Engine().BeginQueryMemory()
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	df, e := src.GroupBy("x", "y").AggConfigured(Sum("d", "sum"), Count("n"), AvgDecimal128("d", 4, 0, expr.DecimalRoundHalfEven, "mean")).Collect(memory.WithQueryBudget(ctx, q))
	if e != nil {
		t.Fatal(e)
	}
	defer df.Close()
	ycol, e := df.Column("y")
	if e != nil {
		t.Fatal(e)
	}
	mean, e := df.Column("mean")
	if e != nil {
		t.Fatal(e)
	}
	ys, e := ycol.Int64Chunks()
	if e != nil {
		t.Fatal(e)
	}
	means, e := mean.Decimal128Chunks()
	if e != nil {
		t.Fatal(e)
	}
	if len(ys) != 1 || len(means) != 1 || ys[0].Len() != 3 || means[0].Len() != 3 {
		t.Fatal("unexpected output pages", len(ys), len(means))
	}
	seen := map[int64]bool{}
	for i := 0; i < 3; i++ {
		k, kv, e := ys[0].Value(i)
		if e != nil {
			t.Fatal(e)
		}
		if !kv {
			k = 0
		}
		v, vv, e := means[0].Value(i)
		if e != nil || seen[k] || !((!kv && !vv) || kv && (k == 2 || k == 3) && vv && v == decimal128.FromI64(2)) {
			t.Fatal(k, kv, v, vv, e)
		}
		seen[k] = true
	}
	df.Close()
	if q.Used() != 0 {
		t.Fatal(q.Used())
	}
}
