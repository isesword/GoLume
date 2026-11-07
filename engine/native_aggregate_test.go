package engine

import (
	"context"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"strings"
	"testing"
)

type nativeTestColumn struct {
	name  string
	typ   memory.DataType
	bits  []uint64
	valid memory.Bitmap
}

func nativeAggregateFixture(t testing.TB) (*inmemory.Source, []nativeTestColumn) {
	t.Helper()
	neg := func(n int64) uint64 { return uint64(n) }
	cols := []nativeTestColumn{
		{name: "i8", typ: memory.TypeInt8, bits: []uint64{neg(-128), 127, 2, 3, 4, 5}},
		{name: "i16", typ: memory.TypeInt16, bits: []uint64{neg(-32768), 32767, 2, 3, 4, 5}},
		{name: "i32", typ: memory.TypeInt32, bits: []uint64{math.MaxInt32, 1, 2, 3, 4, 5}},
		{name: "i64", typ: memory.TypeInt64, bits: []uint64{math.MaxInt64, 1, 2, 3, 4, 5}},
		{name: "u8", typ: memory.TypeUint8, bits: []uint64{255, 1, 2, 3, 4, 5}},
		{name: "u16", typ: memory.TypeUint16, bits: []uint64{65535, 1, 2, 3, 4, 5}},
		{name: "u32", typ: memory.TypeUint32, bits: []uint64{math.MaxUint32, 1, 2, 3, 4, 5}},
		{name: "u64", typ: memory.TypeUint64, bits: []uint64{math.MaxUint64 - 2, 1, 2, 3, 1<<63 + 7, 9}},
		{name: "f32", typ: memory.TypeFloat32, bits: []uint64{uint64(math.Float32bits(float32(math.NaN()))), uint64(math.Float32bits(7)), 1 << 31, 0, uint64(math.Float32bits(float32(math.Inf(1)))), uint64(math.Float32bits(4))}},
		{name: "f64", typ: memory.TypeFloat64, bits: []uint64{math.Float64bits(math.NaN()), math.Float64bits(7), 1 << 63, 0, math.Float64bits(math.Inf(1)), math.Float64bits(4)}},
	}
	batch := memory.NewBatch()
	batch.Rows = 6
	batch.Int32s["key"] = &memory.Int32Vector{Values: []int32{0, 0, 1, 1, 2, 2}, Validity: memory.Bitmap{0b111100}}
	fields := []memory.Field{{Name: "key", Type: memory.TypeInt32, Nullable: true}}
	for i := range cols {
		col := &cols[i]
		col.valid = memory.Bitmap{0b011111}
		fields = append(fields, memory.Field{Name: col.name, Type: col.typ, Nullable: true})
		fillNativeTestColumn(batch, *col)
	}
	fields = append(fields, memory.Field{Name: "s", Type: memory.TypeString, Nullable: true}, memory.Field{Name: "flag", Type: memory.TypeBool, Nullable: true})
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 1, 1, 2, 3, 3}, Data: []byte("abc"), Validity: memory.Bitmap{0b011011}}
	batch.Uint32s["flag"] = &memory.Uint32Vector{Values: []uint32{0, 1, 1, 0, 0, 1}, Validity: memory.Bitmap{0b100101}}
	cols = append(cols, nativeTestColumn{name: "s", typ: memory.TypeString, bits: make([]uint64, 6), valid: memory.Bitmap{0b011011}}, nativeTestColumn{name: "flag", typ: memory.TypeBool, bits: make([]uint64, 6), valid: memory.Bitmap{0b100101}})
	src, err := inmemory.New(memory.NewSchema(fields...), batch)
	if err != nil {
		t.Fatal(err)
	}
	return src, cols
}

func fillNativeTestColumn(b *memory.Batch, c nativeTestColumn) {
	switch c.typ {
	case memory.TypeInt8:
		v := make([]int8, len(c.bits))
		for i, bits := range c.bits {
			v[i] = int8(bits)
		}
		b.Int8s[c.name] = &memory.Int8Vector{Values: v, Validity: c.valid}
	case memory.TypeInt16:
		v := make([]int16, len(c.bits))
		for i, bits := range c.bits {
			v[i] = int16(bits)
		}
		b.Int16s[c.name] = &memory.Int16Vector{Values: v, Validity: c.valid}
	case memory.TypeInt32:
		v := make([]int32, len(c.bits))
		for i, bits := range c.bits {
			v[i] = int32(bits)
		}
		b.Int32s[c.name] = &memory.Int32Vector{Values: v, Validity: c.valid}
	case memory.TypeInt64:
		v := make([]int64, len(c.bits))
		for i, bits := range c.bits {
			v[i] = int64(bits)
		}
		b.Int64s[c.name] = &memory.Int64Vector{Values: v, Validity: c.valid}
	case memory.TypeUint8:
		v := make([]uint8, len(c.bits))
		for i, bits := range c.bits {
			v[i] = uint8(bits)
		}
		b.Uint8s[c.name] = &memory.Uint8Vector{Values: v, Validity: c.valid}
	case memory.TypeUint16:
		v := make([]uint16, len(c.bits))
		for i, bits := range c.bits {
			v[i] = uint16(bits)
		}
		b.Uint16s[c.name] = &memory.Uint16Vector{Values: v, Validity: c.valid}
	case memory.TypeUint32:
		v := make([]uint32, len(c.bits))
		for i, bits := range c.bits {
			v[i] = uint32(bits)
		}
		b.Uint32s[c.name] = &memory.Uint32Vector{Values: v, Validity: c.valid}
	case memory.TypeUint64:
		v := make([]uint64, len(c.bits))
		for i, bits := range c.bits {
			v[i] = bits
		}
		b.Uint64s[c.name] = &memory.Uint64Vector{Values: v, Validity: c.valid}
	case memory.TypeFloat32:
		v := make([]float32, len(c.bits))
		for i, bits := range c.bits {
			v[i] = math.Float32frombits(uint32(bits))
		}
		b.Float32s[c.name] = &memory.Float32Vector{Values: v, Validity: c.valid}
	case memory.TypeFloat64:
		v := make([]float64, len(c.bits))
		for i, bits := range c.bits {
			v[i] = math.Float64frombits(bits)
		}
		b.Float64s[c.name] = &memory.Float64Vector{Values: v, Validity: c.valid}
	}
}

func TestNativeAggregateStrategiesAndExactResults(t *testing.T) {
	for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "sorted", "spill", "global"} {
		t.Run(strategy, func(t *testing.T) {
			src, cols := nativeAggregateFixture(t)
			q := expr.Query{GroupBy: []string{"key"}, Hints: expr.Hints{Cardinality: 8}}
			for _, c := range cols {
				if c.typ != memory.TypeString && c.typ != memory.TypeBool {
					q.Aggregates = append(q.Aggregates, expr.Sum(c.name, c.name+"_sum"), expr.Min(c.name, c.name+"_min"), expr.Max(c.name, c.name+"_max"), expr.Avg(c.name, c.name+"_avg"))
				}
				q.Aggregates = append(q.Aggregates, expr.CountColumn(c.name, c.name+"_count"))
			}
			q.Aggregates = append(q.Aggregates, expr.Count("rows"))
			opts := streamTestOptions()
			opts.Workers = 3
			opts.MemoryLimitBytes = 1 << 20
			if strategy == "global" {
				q.GroupBy = nil
			} else if strategy == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 4 << 20}
			} else {
				q.Hints.ForceStrategy = strategy
			}
			if strategy == "sorted" {
				src.WithOrdering(source.OrderingColumn{Name: "key"})
			}
			e := New(opts)
			defer e.Close()
			p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Execute(context.Background(), ExecuteOptions{Materialize: true}); err == nil || !strings.Contains(err.Error(), "ExecuteTyped") {
				t.Fatalf("legacy native materialize error: %v", err)
			}
			for repeat := 0; repeat < 2; repeat++ {
				budget, err := e.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				wantGroups := 3
				if strategy == "global" {
					wantGroups = 1
				}
				if len(result.Rows) != wantGroups || result.Summary.GroupCount != wantGroups || result.Summary.Rows != nil || result.Summary.SelectedRows != 6 {
					t.Fatalf("summary %+v", result.Summary)
				}
				for _, row := range result.Rows {
					lo, hi := 0, 6
					if strategy != "global" {
						key := int(row.Keys[0].Value.Bits)
						lo, hi = key*2, key*2+2
					}
					for i, a := range q.Aggregates {
						got := row.Aggregates[i].Value
						if a.Kind == expr.AggCountRows {
							if n, ok := got.AsUint64(); !ok || n != uint64(hi-lo) {
								t.Fatalf("rows %+v", got)
							}
							continue
						}
						var col nativeTestColumn
						for _, c := range cols {
							if c.name == a.Column {
								col = c
								break
							}
						}
						layout := aggregate.Layout{CountValues: true}
						if col.typ != memory.TypeString && col.typ != memory.TypeBool {
							layout.Sum = true
							layout.Min = true
							layout.Max = true
							layout.Avg = true
						}
						state, err := aggregate.NewNumericVector(1, col.typ, layout)
						if err != nil {
							t.Fatal(err)
						}
						for r := lo; r < hi; r++ {
							state.AddBits(0, col.bits[r], memory.IsSet(col.valid, r))
						}
						want, valid := state.Result(0).Value(aggregateStateKind(a.Kind))
						typ, _ := aggregate.NumericAggregateType(col.typ, aggregateStateKind(a.Kind))
						equal := got.Bits == want
						if got.Type == memory.TypeFloat32 {
							equal = equal || (math.IsNaN(float64(math.Float32frombits(uint32(got.Bits)))) && math.IsNaN(float64(math.Float32frombits(uint32(want)))))
						}
						if got.Type == memory.TypeFloat64 {
							equal = equal || (math.IsNaN(math.Float64frombits(got.Bits)) && math.IsNaN(math.Float64frombits(want)))
						}
						if got.Type != typ || got.Valid != valid || (valid && !equal) {
							t.Fatalf("%s range %d:%d got %+v want type=%v bits=%x valid=%v", a.Alias, lo, hi, got, typ, want, valid)
						}
					}
				}
				if budget.Used() != 0 {
					t.Fatalf("leaked %d", budget.Used())
				}
				budget.Close()
			}
			summary, err := p.ExecuteTyped(context.Background(), ExecuteOptions{})
			if err != nil || len(summary.Rows) != 0 {
				t.Fatalf("summary-only err=%v", err)
			}
		})
	}
}

func TestTypedLegacyAndOwnershipErrors(t *testing.T) {
	src, _ := multiAggregateFixture(t, 64)
	opts := streamTestOptions()
	e := New(opts)
	q := multiAggregateQuery()
	p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := p.Execute(context.Background(), ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != len(legacy.Rows) {
		t.Fatal("legacy typed cardinality")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ExecuteTyped(ctx, ExecuteOptions{Materialize: true}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.Close()
	e.Close()
	if len(got.Rows) == 0 || !got.Rows[0].Aggregates[0].Value.Valid {
		t.Fatal("result invalidated by close")
	}
	if _, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true}); err == nil {
		t.Fatal("closed query executed")
	}
}

func TestNativeTypedBudgetAndEmpty(t *testing.T) {
	src, _ := nativeAggregateFixture(t)
	q := expr.Query{Aggregates: []expr.Aggregate{expr.Sum("u64", "sum"), expr.CountColumn("s", "count")}}
	for _, limit := range []int64{64, 1 << 20} {
		e := New(Options{MemoryLimitBytes: limit})
		p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		budget, err := e.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: true})
		if limit == 64 {
			if err == nil || len(result.Rows) != 0 {
				t.Fatal("budget failure exposed partial result")
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if budget.Used() != 0 {
			t.Fatalf("budget leak %d", budget.Used())
		}
		budget.Close()
		p.Close()
		e.Close()
	}
	empty, err := inmemory.New(src.Schema())
	if err != nil {
		t.Fatal(err)
	}
	e := New(Options{MemoryLimitBytes: 1 << 20, ExecutionPolicy: ExecutionBounded})
	defer e.Close()
	result, err := e.ExecuteTyped(context.Background(), empty, q, ExecuteOptions{Materialize: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Aggregates[0].Value.Valid || result.Rows[0].Aggregates[1].Value.Bits != 0 || !result.Rows[0].Aggregates[1].Value.Valid {
		t.Fatalf("empty result %+v", result)
	}
}

func TestNativeSpillStreamPrecision(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "key", Type: memory.TypeInt32}, memory.Field{Name: "v", Type: memory.TypeUint64})
	batches := make([]*memory.Batch, 0, 64)
	for part := 0; part < 64; part++ {
		b := memory.NewBatch()
		b.Rows = 64
		k, v := make([]int32, 64), make([]uint64, 64)
		for row := range k {
			k[row] = int32((part*64 + row) % 1024)
			v[row] = 1<<63 + uint64(k[row])
		}
		b.Int32s["key"] = &memory.Int32Vector{Values: k}
		b.Uint64s["v"] = &memory.Uint64Vector{Values: v}
		batches = append(batches, b)
	}
	src, err := inmemory.New(schema, batches...)
	if err != nil {
		t.Fatal(err)
	}
	opts := streamTestOptions()
	opts.MemoryLimitBytes = 128 << 10
	opts.ExecutionPolicy = ExecutionBounded
	opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
	e := New(opts)
	defer e.Close()
	p, err := e.Prepare(context.Background(), src, expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("v", "sum"), expr.Min("v", "min"), expr.Count("n")}}, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewSpillableAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := make([]bool, 1024)
	count := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for row := 0; row < out.Rows; row++ {
			key := int(out.Int32s["key"].Values[row])
			if seen[key] {
				t.Fatal("duplicate")
			}
			seen[key] = true
			count++
			if out.Uint64s["sum"].Values[row] != uint64(key)*4 || out.Uint64s["min"].Values[row] != (1<<63+uint64(key)) || out.Uint64s["n"].Values[row] != 4 {
				t.Fatalf("key %d lost native precision", key)
			}
		}
	}
	if count != 1024 || budget.Used() != 0 {
		t.Fatalf("count %d budget %d", count, budget.Used())
	}
	if stats := stream.(*spillableAggregateStream).Stats(); stats.SpillRuns < 2 || stats.SpillBytes == 0 {
		t.Fatalf("stats %+v", stats)
	}
}
