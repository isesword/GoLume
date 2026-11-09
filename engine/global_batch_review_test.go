package engine

import (
	"context"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
	"math"
	"testing"
)

// Unlike the broad native aggregate fixture, these queries contain only SUM
// and COUNT, so every width must execute through the eligible global consumer.
// Expected sums are fixed independently of the numeric aggregation helpers.
func TestGlobalBatchReviewAllWidthsAndChecksum(t *testing.T) {
	negative := func(v int64) uint64 { return uint64(v) }
	cols := []struct {
		name   string
		typ    memory.DataType
		bits   []uint64
		output memory.DataType
		sum    uint64
	}{
		{"i8", memory.TypeInt8, []uint64{negative(-128), 80, 2, negative(-3), 99, 70, negative(-6), 4}, memory.TypeInt64, negative(-131)},
		{"i16", memory.TypeInt16, []uint64{negative(-32768), 80, 2, negative(-3), 99, 70, negative(-6), 4}, memory.TypeInt64, negative(-32771)},
		{"i32", memory.TypeInt32, []uint64{math.MaxInt32, 80, 1, 1, 99, 70, 1, 1}, memory.TypeInt32, negative(math.MinInt32 + 3)},
		{"i64", memory.TypeInt64, []uint64{math.MaxInt64, 80, 1, 1, 99, 70, 1, 1}, memory.TypeInt64, negative(math.MinInt64 + 3)},
		{"u8", memory.TypeUint8, []uint64{255, 80, 2, 3, 99, 70, 4, 5}, memory.TypeInt64, 269},
		{"u16", memory.TypeUint16, []uint64{65535, 80, 2, 3, 99, 70, 4, 5}, memory.TypeInt64, 65549},
		{"u32", memory.TypeUint32, []uint64{math.MaxUint32, 80, 1, 1, 99, 70, 1, 1}, memory.TypeUint32, 3},
		{"u64", memory.TypeUint64, []uint64{math.MaxUint64, 80, 1, 1, 99, 70, 1, 1}, memory.TypeUint64, 3},
		{"f32", memory.TypeFloat32, []uint64{uint64(math.Float32bits(1 << 24)), uint64(math.Float32bits(80)), uint64(math.Float32bits(1)), uint64(math.Float32bits(-(1 << 24))), uint64(math.Float32bits(float32(math.NaN()))), uint64(math.Float32bits(70)), uint64(math.Float32bits(0.5)), uint64(math.Float32bits(0.25))}, memory.TypeFloat32, uint64(math.Float32bits(0.75))},
		{"f64", memory.TypeFloat64, []uint64{math.Float64bits(1e16), math.Float64bits(80), math.Float64bits(1), math.Float64bits(-1e16), math.Float64bits(math.NaN()), math.Float64bits(70), math.Float64bits(0.5), math.Float64bits(0.25)}, memory.TypeFloat64, math.Float64bits(0.75)},
	}
	fields := []memory.Field{{Name: "pred", Type: memory.TypeInt32}}
	for _, col := range cols {
		fields = append(fields, memory.Field{Name: col.name, Type: col.typ, Nullable: true})
	}
	// Source partitions deliberately divide the floating cancellation sequence.
	var batches []*memory.Batch
	for _, span := range [][2]int{{0, 3}, {3, 8}} {
		b := memory.NewBatch()
		b.Rows = span[1] - span[0]
		pred := []int32{1, 0, 1, 1, 1, 0, 1, 1}
		b.Int32s["pred"] = &memory.Int32Vector{Values: pred[span[0]:span[1]]}
		validity := make(memory.Bitmap, memory.WordsFor(b.Rows))
		for row := span[0]; row < span[1]; row++ {
			if row != 4 {
				memory.Set(validity, row-span[0])
			}
		}
		for _, col := range cols {
			fillNativeTestColumn(b, nativeTestColumn{name: col.name, typ: col.typ, bits: col.bits[span[0]:span[1]], valid: validity})
		}
		batches = append(batches, b)
	}
	src, err := inmemory.New(memory.NewSchema(fields...), batches...)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, int64First := range []bool{false, true} {
			name := "narrow-first"
			if int64First {
				name = "int64-first"
			}
			t.Run(mode.String()+"/"+name, func(t *testing.T) {
				order := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
				if int64First {
					order[0], order[3] = order[3], order[0]
				}
				q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(0)), Aggregates: []expr.Aggregate{expr.Count("rows")}}
				for _, index := range order {
					col := cols[index]
					q.Aggregates = append(q.Aggregates, expr.Sum(col.name, col.name+"_sum"), expr.CountColumn(col.name, col.name+"_count"))
				}
				opts := DefaultOptions()
				opts.MemoryLimitBytes, opts.KernelMode = 1<<20, mode
				opts.Workers, opts.Pipeline.Workers = 3, 3
				e := New(opts)
				defer e.Close()
				p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				if !p.globalAggregateBindingsEligible() || len(p.cq.MultiAggregate.Columns) != len(cols) {
					t.Fatal("query missed the eligible ten-lane global path")
				}
				wantChecksum := int64(0)
				if int64First {
					wantChecksum = math.MinInt64 + 3
				}
				for _, materialize := range []bool{true, false, true} {
					budget, err := e.BeginQueryMemory()
					if err != nil {
						t.Fatal(err)
					}
					result, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), budget), ExecuteOptions{Materialize: materialize})
					used := budget.Used()
					closeErr := budget.Close()
					if err != nil || closeErr != nil || used != 0 {
						t.Fatalf("execute=%v close=%v retained=%d", err, closeErr, used)
					}
					if result.Summary.SelectedRows != 6 || result.Summary.GroupCount != 1 || result.Summary.Checksum != wantChecksum {
						t.Fatalf("summary=%+v want checksum=%d", result.Summary, wantChecksum)
					}
					if !materialize {
						if len(result.Rows) != 0 {
							t.Fatal("summary-only execution materialized rows")
						}
						continue
					}
					if len(result.Rows) != 1 || len(result.Rows[0].Keys) != 0 || len(result.Rows[0].Aggregates) != 1+2*len(cols) {
						t.Fatalf("unexpected global result: %+v", result)
					}
					values := result.Rows[0].Aggregates
					if got := values[0].Value; got != (TypedValue{Type: memory.TypeUint64, Valid: true, Bits: 6}) {
						t.Fatalf("COUNT(*)=%+v", got)
					}
					for lane, index := range order {
						col := cols[index]
						if got := values[1+lane*2].Value; got != (TypedValue{Type: col.output, Valid: true, Bits: col.sum}) {
							t.Fatalf("%s SUM=%+v want type=%s bits=%x", col.name, got, col.output, col.sum)
						}
						if got := values[2+lane*2].Value; got != (TypedValue{Type: memory.TypeUint64, Valid: true, Bits: 5}) {
							t.Fatalf("%s COUNT=%+v", col.name, got)
						}
					}
				}
			})
		}
	}
}

// COUNT(column) depends only on parent validity. Payload-free nested and opaque
// headers prove that the global consumer never tries to read child values or
// a numeric vector merely because a count-only column has a numeric schema.
func TestGlobalBatchReviewCountOnlyParentValidity(t *testing.T) {
	names := []string{"plain", "dict", "nested", "list", "opaque", "numeric", "date"}
	fields := []memory.Field{
		{Name: "pred", Type: memory.TypeInt32},
		{Name: "plain", Type: memory.TypeString, Nullable: true},
		{Name: "dict", Type: memory.TypeString, Nullable: true, Encoding: memory.EncodingDictionary},
		// The source schema is valid, but the replacement stream below omits
		// this child payload to prove COUNT reads only the parent validity.
		memory.StructField("nested", true, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}),
		memory.ListStringField("list", true, true),
		{Name: "opaque", Type: memory.TypeArrow, Nullable: true},
		{Name: "numeric", Type: memory.TypeFloat64, Nullable: true},
		{Name: "date", Type: memory.TypeDate32, Nullable: true},
	}
	q := expr.Query{Filter: expr.GT(expr.Col("pred"), expr.I64(0)), Aggregates: []expr.Aggregate{expr.Count("rows")}}
	for _, name := range names {
		q.Aggregates = append(q.Aggregates, expr.CountColumn(name, name+"_count"))
	}
	s, budget := globalBindingTestStream(t, memory.NewSchema(fields...), q)
	if !s.query.globalAggregateBindingsEligible() || len(s.globalBindings) != len(names) {
		t.Fatal("count-only query missed the global path")
	}
	if err := s.input.Close(); err != nil {
		t.Fatal(err)
	}
	batchIndex := 0
	input := &globalBindingTestInput{next: func(_ context.Context, b *memory.Batch) (bool, error) {
		assertGlobalBindingsCleared(t, s)
		b.ClearColumns()
		if batchIndex == 2 {
			return false, nil
		}
		b.Rows = 4
		validity := func(mask uint64) memory.Bitmap {
			if batchIndex == 1 {
				return nil
			}
			return memory.Bitmap{mask}
		}
		b.Int32s["pred"] = &memory.Int32Vector{Values: []int32{1, 0, 1, 1}}
		b.Strings["plain"] = &memory.StringViewVector{Offsets: make([]uint32, 5), Validity: validity(0b0101)}
		b.DictStrings["dict"] = &memory.DictionaryStringVector{Indices: make([]uint32, 4), Validity: validity(0b1010)}
		b.Structs["nested"] = &memory.StructVector{Rows: 4, Validity: validity(0b1100)}
		b.StringLists["list"] = &memory.StringListVector{Offsets: make([]uint32, 5), Validity: validity(0b1001)}
		b.Opaque["opaque"] = &memory.OpaqueVector{Rows: 4, Validity: validity(0b0110)}
		b.Opaque["numeric"] = &memory.OpaqueVector{Rows: 4, Validity: validity(0b0010)}
		b.Int32s["date"] = &memory.Int32Vector{Values: []int32{10, 20, 30, 40}, Validity: validity(0b1110)}
		batchIndex++
		return true, nil
	}}
	s.input = input
	out := memory.NewBatch()
	if ok, err := s.Next(context.Background(), out); !ok || err != nil {
		t.Fatalf("next=%v err=%v", ok, err)
	}
	if out.Rows != 1 || out.Uint64s["rows"].Values[0] != 6 {
		t.Fatalf("rows=%d COUNT(*)=%v", out.Rows, out.Uint64s["rows"])
	}
	for lane, want := range []uint64{5, 4, 5, 5, 4, 3, 5} {
		if got := out.Uint64s[names[lane]+"_count"]; !got.IsValid(0) || got.Values[0] != want {
			t.Fatalf("%s count=%v want=%d", names[lane], got, want)
		}
	}
	if stats := s.Stats(); stats.SelectedRows != 6 || stats.Groups != 1 || stats.Checksum != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	assertGlobalBindingsCleared(t, s)
	if input.closed != 1 || budget.Used() == 0 {
		t.Fatalf("closed=%d output charge=%d", input.closed, budget.Used())
	}
	if ok, err := s.Next(context.Background(), memory.NewBatch()); ok || err != nil || out.Rows != 0 || budget.Used() != 0 || input.closed != 1 {
		t.Fatalf("EOF=%v err=%v rows=%d retained=%d closed=%d", ok, err, out.Rows, budget.Used(), input.closed)
	}
}
