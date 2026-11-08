package engine

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// This independent engine oracle complements the physical filter/project row
// oracle. It verifies the fused 64-row masks as well as bitmap and global paths.
// All APIs used here exist on the preceding tree for identical-harness timing.
type narrowEngineCase struct {
	typ               memory.DataType
	op                kernel.CompareOp
	literal           int64
	rows, batchRows   int
	pattern, strategy string
}

func (c narrowEngineCase) name() string {
	return fmt.Sprintf("type=%s/op=%s/lit=%d/rows=%d/batch=%d/%s/path=%s", c.typ, c.op, c.literal, c.rows, c.batchRows, c.pattern, c.strategy)
}
func narrowEngineCompare(a, b int64, op kernel.CompareOp) bool {
	switch op {
	case kernel.CompareGT:
		return a > b
	case kernel.CompareGE:
		return a >= b
	case kernel.CompareLT:
		return a < b
	case kernel.CompareLE:
		return a <= b
	case kernel.CompareEQ:
		return a == b
	}
	panic("bad op")
}
func narrowEngineExpr(c narrowEngineCase) expr.Expr {
	l, r := expr.Col("pred"), expr.I64(c.literal)
	switch c.op {
	case kernel.CompareGT:
		return expr.GT(l, r)
	case kernel.CompareGE:
		return expr.GE(l, r)
	case kernel.CompareLT:
		return expr.LT(l, r)
	case kernel.CompareLE:
		return expr.LE(l, r)
	case kernel.CompareEQ:
		return expr.EQ(l, r)
	}
	panic("bad op")
}
func narrowEngineValue(typ memory.DataType, row int, pattern string) int64 {
	if pattern == "dense" || pattern == "selective" {
		return int64((uint32(row)*1664525 + 1013904223) % 100)
	}
	var edge []int64
	switch typ {
	case memory.TypeInt16:
		edge = []int64{-32768, -32767, -1, 0, 1, 32766, 32767}
	case memory.TypeUint8:
		edge = []int64{0, 1, 127, 128, 129, 254, 255}
	case memory.TypeUint16:
		edge = []int64{0, 1, 32767, 32768, 32769, 65534, 65535}
	}
	if row%3 != 2 {
		return edge[(row/3+row)%len(edge)]
	}
	n := uint32(row)*1664525 + 1013904223
	switch typ {
	case memory.TypeInt16:
		return int64(int16(n))
	case memory.TypeUint8:
		return int64(uint8(n))
	case memory.TypeUint16:
		return int64(uint16(n))
	}
	panic("bad type")
}

type narrowEngineExpected struct {
	sum             int64
	count, selected uint64
}
type narrowEngineFixture struct {
	src      *inmemory.Source
	q        expr.Query
	want     map[int32]narrowEngineExpected
	selected int
}

func narrowEngineMake(tb testing.TB, c narrowEngineCase) narrowEngineFixture {
	tb.Helper()
	f := narrowEngineFixture{want: make(map[int32]narrowEngineExpected)}
	f.q = expr.Query{Filter: narrowEngineExpr(c), Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.CountColumn("value", "count"), expr.Count("rows")}}
	if c.strategy == "global" {
		f.want[0] = narrowEngineExpected{}
	} else {
		f.q.GroupBy = []string{"key"}
		f.q.Hints = expr.Hints{ForceStrategy: c.strategy, Cardinality: 13}
	}
	var batches []*memory.Batch
	for start := 0; start < c.rows; start += c.batchRows {
		n := min(c.batchRows, c.rows-start)
		b := memory.NewBatch()
		b.Rows = n
		values := make([]int64, n)
		keys := make([]int32, n)
		p16, pu8, pu16 := make([]int16, n), make([]uint8, n), make([]uint16, n)
		var pv memory.Bitmap
		if c.pattern == "nullable" || c.pattern == "allnull" {
			pv = make(memory.Bitmap, memory.WordsFor(n))
		}
		vv := make(memory.Bitmap, memory.WordsFor(n))
		for j := 0; j < n; j++ {
			row := start + j
			p := narrowEngineValue(c.typ, row, c.pattern)
			p16[j], pu8[j], pu16[j] = int16(p), uint8(p), uint16(p)
			values[j] = int64(row)*17 - 91
			keys[j] = int32(row % 13)
			valid := c.pattern != "allnull" && (c.pattern != "nullable" || row%11 != 0)
			if pv != nil && valid {
				memory.Set(pv, j)
			}
			if row%7 != 0 {
				memory.Set(vv, j)
			}
			if valid && narrowEngineCompare(p, c.literal, c.op) {
				key := keys[j]
				if c.strategy == "global" {
					key = 0
				}
				v := f.want[key]
				v.selected++
				if row%7 != 0 {
					v.sum += values[j]
					v.count++
				}
				f.want[key] = v
				f.selected++
			}
		}
		b.Int64s["value"] = &memory.Int64Vector{Values: values, Validity: vv}
		b.Int32s["key"] = &memory.Int32Vector{Values: keys}
		switch c.typ {
		case memory.TypeInt16:
			b.Int16s["pred"] = &memory.Int16Vector{Values: p16, Validity: pv}
		case memory.TypeUint8:
			b.Uint8s["pred"] = &memory.Uint8Vector{Values: pu8, Validity: pv}
		case memory.TypeUint16:
			b.Uint16s["pred"] = &memory.Uint16Vector{Values: pu16, Validity: pv}
		}
		batches = append(batches, b)
	}
	var err error
	f.src, err = inmemory.New(memory.NewSchema(memory.Field{Name: "pred", Type: c.typ, Nullable: c.pattern == "nullable" || c.pattern == "allnull"}, memory.Field{Name: "value", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "key", Type: memory.TypeInt32}), batches...)
	if err != nil {
		tb.Fatal(err)
	}
	return f
}
func narrowEnginePrepare(tb testing.TB, c narrowEngineCase, mode kernel.Mode, f narrowEngineFixture) (*Engine, *PreparedQuery) {
	tb.Helper()
	o := DefaultOptions()
	o.Workers = 1
	o.Pipeline.Workers = 1
	o.KernelMode = mode
	o.PreparedCacheBytes = 0
	o.MemoryLimitBytes = 64 << 20
	o.Pipeline.MaxMorselRows = 65536
	o.Pipeline.TargetMorselBytes = 4 << 20
	o.Pipeline.MaxMorselBytes = 8 << 20
	e := New(o)
	p, err := e.Prepare(context.Background(), f.src, f.q, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		e.Close()
		tb.Fatal(err)
	}
	if p.cq.SimpleComparison == nil {
		tb.Fatal("missed simple predicate")
	}
	return e, p
}
func narrowEngineCheck(tb testing.TB, c narrowEngineCase, f narrowEngineFixture, r TypedResult) {
	tb.Helper()
	if len(r.Rows) != len(f.want) || r.Summary.SelectedRows != f.selected {
		tb.Fatalf("rows=%d want=%d selected=%d want=%d", len(r.Rows), len(f.want), r.Summary.SelectedRows, f.selected)
	}
	seen := make(map[int32]bool)
	for _, row := range r.Rows {
		key := int32(0)
		if c.strategy != "global" {
			if len(row.Keys) != 1 || !row.Keys[0].Value.Valid || row.Keys[0].Value.Type != memory.TypeInt32 {
				tb.Fatalf("wrong key %+v", row.Keys)
			}
			key = int32(row.Keys[0].Value.Bits)
		}
		want, ok := f.want[key]
		if !ok || seen[key] {
			tb.Fatalf("unexpected/duplicate key %d", key)
		}
		seen[key] = true
		if len(row.Aggregates) != 3 {
			tb.Fatalf("wrong aggregates %+v", row.Aggregates)
		}
		expected := []TypedValue{{Type: memory.TypeInt64, Valid: want.count > 0, Bits: uint64(want.sum)}, {Type: memory.TypeUint64, Valid: true, Bits: want.count}, {Type: memory.TypeUint64, Valid: true, Bits: want.selected}}
		for j, v := range row.Aggregates {
			w := expected[j]
			if v.Value.Type != w.Type || v.Value.Valid != w.Valid || (w.Valid && v.Value.Bits != w.Bits) {
				tb.Fatalf("key=%d aggregate=%s got=%+v want=%+v", key, v.Alias, v.Value, w)
			}
		}
	}
}
func narrowEngineVerify(tb testing.TB, c narrowEngineCase, f narrowEngineFixture, e *Engine, p *PreparedQuery) int64 {
	tb.Helper()
	q, err := e.BeginQueryMemory()
	if err != nil {
		tb.Fatal(err)
	}
	defer q.Close()
	r, err := p.ExecuteTyped(memory.WithQueryBudget(context.Background(), q), ExecuteOptions{Materialize: true})
	if err != nil {
		tb.Fatal(err)
	}
	narrowEngineCheck(tb, c, f, r)
	snap := q.Snapshot()
	if snap.UsedBytes != 0 || e.Memory().Used() != 0 {
		tb.Fatalf("retained query=%d engine=%d", snap.UsedBytes, e.Memory().Used())
	}
	return snap.PeakBytes
}
func TestNarrowSIMDEngineOracle(t *testing.T) {
	for _, typ := range []memory.DataType{memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		lits := []int64{0, 127, 128, 255, 256, 32767, 32768, 65535, 65536, math.MaxInt64}
		if typ == memory.TypeInt16 {
			lits = append(lits, math.MinInt64, -32769, -32768, -1)
		}
		for _, lit := range lits {
			for op := kernel.CompareGT; op <= kernel.CompareEQ; op++ {
				for _, pattern := range []string{"mixed", "nullable", "allnull"} {
					for _, strategy := range []string{"global", "direct-fused", "bitmap-direct"} {
						c := narrowEngineCase{typ, op, lit, 131, 67, pattern, strategy}
						f := narrowEngineMake(t, c)
						var scalarPeak int64
						for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
							t.Run(c.name()+"/mode="+mode.String(), func(t *testing.T) {
								e, p := narrowEnginePrepare(t, c, mode, f)
								defer e.Close()
								defer p.Close()
								peak := narrowEngineVerify(t, c, f, e, p)
								if mode == kernel.ModeScalar {
									scalarPeak = peak
								} else if peak != scalarPeak {
									t.Fatalf("peak=%d scalar=%d", peak, scalarPeak)
								}
							})
						}
					}
				}
			}
		}
	}
}
func narrowEngineBenchmarkCases() []narrowEngineCase {
	var out []narrowEngineCase
	for _, typ := range []memory.DataType{memory.TypeInt16, memory.TypeUint8, memory.TypeUint16} {
		for _, pattern := range []string{"dense", "selective", "nullable"} {
			lit := int64(9)
			if pattern == "selective" {
				lit = 98
			}
			if pattern == "nullable" {
				lit = 127
			}
			for _, rows := range []int{7, 65543} {
				for _, strategy := range []string{"global", "direct-fused"} {
					out = append(out, narrowEngineCase{typ, kernel.CompareGT, lit, rows, 4096, pattern, strategy})
				}
			}
		}
	}
	return out
}

var narrowEngineSink TypedResult

func BenchmarkNarrowSIMDEngine(b *testing.B) {
	for _, c := range narrowEngineBenchmarkCases() {
		f := narrowEngineMake(b, c)
		for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
			b.Run(c.name()+"/mode="+mode.String(), func(b *testing.B) {
				e, p := narrowEnginePrepare(b, c, mode, f)
				defer e.Close()
				defer p.Close()
				peak := narrowEngineVerify(b, c, f, e, p)
				id, _ := kernel.ComparisonID(c.typ, c.op)
				cap, _ := e.KernelCapabilities().For(id)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r, err := p.ExecuteTyped(context.Background(), ExecuteOptions{Materialize: true})
					if err != nil {
						b.Fatal(err)
					}
					narrowEngineSink = r
				}
				b.StopTimer()
				if e.Memory().Used() != 0 {
					b.Fatalf("retained=%d", e.Memory().Used())
				}
				b.ReportMetric(float64(peak), "peak-charged-B")
				if cap.HardwareSIMD {
					b.ReportMetric(1, "hardware-simd")
				}
				b.ReportMetric(float64(c.rows), "source-rows/op")
			})
		}
	}
}
