package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/physical"
	"gocolumn/pipeline"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Hides replay capability but never changes caller read options or data.
type architectureUnknownSource struct{ source.Source }

func (s architectureUnknownSource) NewBatchStreamWithPartitions(c context.Context, o source.ReadOptions, p []source.Partition) (source.BatchStream, error) {
	return source.OpenBatchStreamWithPartitions(c, s.Source, o, p)
}

// A single-use leaf adapts a live fan-out stream to the physical Source API.
// Opening it cannot create another scan, change budgets or re-batch the input.
type architectureBranchSource struct {
	source.Source
	stream source.BatchStream
	opened atomic.Bool
}

func (s *architectureBranchSource) NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error) {
	if !s.opened.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("branch opened more than once")
	}
	return s.stream, nil
}
func (s *architectureBranchSource) NewBatchStreamWithPartitions(c context.Context, o source.ReadOptions, _ []source.Partition) (source.BatchStream, error) {
	return s.NewBatchStream(c, o)
}

func architectureText(i int) []byte {
	return []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i * 3), byte(i * 5), byte(i * 7), byte(i * 11), byte(i * 13)}
}
func architectureFixture(kind string, n int) (*memory.Batch, memory.Schema) {
	b := memory.NewBatch()
	b.Rows = n
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i)
	}
	b.Int64s["id"] = &memory.Int64Vector{Values: ids}
	fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}}
	switch kind {
	case "scalar":
		x := make([]int64, n)
		for i := range x {
			x[i] = int64(i * 3)
		}
		b.Int64s["x"] = &memory.Int64Vector{Values: x}
		fields = append(fields, memory.Field{Name: "x", Type: memory.TypeInt64})
	case "string":
		o := make([]uint32, n+1)
		d := make([]byte, 0, n*8)
		for i := 0; i < n; i++ {
			o[i] = uint32(len(d))
			d = append(d, architectureText(i)...)
		}
		o[n] = uint32(len(d))
		b.Strings["x"] = &memory.StringViewVector{Offsets: o, Data: d}
		fields = append(fields, memory.Field{Name: "x", Type: memory.TypeString})
	default:
		o := make([]uint32, n+1)
		d := make([]byte, n)
		for i := range o {
			o[i] = uint32(i)
		}
		for i := range d {
			d[i] = byte(i % 251)
		}
		child := memory.NewBatch()
		child.Rows = n
		child.StringLists["l"] = &memory.StringListVector{Offsets: o, Values: &memory.StringViewVector{Offsets: o, Data: d}}
		f := memory.ListStringField("l", false, false)
		b.Structs["s"] = &memory.StructVector{Rows: n, Fields: []memory.Field{f}, Values: child}
		fields = append(fields, memory.StructField("s", false, f))
	}
	return b, memory.NewSchema(fields...)
}
func architectureCheck(kind string, b *memory.Batch, start int) (uint64, error) {
	var sum uint64
	for i := 0; i < b.Rows; i++ {
		row := start + i
		if b.Int64s["id"].Values[i] != int64(row) || b.Int64s["y"].Values[i] != int64(row+1) {
			return 0, fmt.Errorf("order/projection row %d", row)
		}
		sum += uint64(row) + uint64(row+1)
		switch kind {
		case "scalar":
			v := b.Int64s["x"].Values[i]
			if v != int64(row*3) {
				return 0, fmt.Errorf("scalar row %d", row)
			}
			sum += uint64(v)
		case "string":
			v := b.Strings["x"]
			if !v.IsValid(i) || !bytes.Equal(v.ValueBytes(i), architectureText(row)) {
				return 0, fmt.Errorf("string row %d", row)
			}
			for _, v := range v.ValueBytes(i) {
				sum += uint64(v)
			}
		default:
			s := b.Structs["s"]
			if !s.IsValid(i) {
				return 0, fmt.Errorf("struct validity row %d", row)
			}
			v := s.Values.StringLists["l"]
			a, z, ok := v.ValueRange(i)
			if !ok || z-a != 1 || !v.Values.IsValid(a) || !bytes.Equal(v.Values.ValueBytes(a), []byte{byte(row % 251)}) {
				return 0, fmt.Errorf("struct/list row %d", row)
			}
			sum += uint64(byte(row % 251))
		}
	}
	if _, ok := b.Int64s["__tmp"]; ok {
		return 0, fmt.Errorf("DropColumns failed")
	}
	return sum, nil
}
func architectureWant(kind string, n int) uint64 {
	var sum uint64
	for i := 0; i < n; i++ {
		sum += uint64(i) + uint64(i+1)
		switch kind {
		case "scalar":
			sum += uint64(i * 3)
		case "string":
			for _, b := range architectureText(i) {
				sum += uint64(b)
			}
		default:
			sum += uint64(byte(i % 251))
		}
	}
	return sum
}

func TestArchitecturePhysicalFanOut(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		for _, limit := range []int64{512 << 10, 8 << 20} {
			t.Run(fmt.Sprintf("%s/query%d", kind, limit), func(t *testing.T) { architectureRun(t, kind, limit) })
		}
	}
}
func architectureRun(t testing.TB, kind string, limit int64) {
	const rows = 65536
	input, schema := architectureFixture(kind, rows)
	base, err := inmemory.New(schema, input)
	if err != nil {
		t.Fatal(err)
	}
	src := architectureUnknownSource{base}
	engine := memory.NewEngineBudget(1 << 30)
	q, err := engine.BeginQuery(memory.BudgetOptions{HardLimitBytes: limit})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx, cancel := context.WithTimeout(memory.WithQueryBudget(context.Background(), q), 20*time.Second)
	defer cancel()
	ro := source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: rows, BatchPayloadTargetBytes: 64 << 10}
	opts := pipeline.Options{Workers: 1, TargetMorselBytes: 64 << 10, MaxMorselBytes: 256 << 10, MaxInFlightBytes: 1 << 20, MinMorselRows: 256, MaxMorselRows: rows, Ordering: pipeline.OrderingStable}
	parts, err := src.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := pipeline.OpenFanOut(ctx, src, ro, parts, q, opts, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		rows, items int
		sum         uint64
		err         error
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for branch, raw := range streams {
		wg.Add(1)
		go func(branch int, raw source.BatchStream) {
			defer wg.Done()
			defer raw.Close()
			leaf := &architectureBranchSource{Source: base, stream: raw}
			predicate := expr.GT(expr.Col("id"), expr.I64(-1))
			if branch == 1 {
				predicate = expr.LT(expr.Col("id"), expr.I64(rows/2))
			}
			filtered, e := physical.NewFilterSource(leaf, predicate)
			if e != nil {
				results[branch].err = e
				return
			}
			projected, e := physical.NewProjectSource(filtered, []expr.NamedExpr{expr.Named(expr.Add(expr.Col("id"), expr.I64(1)), "y"), expr.Named(expr.Add(expr.Col("id"), expr.I64(2)), "__tmp")}, true)
			if e != nil {
				results[branch].err = e
				return
			}
			dropped, e := physical.NewDropColumnsSource(projected, []string{"__tmp"})
			if e != nil {
				results[branch].err = e
				return
			}
			st, e := source.OpenBatchStream(ctx, dropped, ro)
			if e != nil {
				results[branch].err = e
				return
			}
			defer st.Close()
			out := memory.NewBatch()
			defer out.ClearColumns()
			for {
				ok, e := st.Next(ctx, out)
				if e != nil {
					results[branch].err = e
					return
				}
				if !ok {
					return
				}
				sum, e := architectureCheck(kind, out, results[branch].rows)
				if e != nil {
					results[branch].err = e
					return
				}
				results[branch].sum += sum
				results[branch].rows += out.Rows
				results[branch].items++
				out.ClearColumns()
			}
		}(branch, raw)
	}
	wg.Wait()
	success := true
	hasBudget := false
	for branch, r := range results {
		wantRows := rows
		if branch == 1 {
			wantRows /= 2
		}
		if r.err != nil {
			success = false
			if errors.Is(r.err, memory.ErrBudgetExceeded) {
				hasBudget = true
			} else if !errors.Is(r.err, context.Canceled) {
				t.Errorf("unexpected branch%d error: %v", branch, r.err)
			}
		} else if r.rows != wantRows || r.sum != architectureWant(kind, wantRows) {
			t.Errorf("branch%d wrong result %+v want rows=%d checksum=%d", branch, r, wantRows, architectureWant(kind, wantRows))
		}
	}
	if !success && !hasBudget {
		t.Error("failure without budget error")
	}
	if !success && limit >= 512<<10 {
		t.Error("required-budget physical query failed")
	}
	snap := q.Snapshot()
	if snap.UsedBytes != 0 || engine.Used() != 0 || snap.PeakBytes > limit {
		t.Errorf("budget bound/leak snapshot=%+v engine=%d", snap, engine.Used())
	}
	if _, isTest := t.(*testing.T); isTest {
		t.Logf("success=%t query_limit=%d query_peak=%d branch0_rows=%d items=%d checksum=%d want=%d error=%v branch1_rows=%d items=%d checksum=%d want=%d error=%v", success, limit, snap.PeakBytes, results[0].rows, results[0].items, results[0].sum, architectureWant(kind, rows), results[0].err, results[1].rows, results[1].items, results[1].sum, architectureWant(kind, rows/2), results[1].err)
	}
}

// Measures full query setup, execution,
// teardown and the same full oracle checks, not operators in isolation.
func BenchmarkArchitecturePhysicalFanOut(b *testing.B) {
	for _, kind := range []string{"scalar", "string", "struct_list"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				architectureRun(b, kind, 8<<20)
			}
		})
	}
}
