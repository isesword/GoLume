package physical

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"testing"

	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// Count opens without poisoning shared backend vectors or adding uncharged copies.
type stddevResourceSource struct {
	source.Source
	opens, closes int
}
type stddevResourceStream struct {
	source.BatchStream
	owner  *stddevResourceSource
	closed bool
}

func (s *stddevResourceSource) NewBatchStream(ctx context.Context, o source.ReadOptions) (source.BatchStream, error) {
	s.opens++
	if s.opens != 1 {
		return nil, fmt.Errorf("stddev source reopened")
	}
	ch, e := s.Source.NewBatchStream(ctx, o)
	if e != nil {
		return nil, e
	}
	return &stddevResourceStream{BatchStream: ch, owner: s}, nil
}
func (s *stddevResourceStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.owner.closes++
	return s.BatchStream.Close()
}
func stddevResourceValue(id int) (int64, bool) { return int64(id%17 - 8), id%13 != 0 }
func stddevResourceFixture(t *testing.T, n int, reverse bool) *stddevResourceSource {
	t.Helper()
	b := memory.NewBatch()
	b.Reset(n)
	for _, name := range []string{"g", "k", "id", "v"} {
		b.Int64s[name] = &memory.Int64Vector{Values: make([]int64, n)}
	}
	b.Int64s["v"].Validity = make(memory.Bitmap, memory.WordsFor(n))
	for row := 0; row < n; row++ {
		id := row
		if reverse {
			id = n - 1 - row
		}
		v, valid := stddevResourceValue(id)
		b.Int64s["id"].Values[row] = int64(id)
		b.Int64s["k"].Values[row] = int64(id / 3)
		b.Int64s["v"].Values[row] = v
		if valid {
			memory.Set(b.Int64s["v"].Validity, row)
		}
	}
	sc := memory.NewSchema(memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "k", Type: memory.TypeInt64}, memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeInt64, Nullable: true})
	src, e := inmemory.New(sc, b)
	if e != nil {
		t.Fatal(e)
	}
	return &stddevResourceSource{Source: src}
}

// Exact integer raw sums over this deliberately small bounded fixture are an
// independent oracle, not the production online/removable moment recurrence.
// Roots are rounded directly from the exact rational, never from float variance.
// This test-owned cache is independent expected-data storage, not query memory.
func stddevResourceOracle(id, n int, prefix bool, ddof int64, cache map[[4]int64]float64) (float64, bool) {
	lo, hi := id-7, id+3
	if prefix {
		lo = 0
		hi = id
	}
	if lo < 0 {
		lo = 0
	}
	if hi >= n {
		hi = n - 1
	}
	var count, sum, squares int64
	for j := lo; j <= hi; j++ {
		v, valid := stddevResourceValue(j)
		if valid {
			count++
			sum += v
			squares += v * v
		}
	}
	if count <= ddof {
		return 0, false
	}
	key := [4]int64{count, sum, squares, ddof}
	if root, ok := cache[key]; ok {
		return root, true
	}
	exact := new(big.Rat).SetFrac(big.NewInt(count*squares-sum*sum), big.NewInt(count*(count-ddof)))
	root := stddevExactRootOracle(exact)
	cache[key] = root
	return root, true
}
func stddevResourceRun(t *testing.T, n, batch int, mode WindowExecutionMode, disk, prefix bool) {
	t.Helper()
	ctx, q := percentRankQuery(t, 4<<20, disk)
	// Register early so failure logs retain the actual peak/admission evidence.
	t.Cleanup(func() {
		t.Logf("resource snapshot mode=%d rows=%d disk=%v prefix=%v: %+v", mode, n, disk, prefix, q.Snapshot())
	})
	reverse := mode != WindowExecutionOrdered
	src := stddevResourceFixture(t, n, reverse)
	frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: 7}, End: WindowBound{Kind: WindowFollowing, Offset: 3}}
	if prefix {
		frame = WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}
	}
	ex := []WindowExpression{{Function: WindowStdDevPop, Column: "v", Frame: frame, Alias: "sp"}, {Function: WindowStdDevSamp, Column: "v", Frame: frame, Alias: "ss"}, {Function: WindowRowNumber, Alias: "rn"}}
	fields := []memory.Field{{Name: "sp", Type: memory.TypeFloat64, Nullable: true}, {Name: "ss", Type: memory.TypeFloat64, Nullable: true}, {Name: "rn", Type: memory.TypeInt64}}
	win, e := NewWindowSourceWithOptions(src, []string{"g"}, []SortKey{{Column: "id"}}, ex, fields, q, WindowSourceOptions{Mode: mode})
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range fields {
		got, ok := win.Schema().Field(f.Name)
		if !ok || got.Type != f.Type || got.Nullable != f.Nullable || got.Encoding != memory.EncodingPlain {
			t.Fatal("output schema", f, got)
		}
	}
	st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: batch})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := st.Close(); e != nil {
			t.Error(e)
		}
	})
	out := memory.NewBatch()
	seen := 0
	oracleCache := make(map[[4]int64]float64)
	for {
		ok, e := st.Next(ctx, out)
		if e != nil {
			// Snapshot immediately after the failed Next, before this test's Close.
			// An operator may already have performed its own terminal cleanup.
			t.Logf("failed Next before test cleanup: seen=%d ok=%v output_rows=%d source_opens=%d source_closes=%d query=%+v", seen, ok, out.Rows, src.opens, src.closes, q.Snapshot())
			if d, hasDiagnostics := st.(source.BatchStreamDiagnostics); hasDiagnostics {
				t.Logf("failed Next stage diagnostics: %v", d.StreamDiagnostics())
			} else {
				t.Log("failed Next stage diagnostics unavailable")
			}
			t.Fatal(e)
		}
		if !ok {
			break
		}
		for row := 0; row < out.Rows; row++ {
			id := seen + row
			if reverse {
				id = n - 1 - id
			}
			v, valid := stddevResourceValue(id)
			for name, want := range map[string]int64{"g": 0, "k": int64(id / 3), "id": int64(id), "v": v} {
				got := out.Int64s[name]
				wv := true
				if name == "v" {
					wv = valid
				}
				if got == nil || got.IsValid(row) != wv || (wv && got.Values[row] != want) {
					t.Fatalf("input %s id=%d", name, id)
				}
			}
			rn := out.Int64s["rn"]
			if rn == nil || !rn.IsValid(row) || rn.Values[row] != int64(id+1) {
				t.Fatalf("row number id=%d", id)
			}
			for ddof, name := range []string{"sp", "ss"} {
				want, wv := stddevResourceOracle(id, n, prefix, int64(ddof), oracleCache)
				got := out.Float64s[name]
				if got == nil || got.IsValid(row) != wv {
					t.Fatalf("validity %s id=%d want=%v", name, id, wv)
				}
				if wv && (math.Float64bits(got.Values[row]) != math.Float64bits(want)) {
					t.Fatalf("%s id=%d got=%.17g want=%.17g", name, id, got.Values[row], want)
				}
			}
		}
		seen += out.Rows
	}
	if seen != n {
		t.Fatalf("rows %d want %d", seen, n)
	}
	if d, ok := st.(source.BatchStreamDiagnostics); ok {
		diag := d.StreamDiagnostics()
		t.Logf("actual stage diagnostics: %v", diag)
		if n == 262144 && mode == WindowExecutionExternalSort {
			for _, key := range []string{"front_sort_spill_runs", "front_sort_spill_bytes", "restore_sort_spill_runs", "restore_sort_spill_bytes"} {
				v, e := strconv.ParseInt(diag[key], 10, 64)
				if e != nil || v <= 0 {
					t.Fatalf("missing actual sort spill %s: %v", key, diag)
				}
			}
		}
	} else if n == 262144 && mode == WindowExecutionExternalSort {
		t.Fatal("missing external stage diagnostics")
	}
	if e = st.Close(); e != nil {
		t.Fatal(e)
	}
	if src.opens != 1 || src.closes != 1 {
		t.Fatal("source lifecycle", src.opens, src.closes)
	}
	snap := q.Snapshot()
	if snap.PeakBytes <= 0 || snap.PeakBytes > 4<<20 {
		t.Fatal("invalid peak", snap)
	}
	if q.Used() != 0 {
		t.Fatal("operator leak before query Close", q.Snapshot())
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	if q.Used() != 0 {
		t.Fatal("query leak", q.Snapshot())
	}
}
func TestWindowStdDevResourceSmallMatrix(t *testing.T) {
	for _, n := range []int{1, 2, 37} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			for _, prefix := range []bool{false, true} {
				t.Run(fmt.Sprintf("n%d/mode%d/prefix%v", n, mode, prefix), func(t *testing.T) { stddevResourceRun(t, n, 5, mode, true, prefix) })
			}
		}
	}
}

// Frozen large fixture: four Int64 inputs, 262144 rows, batch1024, hard4MiB,
// disk256MiB when enabled. Ordered finite frames must also succeed without disk.
func TestWindowStdDevFixedStreamingResources(t *testing.T) {
	for _, tc := range []struct {
		mode WindowExecutionMode
		disk bool
	}{{WindowExecutionOrdered, false}, {WindowExecutionOrdered, true}, {WindowExecutionExternalSort, true}} {
		t.Run(fmt.Sprintf("mode%d/disk%v", tc.mode, tc.disk), func(t *testing.T) { stddevResourceRun(t, 262144, 1024, tc.mode, tc.disk, false) })
	}
}
