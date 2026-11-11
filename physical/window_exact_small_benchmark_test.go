package physical

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
)

// This file is intentionally self-contained and uses only APIs present at
// cb36600a68c69dd307c2c438eb8b85ab056c287a. Overlay this identical file on
// that baseline for comparisons, including the adversarial special-value cases.
const smallWindowRows = 4096

type smallWindowCell struct {
	value float64
	valid bool
}
type smallWindowFixture struct {
	src      source.Source
	expected map[string][]smallWindowCell
	reverse  bool
}

// Enumerate each frame independently. No rolling production state, production
// range helper, or intermediate rounded sum participates in the oracle.
func smallWindowOracle(values []float64, valid []bool, groups []int64, row, width int, avg bool) smallWindowCell {
	sum := new(big.Rat)
	count := int64(0)
	pos, neg, nan := false, false, false
	for j := max(0, row-width+1); j <= row; j++ {
		if groups[j] != groups[row] || !valid[j] {
			continue
		}
		count++
		v := values[j]
		switch {
		case math.IsNaN(v):
			nan = true
		case math.IsInf(v, 1):
			pos = true
		case math.IsInf(v, -1):
			neg = true
		default:
			sum.Add(sum, new(big.Rat).SetFloat64(v))
		}
	}
	if count == 0 {
		return smallWindowCell{}
	}
	if nan || pos && neg {
		return smallWindowCell{math.NaN(), true}
	}
	if pos {
		return smallWindowCell{math.Inf(1), true}
	}
	if neg {
		return smallWindowCell{math.Inf(-1), true}
	}
	if avg {
		sum.Quo(sum, new(big.Rat).SetInt64(count))
	}
	v, _ := sum.Float64()
	return smallWindowCell{v, true}
}

func smallWindowInput(tb testing.TB, workload string, width int, names []string, reverse bool) smallWindowFixture {
	tb.Helper()
	values, valid, groups := make([]float64, smallWindowRows), make([]bool, smallWindowRows), make([]int64, smallWindowRows)
	for i := range values {
		values[i], valid[i] = float64(i%127-63), true
		switch workload {
		case "nullable":
			valid[i] = i%19 >= 5
		case "mixed_scale":
			values[i] = math.Ldexp(float64(i%127-63), []int{-8, 0, 8, 16}[i%4])
		case "multipartition":
			groups[i] = int64(i / 37)
			valid[i] = groups[i]%7 != 2 && i%5 != 0
		case "special":
			pattern := []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, 1e16, 1, -1e16, math.NaN(), 2, 3, math.Inf(1), math.Inf(-1), 4, 5, math.SmallestNonzeroFloat64, 0, -math.SmallestNonzeroFloat64}
			values[i] = pattern[i%len(pattern)]
			groups[i] = int64(i / 97)
			valid[i] = i%23 != 0
		}
	}
	expected := make(map[string][]smallWindowCell)
	for _, name := range names {
		expected[name] = make([]smallWindowCell, smallWindowRows)
		for i := range values {
			expected[name][i] = smallWindowOracle(values, valid, groups, i, width, name == "avg")
		}
	}
	batch := memory.NewBatch()
	batch.Rows = smallWindowRows
	ids, physicalGroups, physicalValues := make([]int64, smallWindowRows), make([]int64, smallWindowRows), make([]float64, smallWindowRows)
	bitmap := memory.EnsureBitmap(nil, smallWindowRows)
	for i := range ids {
		id := i
		if reverse {
			id = smallWindowRows - 1 - i
		}
		ids[i], physicalGroups[i], physicalValues[i] = int64(id), groups[id], values[id]
		if valid[id] {
			memory.Set(bitmap, i)
		}
	}
	batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
	batch.Int64s["g"] = &memory.Int64Vector{Values: physicalGroups}
	batch.Float64s["v"] = &memory.Float64Vector{Values: physicalValues, Validity: bitmap}
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "g", Type: memory.TypeInt64}, memory.Field{Name: "v", Type: memory.TypeFloat64, Nullable: true})
	src, err := inmemory.New(schema, batch)
	if err != nil {
		tb.Fatal(err)
	}
	return smallWindowFixture{src, expected, reverse}
}

func smallWindowBenchmark(b *testing.B, mode WindowExecutionMode, workload string, width int, names []string) {
	b.StopTimer()
	external := mode == WindowExecutionExternalSort
	fixture := smallWindowInput(b, workload, width, names, external)
	frame := WindowFrame{Start: WindowBound{Kind: WindowPreceding, Offset: int64(width - 1)}, End: WindowBound{Kind: WindowCurrentRow}}
	var exprs []WindowExpression
	var fields []memory.Field
	got := make(map[string][]smallWindowCell)
	for _, name := range names {
		fn := WindowSum
		if name == "avg" {
			fn = WindowAvg
		}
		exprs = append(exprs, WindowExpression{Function: fn, Column: "v", Alias: name, Frame: frame})
		fields = append(fields, memory.Field{Name: name, Type: memory.TypeFloat64, Nullable: true})
		got[name] = make([]smallWindowCell, smallWindowRows)
	}
	ids := make([]int64, smallWindowRows)
	kernels := kernel.New(kernel.ModeScalar)
	dir := ""
	if external {
		dir = b.TempDir()
	}
	// Compare precomputed exact results without allocating. The identical
	// validation cost is included in both sides of every comparison.
	validate := func() {
		for i, id := range ids {
			wantID := i
			if fixture.reverse {
				wantID = smallWindowRows - 1 - i
			}
			if id != int64(wantID) {
				b.Fatalf("row %d id=%d want=%d", i, id, wantID)
			}
			for _, name := range names {
				actual, want := got[name][i], fixture.expected[name][id]
				if actual.valid != want.valid || want.valid && !(math.IsNaN(actual.value) && math.IsNaN(want.value)) && math.Float64bits(actual.value) != math.Float64bits(want.value) {
					b.Fatalf("row %d %s got=%g/%v want=%g/%v", id, name, actual.value, actual.valid, want.value, want.valid)
				}
			}
		}
	}
	var peak, spilled int64
	b.ReportAllocs()
	b.SetBytes(smallWindowRows * 8)
	b.ResetTimer()
	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		// Measure complete query setup/open/read/capture/validation/close.
		// There are no per-iteration timer transitions.
		ctx := context.Background()
		var manager memory.Manager = memory.NewBudgetManager(8 << 20)
		var query *memory.QueryBudget
		if external {
			var err error
			query, err = memory.NewEngineBudget(2 << 20).BeginQuery(memory.BudgetOptions{SoftLimitBytes: 48 << 10, HardLimitBytes: 2 << 20, Spill: memory.SpillOptions{Directory: dir, MaxBytes: 64 << 20}})
			if err != nil {
				b.Fatal(err)
			}
			manager = query
			ctx = memory.WithQueryBudget(ctx, query)
		}
		win, err := NewWindowSourceWithOptions(fixture.src, []string{"g"}, []SortKey{{Column: "id"}}, exprs, fields, manager, WindowSourceOptions{Mode: mode})
		if err != nil {
			b.Fatal(err)
		}
		stream, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: kernels, BatchRows: 1024})
		if err != nil {
			b.Fatal(err)
		}
		out := memory.NewBatch()
		seen := 0
		for {
			ok, err := stream.Next(ctx, out)
			if err != nil {
				stream.Close()
				b.Fatal(err)
			}
			if !ok {
				break
			}
			if seen+out.Rows > smallWindowRows {
				stream.Close()
				b.Fatal("too many output rows")
			}
			copy(ids[seen:], out.Int64s["id"].Values[:out.Rows])
			for _, name := range names {
				vector := out.Float64s[name]
				for j := 0; j < out.Rows; j++ {
					got[name][seen+j] = smallWindowCell{vector.Values[j], vector.IsValid(j)}
				}
			}
			seen += out.Rows
		}
		out.ClearColumns()
		if err := stream.Close(); err != nil {
			b.Fatal(err)
		}
		if seen != smallWindowRows || manager.Used() != 0 {
			b.Fatalf("rows=%d budget=%d", seen, manager.Used())
		}
		validate()
		if query != nil {
			snapshot := query.Snapshot()
			if snapshot.SpilledBytes == 0 {
				b.Fatal("external case did not spill")
			}
			peak = max(peak, snapshot.PeakBytes)
			spilled += snapshot.SpilledBytes
			store, err := query.SpillManager()
			if err != nil {
				b.Fatal(err)
			}
			if store.(*memory.QuerySpillStore).Used() != 0 {
				b.Fatal("spill files remain after stream close")
			}
			if err := query.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	// Recheck the full final measured result outside the timed region too.
	validate()
	if external {
		b.ReportMetric(float64(peak), "query-peak-B")
		b.ReportMetric(float64(spilled)/float64(b.N), "spill-B/op")
	}
}

func BenchmarkWindowExactSmallMatrix(b *testing.B) {
	for _, mode := range []struct {
		name string
		mode WindowExecutionMode
	}{{"resident", WindowExecutionResident}, {"ordered", WindowExecutionOrdered}} {
		for _, width := range []int{1, 2, 4, 8, 16, 32, 64} {
			for _, names := range [][]string{{"sum"}, {"avg"}, {"sum", "avg"}} {
				label := names[0]
				if len(names) == 2 {
					label = "both"
				}
				b.Run(fmt.Sprintf("%s/width%d/%s", mode.name, width, label), func(b *testing.B) { smallWindowBenchmark(b, mode.mode, "integer", width, names) })
			}
		}
	}
}

func BenchmarkWindowExactSmallRepresentative(b *testing.B) {
	for _, mode := range []struct {
		name string
		mode WindowExecutionMode
	}{{"resident", WindowExecutionResident}, {"ordered", WindowExecutionOrdered}, {"external", WindowExecutionExternalSort}} {
		for _, workload := range []string{"nullable", "mixed_scale", "multipartition"} {
			b.Run(mode.name+"/"+workload+"/width8/both", func(b *testing.B) { smallWindowBenchmark(b, mode.mode, workload, 8, []string{"sum", "avg"}) })
		}
	}
}

func BenchmarkWindowExactSpecial(b *testing.B) {
	for _, mode := range []struct {
		name string
		mode WindowExecutionMode
	}{{"resident", WindowExecutionResident}, {"ordered", WindowExecutionOrdered}, {"external", WindowExecutionExternalSort}} {
		b.Run(mode.name+"/width4/both", func(b *testing.B) { smallWindowBenchmark(b, mode.mode, "special", 4, []string{"sum", "avg"}) })
	}
}
