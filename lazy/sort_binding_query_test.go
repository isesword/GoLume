package lazy

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"gocolumn/engine"
	"gocolumn/memory"
	"gocolumn/source/inmemory"
)

// benchmarkSortBindingQuery measures the complete prepared scan/projection,
// ordering, collection and cleanup path, checking every output ordinal. Its
// fixtures are deterministic synthetic inputs, not production performance data.
func benchmarkSortBindingQuery(b *testing.B, rows int, kind, strategy string) {
	schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeInt64})
	if kind == "nullable-float" {
		schema = memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeFloat64, Nullable: true})
	} else if kind == "dictionary" {
		schema = memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64}, memory.Field{Name: "key", Type: memory.TypeString, Encoding: memory.EncodingDictionary})
	}
	values := make([]int64, rows)
	floats := make([]float64, rows)
	valid := make([]bool, rows)
	var batches []*memory.Batch
	for base := 0; base < rows; base += 4096 {
		n := min(4096, rows-base)
		batch := memory.NewBatch()
		ids := make([]int64, n)
		for i := range n {
			row := base + i
			ids[i] = int64(row)
			values[row] = int64((uint64(row)*6364136223846793005 + 1442695040888963407) % 257)
			floats[row] = float64(values[row])
			if row%37 == 0 {
				floats[row] = math.NaN()
			}
			if row%43 == 0 {
				floats[row] = math.Copysign(0, -1)
			}
			valid[row] = row%11 != 0
		}
		batch.Int64s["id"] = &memory.Int64Vector{Values: ids}
		switch kind {
		case "nullable-float":
			bitmap := memory.EnsureBitmap(nil, n)
			for i := range n {
				if valid[base+i] {
					memory.Set(bitmap, i)
				}
			}
			batch.Float64s["key"] = &memory.Float64Vector{Values: floats[base : base+n], Validity: bitmap}
		case "dictionary":
			codes := make([]uint32, n)
			for i := range n {
				codes[i] = uint32(values[base+i] % 4)
			}
			batch.DictStrings["key"] = &memory.DictionaryStringVector{Indices: codes, Offsets: []uint32{0, 1, 2, 3, 4}, Data: []byte("zamb"), DictionaryDomain: "sort-binding-query"}
		default:
			batch.Int64s["key"] = &memory.Int64Vector{Values: values[base : base+n]}
		}
		batch.Reset(n)
		batches = append(batches, batch)
	}
	input, err := inmemory.New(schema, batches...)
	if err != nil {
		b.Fatal(err)
	}
	want := make([]int, rows)
	for i := range want {
		want[i] = i
	}
	if strategy != "projection-control" {
		sort.SliceStable(want, func(i, j int) bool {
			x, y := want[i], want[j]
			if kind == "nullable-float" {
				if valid[x] != valid[y] {
					return valid[x]
				}
				if !valid[x] {
					return false
				}
				a, z := floats[x], floats[y]
				if math.IsNaN(a) {
					return !math.IsNaN(z)
				}
				if math.IsNaN(z) {
					return false
				}
				return a > z
			}
			if kind == "dictionary" {
				return "zamb"[values[x]%4] > "zamb"[values[y]%4]
			}
			return values[x] > values[y]
		})
	}
	options := engine.DefaultOptions()
	options.Workers = 1
	options.MemoryLimitBytes = 64 << 20
	if strategy == "parallel" {
		options.Workers = 4
	}
	if strategy == "external" {
		options.ExecutionPolicy = engine.ExecutionBounded
		options.MemoryLimitBytes = 4 << 20
		options.Spill = engine.SpillOptions{Directory: b.TempDir(), MaxBytes: 128 << 20}
	}
	session := NewSession(options)
	defer session.Close()
	query := session.FromSource(input).Filter(Col("id").Ge(Lit(0)))
	if strategy == "topk-control" {
		query = query.TopK(100, Desc("key"))
		want = want[:min(100, len(want))]
	} else if strategy != "projection-control" {
		query = query.SortBy(SortExpr{Expr: Col("key"), Descending: true, NullsLast: true})
	}
	query = query.Select("id")
	plan, err := query.ExplainPhysical(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	if strategy == "external" && !strings.Contains(plan, "strategy=external-stable") {
		b.Fatalf("external sort not selected: %s", plan)
	}
	if (strategy == "resident" || strategy == "parallel") && !strings.Contains(plan, "strategy=resident-stable") {
		b.Fatalf("resident sort not selected: %s", plan)
	}
	prepared, err := query.Prepare(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer prepared.Close()
	var maxPeak int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		budget, err := session.Engine().BeginQueryMemory()
		if err != nil {
			b.Fatal(err)
		}
		frame, err := prepared.Execute(memory.WithQueryBudget(context.Background(), budget))
		if err != nil {
			b.Fatal(err)
		}
		at := 0
		for _, batch := range frame.batches() {
			for _, id := range batch.Int64s["id"].Values {
				if at >= len(want) || id != int64(want[at]) {
					b.Fatalf("row=%d id=%d", at, id)
				}
				at++
			}
		}
		if at != len(want) {
			b.Fatalf("rows=%d want=%d", at, len(want))
		}
		if err := frame.Close(); err != nil {
			b.Fatal(err)
		}
		snapshot := budget.Snapshot()
		maxPeak = max(maxPeak, snapshot.PeakBytes)
		if snapshot.UsedBytes != 0 || snapshot.PeakBytes > budget.Limit() {
			b.Fatalf("budget=%+v", snapshot)
		}
		if err := budget.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(maxPeak), "peak-query-B")
}

func BenchmarkSortBindingQuery(b *testing.B) {
	for _, fixture := range []struct {
		rows           int
		kind, strategy string
	}{
		{31, "int", "resident"}, {65536, "int", "resident"}, {65536, "int", "parallel"},
		{65536, "nullable-float", "resident"}, {65536, "dictionary", "resident"},
		{16384, "int", "external"}, {16384, "nullable-float", "external"}, {16384, "dictionary", "external"}, {65536, "int", "topk-control"}, {65536, "int", "projection-control"},
	} {
		b.Run(fmt.Sprintf("%s/%s/%d", fixture.strategy, fixture.kind, fixture.rows), func(b *testing.B) { benchmarkSortBindingQuery(b, fixture.rows, fixture.kind, fixture.strategy) })
	}
}
