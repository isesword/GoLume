package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/planner"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func partitionedQuery() expr.Query {
	return expr.Query{GroupBy: []string{"key"}, Aggregates: []expr.Aggregate{expr.Sum("value", "sum"), expr.Count("rows"), expr.CountColumn("value", "count"), expr.Min("value", "min"), expr.Max("value", "max"), expr.Avg("value", "avg")}, Hints: expr.Hints{ForceStrategy: "partitioned-hash", Cardinality: 17}}
}

func TestPartitionedNativeSingleFragmentAndRecordCursor(t *testing.T) {
	for _, workers := range []int{1, 2, 3, 7} {
		for _, rows := range []int{0, 63, 64, 65, 131} {
			t.Run(fmt.Sprintf("workers=%d/rows=%d", workers, rows), func(t *testing.T) {
				src := &generatedStreamSource{rows: rows, groups: 17}
				options := streamTestOptions()
				options.Workers = workers
				eng := New(options)
				defer eng.Close()
				prepared, err := eng.Prepare(context.Background(), src, partitionedQuery(), PrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				if plan := prepared.Plan(); plan.Strategy.Kind != planner.StrategyPartitionedHash || plan.Strategy.Workers != workers {
					t.Fatalf("plan=%+v", plan)
				}
				result, err := prepared.Execute(context.Background(), ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if result.GroupCount != min(rows, 17) || result.SelectedRows != rows || result.Checksum != int64(rows) {
					t.Fatalf("result=%+v", result)
				}
				expected := map[int64]AggregateRow{}
				for _, row := range result.Rows {
					key := row.Keys[0].Int64
					want := int64(rows / 17)
					if int(key) < rows%17 {
						want++
					}
					if row.Sum != want || row.Count != uint64(want) || len(row.Aggregates) != 6 || row.Aggregates[2].Int64 != want || row.Aggregates[3].Int64 != 1 || row.Aggregates[4].Int64 != 1 || row.Aggregates[5].Float64 != 1 {
						t.Fatalf("row=%+v want=%d", row, want)
					}
					expected[key] = row
				}
				budget, err := eng.BeginQueryMemory()
				if err != nil {
					t.Fatal(err)
				}
				defer budget.Close()
				ctx := memory.WithQueryBudget(context.Background(), budget)
				stream, err := prepared.NewResidentAggregateStream(ctx, prepared.resultSchema(), 1)
				if err != nil {
					t.Fatal(err)
				}
				batch := memory.NewBatch()
				defer batch.ReleaseOwned()
				emitted := 0
				for {
					ok, err := stream.Next(ctx, batch)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					if batch.Rows != 1 {
						t.Fatalf("output rows=%d", batch.Rows)
					}
					key := int64(batch.Int32s["key"].Values[0])
					row, ok := expected[key]
					if !ok {
						t.Fatalf("duplicate/unexpected output key=%d", key)
					}
					if batch.Int64s["sum"].Values[0] != row.Sum || batch.Uint64s["rows"].Values[0] != row.Count || batch.Uint64s["count"].Values[0] != row.Count || batch.Float64s["avg"].Values[0] != 1 {
						t.Fatalf("columnar/record sink mismatch for key=%d", key)
					}
					delete(expected, key)
					emitted++
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				if len(expected) != 0 || emitted != result.GroupCount || eng.Memory().Used() != 0 {
					t.Fatalf("remaining=%d emitted=%d groups=%d used=%d", len(expected), emitted, result.GroupCount, eng.Memory().Used())
				}
			})
		}
	}
}

func TestPartitionedNativeBudgetFailureAndCancellation(t *testing.T) {
	for _, limit := range []int64{2 << 10, 16 << 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			src := &generatedStreamSource{rows: 1 << 20, groups: 1 << 20}
			options := streamTestOptions()
			options.Workers = 4
			options.MemoryLimitBytes = limit
			eng := New(options)
			defer eng.Close()
			query := partitionedQuery()
			query.Hints.Cardinality = 8
			_, err := eng.Execute(context.Background(), src, query, ExecuteOptions{})
			if !errors.Is(err, memory.ErrBudgetExceeded) {
				t.Fatalf("expected originating budget error, got %v", err)
			}
			if eng.Memory().Used() != 0 || src.opened.Load() != src.closed.Load() {
				t.Fatalf("used=%d opened=%d closed=%d", eng.Memory().Used(), src.opened.Load(), src.closed.Load())
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &generatedStreamSource{rows: 1 << 20, groups: 17, cancel: cancel, cancelAt: 256}
	options := streamTestOptions()
	options.Workers = 7
	eng := New(options)
	defer eng.Close()
	_, err := eng.Execute(ctx, src, partitionedQuery(), ExecuteOptions{})
	if !errors.Is(err, context.Canceled) || eng.Memory().Used() != 0 || src.closed.Load() != 1 {
		t.Fatalf("error=%v used=%d closed=%d", err, eng.Memory().Used(), src.closed.Load())
	}
}

func TestPartitionedScratchIndependentOfInputRows(t *testing.T) {
	var peak int64
	for _, rows := range []int{1024, 1 << 20} {
		src := &generatedStreamSource{rows: rows, groups: 17}
		options := streamTestOptions()
		options.Workers = 4
		eng := New(options)
		budget, err := eng.BeginQueryMemory()
		if err != nil {
			t.Fatal(err)
		}
		_, err = eng.Execute(memory.WithQueryBudget(context.Background(), budget), src, partitionedQuery(), ExecuteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		snap := budget.Snapshot()
		if snap.UsedBytes != 0 || src.maxBatch.Load() > 64 {
			t.Fatalf("snapshot=%+v maxbatch=%d", snap, src.maxBatch.Load())
		}
		if peak != 0 && peak != snap.PeakBytes {
			t.Fatalf("scratch scales with total input: %d -> %d", peak, snap.PeakBytes)
		}
		peak = snap.PeakBytes
		budget.Close()
		eng.Close()
	}
}

var _ source.Source = (*generatedStreamSource)(nil)

type partitionOracleState struct {
	sum, min, max int64
	rows, values  uint64
}

func TestPartitionedNullableKeysAndAggregateStates(t *testing.T) {
	for _, shape := range []string{"int32", "int64", "uint32", "pair", "string", "dictionary"} {
		for _, workers := range []int{1, 2, 3, 7} {
			t.Run(fmt.Sprintf("%s/workers=%d", shape, workers), func(t *testing.T) {
				keyType := memory.TypeInt32
				if shape == "int64" {
					keyType = memory.TypeInt64
				}
				if shape == "uint32" {
					keyType = memory.TypeUint32
				}
				if shape == "string" || shape == "dictionary" {
					keyType = memory.TypeString
				}
				keyField := memory.Field{Name: "key", Type: keyType, Nullable: true}
				if shape == "string" {
					keyField.Encoding = memory.EncodingView
				}
				if shape == "dictionary" {
					keyField.Encoding = memory.EncodingDictionary
					keyField.DictionaryDomain = "partition-test-v1"
				}
				fields := []memory.Field{keyField, {Name: "value", Type: memory.TypeInt64, Nullable: true}, {Name: "pred", Type: memory.TypeInt64}}
				groups := []string{"key"}
				if shape == "pair" {
					fields = append(fields, memory.Field{Name: "other", Type: memory.TypeInt64, Nullable: true})
					groups = append(groups, "other")
				}
				dictOffsets := make([]uint32, 12)
				var dictData []byte
				for key := 0; key < 11; key++ {
					dictData = append(dictData, fmt.Sprintf("word-%d", key)...)
					dictOffsets[key+1] = uint32(len(dictData))
				}
				expected := map[string]partitionOracleState{}
				batches := make([]*memory.Batch, 0, 4)
				totalSelected, totalSum := 0, int64(0)
				absolute := 0
				for _, n := range []int{63, 65, 131, 64, 8193} {
					b := memory.NewBatch()
					b.Rows = n
					validity := memory.EnsureBitmap(nil, n)
					otherValidity := memory.EnsureBitmap(nil, n)
					valueValidity := memory.EnsureBitmap(nil, n)
					keys := make([]int32, n)
					other := make([]int64, n)
					values := make([]int64, n)
					pred := make([]int64, n)
					codes := make([]uint32, n)
					offsets := make([]uint32, n+1)
					var data []byte
					for row := 0; row < n; row++ {
						index := absolute + row
						firstValid, secondValid, valueValid := index%5 != 0, index%3 != 0, index%4 != 0 && index%11 != 2
						if firstValid {
							memory.Set(validity, row)
						}
						if secondValid {
							memory.Set(otherValidity, row)
						}
						if valueValid {
							memory.Set(valueValidity, row)
						}
						keys[row] = int32(index % 11)
						other[row] = int64(index%7 - 3)
						codes[row] = uint32(index % 11)
						keyText := fmt.Sprintf("word-%d", index%11)
						if !firstValid {
							keys[row] = int32(10000 + index)
							codes[row] = uint32(10000 + index)
							keyText = fmt.Sprintf("ignored-%d", index)
						}
						if !secondValid {
							other[row] = int64(10000 + index)
						}
						data = append(data, keyText...)
						offsets[row+1] = uint32(len(data))
						values[row] = int64(index%13 - 6)
						if !valueValid {
							values[row] = 9999
						}
						pred[row] = int64(index % 3)
						if pred[row] == 0 {
							continue
						}
						canonical := "NULL"
						if firstValid {
							if keyType == memory.TypeString {
								canonical = fmt.Sprintf("word-%d", index%11)
							} else {
								canonical = fmt.Sprint(index % 11)
							}
						}
						if shape == "pair" {
							second := "NULL"
							if secondValid {
								second = fmt.Sprint(index%7 - 3)
							}
							canonical += "/" + second
						}
						state := expected[canonical]
						state.rows++
						if valueValid {
							value := values[row]
							state.sum += value
							if state.values == 0 || value < state.min {
								state.min = value
							}
							if state.values == 0 || value > state.max {
								state.max = value
							}
							state.values++
							totalSum += value
						}
						expected[canonical] = state
						totalSelected++
					}
					b.Int64s["value"] = &memory.Int64Vector{Values: values, Validity: valueValidity}
					b.Int64s["pred"] = &memory.Int64Vector{Values: pred}
					if shape == "pair" {
						b.Int64s["other"] = &memory.Int64Vector{Values: other, Validity: otherValidity}
					}
					switch shape {
					case "int32", "pair":
						b.Int32s["key"] = &memory.Int32Vector{Values: keys, Validity: validity}
					case "int64":
						wide := make([]int64, n)
						for row, key := range keys {
							wide[row] = int64(key)
						}
						b.Int64s["key"] = &memory.Int64Vector{Values: wide, Validity: validity}
					case "uint32":
						b.Uint32s["key"] = &memory.Uint32Vector{Values: codes, Validity: validity}
					case "string":
						b.Strings["key"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity, Encoding: memory.EncodingView}
					case "dictionary":
						b.DictStrings["key"] = &memory.DictionaryStringVector{Indices: codes, Offsets: dictOffsets, Data: dictData, Validity: validity, Encoding: memory.EncodingDictionary, DictionaryDomain: "partition-test-v1"}
					}
					batches = append(batches, b)
					absolute += n
				}
				src, err := inmemory.New(memory.NewSchema(fields...), batches...)
				if err != nil {
					t.Fatal(err)
				}
				if shape == "dictionary" {
					src.WithDictionary("key", source.Dictionary{Domain: "partition-test-v1", Offsets: dictOffsets, Data: dictData})
				}
				options := streamTestOptions()
				options.Workers = workers
				options.MemoryLimitBytes = 8 << 20
				options.Pipeline.MaxInFlightBytes = 1 << 20
				options.Pipeline.MaxMorselBytes = 128 << 10
				options.Pipeline.MaxMorselRows = 8192
				eng := New(options)
				defer eng.Close()
				query := partitionedQuery()
				query.GroupBy = groups
				query.Filter = expr.GT(expr.Col("pred"), expr.I64(0))
				query.Hints.Cardinality = 8
				result, err := eng.Execute(context.Background(), src, query, ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if result.GroupCount != len(expected) || result.SelectedRows != totalSelected || result.Checksum != totalSum {
					t.Fatalf("summary=%+v expectedgroups=%d rows=%d sum=%d", result, len(expected), totalSelected, totalSum)
				}
				for _, row := range result.Rows {
					canonical := "NULL"
					if row.Keys[0].Valid {
						if keyType == memory.TypeString {
							canonical = row.Keys[0].String
						} else if keyType == memory.TypeUint32 {
							canonical = fmt.Sprint(row.Keys[0].Uint64)
						} else {
							canonical = fmt.Sprint(row.Keys[0].Int64)
						}
					}
					if shape == "pair" {
						second := "NULL"
						if row.Keys[1].Valid {
							second = fmt.Sprint(row.Keys[1].Int64)
						}
						canonical += "/" + second
					}
					state, ok := expected[canonical]
					if !ok {
						t.Fatalf("duplicate or unexpected key=%s", canonical)
					}
					delete(expected, canonical)
					aggs := row.Aggregates
					valid := state.values > 0
					if row.Sum != state.sum || row.Count != state.rows || aggs[0].Valid != valid || aggs[0].Int64 != state.sum || aggs[1].Int64 != int64(state.rows) || aggs[2].Int64 != int64(state.values) || aggs[3].Valid != valid || aggs[4].Valid != valid || aggs[5].Valid != valid {
						t.Fatalf("key=%s actual=%+v expected=%+v", canonical, row, state)
					}
					if valid && (aggs[3].Int64 != state.min || aggs[4].Int64 != state.max || aggs[5].Float64 != float64(state.sum)/float64(state.values)) {
						t.Fatalf("key=%s aggregate=%+v expected=%+v", canonical, aggs, state)
					}
				}
				if len(expected) != 0 || eng.Memory().Used() != 0 {
					t.Fatalf("remaining=%d used=%d", len(expected), eng.Memory().Used())
				}
			})
		}
	}
}

func TestPartitionedFiniteBudgetDoesNotPreallocateEmptyShards(t *testing.T) {
	src := &generatedStreamSource{rows: 1 << 18, groups: 1}
	options := streamTestOptions()
	options.Workers = 4
	options.MemoryLimitBytes = 16 << 10
	options.ExecutionPolicy = ExecutionMemoryOnly
	eng := New(options)
	defer eng.Close()
	query := partitionedQuery()
	query.Hints.ForceStrategy = ""
	query.Hints.Cardinality = 1 << 20
	query.Hints.Selectivity = 1
	prepared, err := eng.Prepare(context.Background(), src, query, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.Plan().Strategy.Kind != planner.StrategyPartitionedHash {
		t.Fatalf("plan=%+v", prepared.Plan())
	}
	result, err := prepared.Execute(context.Background(), ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupCount != 1 || result.SelectedRows != src.rows || eng.Memory().Used() != 0 {
		t.Fatalf("result=%+v used=%d", result, eng.Memory().Used())
	}
}
