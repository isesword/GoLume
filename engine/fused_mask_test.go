package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"testing"
)

func TestFusedMaskIndependentNullableAggregateOracle(t *testing.T) {
	for _, shape := range []string{"int32", "int64", "uint32", "pair", "string", "dictionary"} {
		for _, workers := range []int{1} {
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
				query.Hints.ForceStrategy = "direct-fused"
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

func TestFusedMaskGrowthBudgetAndCancellation(t *testing.T) {
	query := partitionedQuery()
	query.Filter = expr.GE(expr.Col("key"), expr.I64(0))
	query.Hints.ForceStrategy = "direct-fused"
	query.Hints.Cardinality = 8
	for _, limit := range []int64{2048, 16384} {
		opts := streamTestOptions()
		opts.MemoryLimitBytes = limit
		eng := New(opts)
		src := &generatedStreamSource{rows: 1 << 20, groups: 1 << 20}
		_, err := eng.Execute(context.Background(), src, query, ExecuteOptions{})
		if !errors.Is(err, memory.ErrBudgetExceeded) {
			t.Fatalf("limit=%d err=%v", limit, err)
		}
		if eng.Memory().Used() != 0 || src.opened.Load() != src.closed.Load() {
			t.Fatal("budget failure leaked")
		}
		eng.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng := New(streamTestOptions())
	defer eng.Close()
	src := &generatedStreamSource{rows: 1 << 20, groups: 17, cancel: cancel, cancelAt: 256}
	_, err := eng.Execute(ctx, src, query, ExecuteOptions{})
	if !errors.Is(err, context.Canceled) || eng.Memory().Used() != 0 || src.opened.Load() != src.closed.Load() {
		t.Fatalf("cancel=%v used=%d", err, eng.Memory().Used())
	}
}

func TestFusedWideMaskBridgeAndFailure(t *testing.T) {
	rows := []int{0, 5, 63, 64, 130}
	produce := func(emit func(int) error) (int, error) {
		for _, row := range rows {
			if err := emit(row); err != nil {
				return 0, err
			}
		}
		return len(rows), nil
	}
	var seen []int
	n, err := collectSelectedMasks(produce, memory.Bitmap{0}, func(base int, word memory.Bitmap) error {
		return consumeSelectedWord(base, word[0], func(row int) error { seen = append(seen, row); return nil })
	})
	if err != nil || n != len(rows) || fmt.Sprint(seen) != fmt.Sprint(rows) {
		t.Fatalf("n=%d seen=%v err=%v", n, seen, err)
	}
	want := errors.New("stop mask")
	_, err = collectSelectedMasks(produce, memory.Bitmap{0}, func(base int, _ memory.Bitmap) error {
		if base == 64 {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}

func TestFusedMaskReservedOnceBeforeAllocation(t *testing.T) {
	for _, limit := range []int64{7, 8} {
		manager := memory.NewBudgetManager(limit)
		workspace := aggregatePredicateWorkspace{manager: manager}
		err := workspace.ensureFused(context.Background())
		if limit == 7 {
			if !errors.Is(err, memory.ErrBudgetExceeded) || workspace.fusedMask != nil || manager.Used() != 0 {
				t.Fatalf("failed reserve allocated: err=%v cap=%d used=%d", err, cap(workspace.fusedMask), manager.Used())
			}
		} else {
			if err != nil || cap(workspace.fusedMask) != 1 || manager.Used() != 8 || workspace.fusedReservation.Bytes() != 8 {
				t.Fatalf("err=%v cap=%d used=%d", err, cap(workspace.fusedMask), manager.Used())
			}
			ptr := &workspace.fusedMask[0]
			if err := workspace.ensureFused(context.Background()); err != nil || ptr != &workspace.fusedMask[0] || manager.Used() != 8 {
				t.Fatal("mask was not reused")
			}
		}
		workspace.close()
		workspace.close()
		if manager.Used() != 0 || workspace.fusedMask != nil {
			t.Fatal("mask close leaked")
		}
	}
}

func TestFusedGeneratorBorrowsTheSameMaskWord(t *testing.T) {
	batch := memory.NewBatch()
	batch.Rows = 131
	values := make([]int32, batch.Rows)
	for i := range values {
		values[i] = int32(i)
	}
	batch.Int32s["pred"] = &memory.Int32Vector{Values: values}
	mask := memory.Bitmap{0}
	calls := 0
	selected, err := forSimpleSelectedBlocks(batch, &expr.SimpleComparison{Column: "pred", Type: memory.TypeInt32, Op: expr.OpGE, LiteralI64: 0}, kernel.New(kernel.ModeScalar), mask, func(base int, borrowed memory.Bitmap) error {
		if len(borrowed) != 1 || &borrowed[0] != &mask[0] {
			t.Fatal("mask copied instead of borrowed")
		}
		if base != calls*64 {
			t.Fatalf("base=%d calls=%d", base, calls)
		}
		want := ^uint64(0)
		if base == 128 {
			want = 7
		}
		if borrowed[0] != want {
			t.Fatalf("word=%x want=%x", borrowed[0], want)
		}
		calls++
		return nil
	})
	if err != nil || calls != 3 || selected != 131 {
		t.Fatalf("calls=%d selected=%d err=%v", calls, selected, err)
	}
}
