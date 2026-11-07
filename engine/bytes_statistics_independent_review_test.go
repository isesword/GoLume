package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"gocolumn/aggregate"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/pipeline"
	"gocolumn/source/inmemory"
	"testing"
)

func FuzzBytesStatisticsIndependentMixedFrames(f *testing.F) {
	seed := make([]byte, 24+4+9+43+92)
	copy(seed, bytesStatisticsAggregateRunMagic[:])
	binary.LittleEndian.PutUint16(seed[8:], 5)
	seed[10] = byte(GroupShapeI32)
	binary.LittleEndian.PutUint32(seed[16:], 2)
	binary.LittleEndian.PutUint32(seed[20:], 1)
	seed[24] = byte(memory.TypeString)
	seed[25] = 6
	seed[26] = byte(memory.TypeFloat64)
	seed[27] = 1
	seed[28] = 1
	f.Add(seed)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, p []byte) {
		r, err := newBytesStatisticsAggregateRunReader(p, GroupShapeI32, nil, nil, []memory.DataType{memory.TypeString, memory.TypeFloat64}, []aggregate.StatisticsLayout{{First: true, Last: true}, {Moments: true}})
		if err == nil {
			err = r.visit(func(a aggregateRecord) error {
				_ = bytesAggregateRecordState(a, 0, r.types, r.layouts)
				_ = statisticsAggregateRecordState(a, 1, memory.TypeFloat64, r.layouts[1])
				return nil
			})
		}
		if err != nil && !errors.Is(err, memory.ErrSpillCorrupt) {
			t.Fatal(err)
		}
	})
}

func TestBytesStatisticsIndependentMergeRetry(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "n", Type: memory.TypeInt64}, memory.Field{Name: "a", Type: memory.TypeString}, memory.Field{Name: "b", Type: memory.TypeBinary})
	compiled, err := compileMultiAggregate(schema, []expr.Aggregate{expr.Sum("n", "sum"), expr.Last("a", "a"), expr.Last("b", "b")})
	if err != nil {
		t.Fatal(err)
	}
	mgr := memory.NewBudgetManager(1 << 20)
	calls, fail := 0, 0
	reserve := func(n int64) (memory.Reservation, error) {
		calls++
		if calls == fail {
			return nil, memory.ErrBudgetExceeded
		}
		return mgr.Reserve(context.Background(), n)
	}
	dst, err := newAggregateStateTable(GroupShapeGlobal, aggregate.Layout{}, 1, 16, 0, reserve, compiled)
	if err != nil {
		t.Fatal(err)
	}
	src, err := newAggregateStateTable(GroupShapeGlobal, aggregate.Layout{}, 1, 16, 0, reserve, compiled)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		src.close()
		dst.close()
		if mgr.Used() != 0 {
			t.Fatal("leak", mgr.Used())
		}
	}()
	b := memory.NewBatch()
	b.Rows = 2
	b.Int64s["n"] = &memory.Int64Vector{Values: []int64{7, 11}}
	b.Strings["a"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("ab")}
	b.Strings["b"] = &memory.StringViewVector{Offsets: []uint32{0, 1, 2}, Data: []byte("cd")}
	if _, err = dst.multi.add(aggregateRecord{ordinal: aggregate.SourceOrdinal{Row: 0}}, b, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = src.multi.add(aggregateRecord{ordinal: aggregate.SourceOrdinal{Row: 1}}, b, 1); err != nil {
		t.Fatal(err)
	}
	used := mgr.Used()
	fail = calls + 2
	if _, err = dst.multi.merge(src.multi.record(0)); !errors.Is(err, memory.ErrBudgetExceeded) {
		t.Fatal(err)
	}
	if mgr.Used() != used || dst.multi.numeric.Result(0, 0).SumBits != 7 || string(dst.multi.statistics.BytesResult(0, 0).Last) != "a" {
		t.Fatal("partial failed merge")
	}
	fail = 0
	if _, err = dst.multi.merge(src.multi.record(0)); err != nil {
		t.Fatal(err)
	}
	if dst.multi.numeric.Result(0, 0).SumBits != 18 || string(dst.multi.statistics.BytesResult(0, 0).Last) != "b" || string(dst.multi.statistics.BytesResult(0, 1).Last) != "d" {
		t.Fatal("retry merge incorrect")
	}
}

func TestBytesStatisticsIndependentDictionaryOrder(t *testing.T) {
	for _, mode := range []string{"partitioned-hash", "spill", "distinct"} {
		t.Run(mode, func(t *testing.T) {
			schema := memory.NewSchema(memory.Field{Name: "k", Type: memory.TypeInt32}, memory.Field{Name: "s", Type: memory.TypeString, Encoding: memory.EncodingDictionary, Nullable: true}, memory.Field{Name: "keep", Type: memory.TypeBool})
			var batches []*memory.Batch
			for part := 0; part < 6; part++ {
				b := memory.NewBatch()
				b.Rows = 3
				b.Int32s["k"] = &memory.Int32Vector{Values: []int32{0, 1, 0}}
				b.Uint32s["keep"] = &memory.Uint32Vector{Values: []uint32{1, 1, 0}}
				// Equal dictionary codes name different bytes in every source partition.
				valid := memory.Bitmap{7}
				if part == 0 {
					valid = memory.Bitmap{5}
				}
				b.DictStrings["s"] = &memory.DictionaryStringVector{Indices: []uint32{0, 1, 0}, Offsets: []uint32{0, 1, 2}, Data: []byte{byte('a' + part), byte('A' + part)}, Validity: valid, Encoding: memory.EncodingDictionary}
				batches = append(batches, b)
			}
			src, err := inmemory.New(schema, batches...)
			if err != nil {
				t.Fatal(err)
			}
			opts := streamTestOptions()
			opts.Workers = 4
			opts.Pipeline.Workers = 4
			opts.Pipeline.Ordering = pipeline.OrderingRelaxed
			opts.Pipeline.MaxMorselRows = 1
			q := expr.Query{GroupBy: []string{"k"}, Filter: expr.Col("keep"), Aggregates: []expr.Aggregate{expr.First("s", "first"), expr.Last("s", "last"), expr.Count("count")}, Hints: expr.Hints{Cardinality: 2}}
			if mode == "spill" {
				opts.ExecutionPolicy = ExecutionBounded
				opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 1 << 20}
			} else if mode == "distinct" {
				q.Aggregates = append(q.Aggregates, expr.NUnique("s", "unique"))
			} else {
				q.Hints.ForceStrategy = mode
			}
			e := New(opts)
			defer e.Close()
			for repeat := 0; repeat < 4; repeat++ {
				r, err := e.ExecuteTyped(context.Background(), src, q, ExecuteOptions{Materialize: true})
				if err != nil {
					t.Fatal(err)
				}
				if len(r.Rows) != 2 {
					t.Fatal(r)
				}
				for _, row := range r.Rows {
					a := row.Aggregates
					if a[2].Value.Bits != 6 {
						t.Fatal("count", a)
					}
					if row.Keys[0].Value.Bits == 0 {
						if !a[0].Value.Valid || a[0].Value.Text != "a" || a[1].Value.Text != "f" {
							t.Fatal("first group", a)
						}
					} else {
						if a[0].Value.Valid || a[1].Value.Text != "F" {
							t.Fatal("null endpoint or order", a)
						}
					}
				}
			}
			e.Close()
			if e.Memory().Used() != 0 {
				t.Fatal("leak", e.Memory().Used())
			}
		})
	}
}
