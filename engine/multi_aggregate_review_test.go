package engine

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"reflect"
	"sort"
	"testing"
)

func TestMultiAggregateReviewKeyShapes(t *testing.T) {
	for _, shape := range []string{"i64", "u32", "string", "dictionary", "pair"} {
		t.Run(shape, func(t *testing.T) {
			fields := []memory.Field{{Name: "a", Type: memory.TypeInt64, Nullable: true}, {Name: "b", Type: memory.TypeInt64, Nullable: true}}
			typ := memory.TypeInt64
			if shape == "u32" {
				typ = memory.TypeUint32
			}
			if shape == "string" || shape == "dictionary" {
				typ = memory.TypeString
			}
			field := memory.Field{Name: "k", Type: typ, Nullable: true}
			if shape == "dictionary" {
				field.Encoding = memory.EncodingDictionary
				field.DictionaryDomain = "review-domain"
			}
			fields = append(fields, field)
			keys := []string{"k"}
			if shape == "pair" {
				fields = append(fields, memory.Field{Name: "j", Type: memory.TypeUint32, Nullable: true})
				keys = append(keys, "j")
			}
			var batches []*memory.Batch
			for start := 0; start < 1027; start += 17 {
				n := min(17, 1027-start)
				b := memory.NewBatch()
				b.Rows = n
				a := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
				v := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
				validity := make(memory.Bitmap, memory.WordsFor(n))
				i64 := make([]int64, n)
				u32 := make([]uint32, n)
				offsets := make([]uint32, n+1)
				var data []byte
				j := &memory.Uint32Vector{Values: make([]uint32, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
				for row := 0; row < n; row++ {
					i := start + row
					k := (i * 37) % 13
					a.Values[row] = int64(i%31 - 15)
					v.Values[row] = int64(i%23 - 11)
					if i%3 != 0 {
						memory.Set(a.Validity, row)
					}
					if i%5 != 0 {
						memory.Set(v.Validity, row)
					}
					if k != 0 {
						memory.Set(validity, row)
					}
					i64[row] = int64(k) - 7
					u32[row] = uint32(k)
					j.Values[row] = uint32((i / 13) % 3)
					if i%7 != 0 {
						memory.Set(j.Validity, row)
					}
					s := fmt.Sprint(k)
					if k == 1 {
						s = ""
					}
					if k == 2 {
						s = "a\x00b"
					}
					data = append(data, s...)
					offsets[row+1] = uint32(len(data))
				}
				b.Int64s["a"], b.Int64s["b"] = a, v
				switch typ {
				case memory.TypeInt64:
					b.Int64s["k"] = &memory.Int64Vector{Values: i64, Validity: validity}
				case memory.TypeUint32:
					b.Uint32s["k"] = &memory.Uint32Vector{Values: u32, Validity: validity}
				case memory.TypeString:
					if shape == "dictionary" {
						dictionaryOffsets := []uint32{0}
						var dictionaryData []byte
						for k := 0; k < 13; k++ {
							dictionaryData = append(dictionaryData, fmt.Sprint(k)...)
							dictionaryOffsets = append(dictionaryOffsets, uint32(len(dictionaryData)))
						}
						b.DictStrings["k"] = &memory.DictionaryStringVector{Indices: u32, Offsets: dictionaryOffsets, Data: dictionaryData, Validity: validity, DictionaryDomain: "review-domain"}
					} else {
						b.Strings["k"] = &memory.StringViewVector{Offsets: offsets, Data: data, Validity: validity}
					}
				}
				if shape == "pair" {
					b.Uint32s["j"] = j
				}
				batches = append(batches, b)
			}
			src, err := inmemory.New(memory.NewSchema(fields...), batches...)
			if err != nil {
				t.Fatal(err)
			}
			if shape == "dictionary" {
				d := batches[0].DictStrings["k"]
				src.WithDictionary("k", source.Dictionary{Domain: d.DictionaryDomain, Offsets: d.Offsets, Data: d.Data})
			}
			var baseline []string
			for _, strategy := range []string{"bitmap-direct", "partitioned-hash", "spill"} {
				opts := streamTestOptions()
				opts.Workers = 4
				opts.MemoryLimitBytes = 1 << 20
				q := expr.Query{GroupBy: keys, Filter: expr.Col("a").Gt(expr.I64(-8)), Aggregates: []expr.Aggregate{expr.Count("rows"), expr.CountColumn("b", "cb"), expr.Min("a", "ma"), expr.Max("b", "xb"), expr.Avg("a", "av")}, Hints: expr.Hints{Cardinality: 8}}
				if strategy == "spill" {
					opts.ExecutionPolicy = ExecutionBounded
					opts.QueryMemorySoftLimitBytes = 24 << 10
					opts.Spill = SpillOptions{Directory: t.TempDir(), MaxBytes: 16 << 20}
				} else {
					q.Hints.ForceStrategy = strategy
				}
				e := New(opts)
				result, err := e.Execute(context.Background(), src, q, ExecuteOptions{Materialize: true})
				if err != nil {
					e.Close()
					t.Fatalf("%s: %v", strategy, err)
				}
				var got []string
				for _, row := range result.Rows {
					got = append(got, fmt.Sprintf("%+v:%+v", row.Keys, row.Aggregates))
				}
				sort.Strings(got)
				if baseline == nil {
					baseline = got
				} else if !reflect.DeepEqual(got, baseline) {
					t.Fatalf("%s differs from resident\ngot=%v\nwant=%v", strategy, got, baseline)
				}
				e.Close()
			}
		})
	}
}

func TestMultiAggregateReviewSortedOneRowChunks(t *testing.T) {
	src, want := multiAggregateFixture(t, 1027)
	src.WithOrdering(source.OrderingColumn{Name: "key"})
	e := New(streamTestOptions())
	defer e.Close()
	q := multiAggregateQuery()
	q.Hints.ForceStrategy = "sorted"
	p, err := e.Prepare(context.Background(), src, q, PrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	budget, err := e.BeginQueryMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	stream, err := p.NewSortedAggregateStream(memory.WithQueryBudget(context.Background(), budget), p.resultSchema(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	out := memory.NewBatch()
	seen := 0
	for {
		ok, err := stream.Next(context.Background(), out)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if out.Rows != 1 {
			t.Fatal(out.Rows)
		}
		k := int64(out.Int32s["key"].Values[0])
		if !out.Int32s["key"].IsValid(0) {
			k = 0
		}
		st := want[k]
		if out.Int64s["sum_a"].Values[0] != st[0].Sum || out.Int64s["sum_b"].Values[0] != st[1].Sum || out.Float64s["avg_b"].Values[0] != st[1].Avg {
			t.Fatalf("wrong state at key%d", k)
		}
		seen++
	}
	if seen != len(want) || budget.Used() != 0 {
		t.Fatalf("seen%d want%d used%d", seen, len(want), budget.Used())
	}
}
