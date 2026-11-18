package physical

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	"gocolumn/memory"
	"gocolumn/source"
	"gocolumn/source/inmemory"
	"math"
	"math/big"
	"testing"
)

// Keep the old unkeyed public expression literal source-compatible.
var ntileReviewOldExpression = WindowExpression{WindowRank, "", WindowFrame{}, "rank"}

func TestNTileReviewExtremeArithmetic(t *testing.T) {
	for _, n := range []int64{1, 2, 7, 19, math.MaxInt64 - 1, math.MaxInt64} {
		for _, b := range []int64{1, 2, 3, 7, math.MaxInt64 - 1, math.MaxInt64} {
			for _, p := range []int64{0, n / 2, n - 1} {
				// Independent arbitrary-precision boundary arithmetic, not the production helper.
				N, B, P := big.NewInt(n), big.NewInt(b), big.NewInt(p)
				q, r := new(big.Int), new(big.Int)
				q.QuoRem(N, B, r)
				size := new(big.Int).Add(q, big.NewInt(1))
				cut := new(big.Int).Mul(r, size)
				want := new(big.Int)
				if P.Cmp(cut) < 0 {
					want.Quo(P, size).Add(want, big.NewInt(1))
				} else {
					want.Sub(P, cut).Quo(want, q).Add(want, r).Add(want, big.NewInt(1))
				}
				got, err := windowNTileValue(p, n, b)
				if err != nil || got != want.Int64() {
					t.Fatalf("p=%d n=%d b=%d got=%d err=%v want=%s", p, n, b, got, err, want)
				}
			}
		}
	}
	for _, v := range [][3]int64{{0, 0, 1}, {-1, 1, 1}, {1, 1, 1}, {0, 1, 0}, {0, 1, -1}} {
		if _, err := windowNTileValue(v[0], v[1], v[2]); err == nil {
			t.Fatal("accepted invalid", v)
		}
	}
}

func TestNTileReviewConfiguredValidation(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, kind := range []string{"zero", "negative", "column", "frameoffset", "ordinarybuckets", "nullable", "encoding", "legacy"} {
			t.Run(fmt.Sprintf("%d/%s", mode, kind), func(t *testing.T) {
				in := percentRankReviewInput(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
				_, q := percentRankReviewQuery(t, 8<<20, true)
				e := ConfiguredWindowExpression{Expr: WindowExpression{Function: WindowNTile, Alias: "tile"}, Buckets: 3}
				f := memory.Field{Name: "tile", Type: memory.TypeInt64}
				switch kind {
				case "zero":
					e.Buckets = 0
				case "negative":
					e.Buckets = -1
				case "column":
					e.Expr.Column = "id"
				case "frameoffset":
					e.Expr.Frame.Start.Offset = 1
				case "ordinarybuckets":
					e.Expr.Function = WindowRank
				case "nullable":
					f.Nullable = true
				case "encoding":
					f.Encoding = memory.EncodingDictionary
				}
				var err error
				if kind == "legacy" {
					_, err = NewWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "key"}}, []WindowExpression{e.Expr}, []memory.Field{f}, q, WindowSourceOptions{Mode: mode})
				} else {
					_, err = NewConfiguredWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "key"}}, []ConfiguredWindowExpression{e}, []memory.Field{f}, q, WindowSourceOptions{Mode: mode})
				}
				if err == nil {
					t.Fatal("invalid configuration accepted")
				}
				if in.opens != 0 || q.Used() != 0 {
					t.Fatal("validation opened source or retained budget", in.opens, q.Used())
				}
			})
		}
	}
}

func TestNTileReviewMixedAliasesAndPeerSplits(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			order := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
			// Preserve original order within each peer while scrambling peer groups.
			if mode != WindowExecutionOrdered {
				order = []int{6, 7, 9, 10, 11, 3, 4, 8, 0, 1, 5, 2}
			}
			in := percentRankReviewInput(t, order)
			ctx, q := percentRankReviewQuery(t, 8<<20, true)
			es := []ConfiguredWindowExpression{
				{Expr: WindowExpression{Function: WindowNTile, Alias: "key"}, Buckets: 3},
				{Expr: ntileReviewOldExpression},
				{Expr: WindowExpression{Function: WindowNTile, Alias: "__golume_ntile"}, Buckets: 3},
				{Expr: WindowExpression{Function: WindowNTile, Alias: "huge"}, Buckets: math.MaxInt64},
				{Expr: WindowExpression{Function: WindowCumeDist, Alias: "cd"}},
			}
			fs := []memory.Field{{Name: "key", Type: memory.TypeInt64}, {Name: "rank", Type: memory.TypeInt64}, {Name: "__golume_ntile", Type: memory.TypeInt64}, {Name: "huge", Type: memory.TypeInt64}, {Name: "cd", Type: memory.TypeFloat64}}
			win, err := NewConfiguredWindowSourceWithOptions(in, []string{"g"}, []SortKey{{Column: "key"}}, es, fs, q, WindowSourceOptions{Mode: mode})
			if err != nil {
				t.Fatal(err)
			}
			// Caller mutation of configuration arrays must not alter the prepared source.
			es[0].Buckets = 1
			es[2].Expr.Alias = "corrupt"
			fs[0].Name = "corrupt"
			st, err := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			tiles := []int64{1, 1, 1, 2, 2, 2, 3, 3, 1, 1, 2, 3}
			positions := []int64{1, 2, 3, 4, 5, 6, 7, 8, 1, 1, 2, 3}
			ranks := []int64{1, 1, 3, 4, 4, 6, 7, 7, 1, 1, 1, 1}
			ends := []int64{2, 2, 3, 5, 5, 6, 8, 8, 1, 3, 3, 3}
			seen := 0
			for {
				out := memory.NewBatch()
				ok, e := st.Next(ctx, out)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				for row, id64 := range out.Int64s["id"].Values {
					id := int(id64)
					if seen >= len(order) || order[seen] != id {
						t.Fatal("original order", seen, id)
					}
					seen++
					for name, want := range map[string]int64{"key": tiles[id], "__golume_ntile": tiles[id], "huge": positions[id], "rank": ranks[id]} {
						v := out.Int64s[name]
						if v == nil || !v.IsValid(row) || v.Values[row] != want {
							t.Fatal("value", name, id, v, want)
						}
					}
					n := int64(8)
					if id == 8 {
						n = 1
					} else if id >= 9 {
						n = 3
					}
					if out.Float64s["cd"].Values[row] != float64(ends[id])/float64(n) {
						t.Fatal("CD must still read original key", id)
					}
				}
			}
			if seen != 12 {
				t.Fatal("rows", seen)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			if in.opens != 1 || in.closes != 1 || q.Used() != 0 {
				t.Fatal("lifecycle", in.opens, in.closes, q.Used())
			}
		})
	}
}

func TestNTileReviewFlatProfileThreePaths(t *testing.T) {
	for _, both := range []bool{false, true} {
		for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
			t.Run(fmt.Sprint(mode, "/both=", both), func(t *testing.T) {
				schema, expected := rankSpoolFlatProfile(t)
				oracle, e := memory.ArrowRecord(am.DefaultAllocator, schema, expected)
				if e != nil {
					t.Fatal(e)
				}
				defer oracle.Release()
				ids := []int{3, 0, 4, 1, 2}
				if mode == WindowExecutionOrdered {
					ids = []int{0, 1, 2, 3, 4}
				}
				var batches []*memory.Batch
				for _, id := range ids {
					b, e := memory.CloneBatchRange(expected, id, 1)
					if e != nil {
						t.Fatal(e)
					}
					batches = append(batches, b)
				}
				input, e := inmemory.New(schema, batches...)
				if e != nil {
					t.Fatal(e)
				}
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				expressions := []ConfiguredWindowExpression{{Expr: WindowExpression{Function: WindowNTile, Alias: "cd"}, Buckets: 3}}
				fields := []memory.Field{{Name: "cd", Type: memory.TypeInt64}}
				if both {
					expressions = append(expressions, ConfiguredWindowExpression{Expr: WindowExpression{Function: WindowPercentRank, Alias: "pr"}})
					fields = append(fields, memory.Field{Name: "pr", Type: memory.TypeFloat64})
				}
				win, e := NewConfiguredWindowSourceWithOptions(input, nil, []SortKey{{Column: "id"}}, expressions, fields, q, WindowSourceOptions{Mode: mode})
				if e != nil {
					t.Fatal(e)
				}
				st, e := source.OpenBatchStream(ctx, win, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				count := 0
				for {
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					rec, e := memory.ArrowRecord(am.DefaultAllocator, win.Schema(), out)
					if e != nil {
						t.Fatal(e)
					}
					for row, id := range out.Int64s["id"].Values {
						if int(id) != ids[count] {
							t.Fatal("row order", id, ids[count])
						}
						count++
						if both && (out.Float64s["pr"].Values[row] != float64(id)/4 || !out.Float64s["pr"].IsValid(row)) {
							t.Fatal("rank", id)
						}
						if out.Int64s["cd"].Values[row] != []int64{1, 1, 2, 2, 3}[id] || !out.Int64s["cd"].IsValid(row) {
							t.Fatal("cume", id)
						}
						for _, f := range fields {
							got, _ := win.Schema().Field(f.Name)
							if got.Type != f.Type || got.Nullable {
								t.Fatal("distribution metadata", got)
							}
						}
						for col, f := range schema.Fields {
							if !arrow.TypeEqual(rec.Schema().Field(col).Type, oracle.Schema().Field(col).Type) {
								t.Fatal("type", f.Name)
							}
							gotField, wantField := rec.Schema().Field(col), oracle.Schema().Field(col)
							if gotField.Name != wantField.Name || gotField.Nullable != wantField.Nullable || !gotField.Metadata.Equal(wantField.Metadata) {
								t.Fatal("metadata", f.Name)
							}
							if rec.Column(col).IsNull(row) != oracle.Column(col).IsNull(int(id)) {
								t.Fatal("NULL", f.Name, id)
							}
							actual := array.NewSlice(rec.Column(col), int64(row), int64(row+1))
							want := array.NewSlice(oracle.Column(col), id, id+1)
							equal := array.Equal(actual, want)
							actual.Release()
							want.Release()
							if !equal {
								t.Fatal("raw value", f.Name, id)
							}
						}
					}
					rec.Release()
				}
				if count != 5 {
					t.Fatal(count)
				}
				if e = st.Close(); e != nil {
					t.Fatal(e)
				}
				if q.Used() != 0 {
					t.Fatal("leak", q.Used())
				}
			})
		}
	}
}

func TestNTileReviewEmptySingleton(t *testing.T) {
	for _, mode := range []WindowExecutionMode{WindowExecutionResident, WindowExecutionOrdered, WindowExecutionExternalSort} {
		for _, rows := range []int{0, 1} {
			t.Run(fmt.Sprintf("%d/%d", mode, rows), func(t *testing.T) {
				schema := memory.NewSchema(memory.Field{Name: "id", Type: memory.TypeInt64})
				b := memory.NewBatch()
				b.Reset(rows)
				b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, rows)}
				in, err := inmemory.New(schema, b)
				if err != nil {
					t.Fatal(err)
				}
				ctx, q := percentRankReviewQuery(t, 8<<20, true)
				w, err := NewConfiguredWindowSourceWithOptions(in, nil, []SortKey{{Column: "id"}}, []ConfiguredWindowExpression{{Expr: WindowExpression{Function: WindowNTile, Alias: "tile"}, Buckets: math.MaxInt64}}, []memory.Field{{Name: "tile", Type: memory.TypeInt64}}, q, WindowSourceOptions{Mode: mode})
				if err != nil {
					t.Fatal(err)
				}
				field, ok := w.Schema().Field("tile")
				if !ok || field.Type != memory.TypeInt64 || field.Nullable {
					t.Fatal("typed result", field)
				}
				st, err := source.OpenBatchStream(ctx, w, source.ReadOptions{Kernels: offsetTestKernels(), BatchRows: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				count := 0
				for {
					out := memory.NewBatch()
					ok, e := st.Next(ctx, out)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					for i := 0; i < out.Rows; i++ {
						v := out.Int64s["tile"]
						if v == nil || !v.IsValid(i) || v.Values[i] != 1 {
							t.Fatal("singleton", v)
						}
						count++
					}
				}
				if count != rows {
					t.Fatal("cardinality", count, rows)
				}
				if err = st.Close(); err != nil {
					t.Fatal(err)
				}
				if q.Used() != 0 {
					t.Fatal("retained", q.Used())
				}
			})
		}
	}
}
