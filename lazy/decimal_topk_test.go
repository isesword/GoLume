package lazy

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/physical"
	"gocolumn/source/arrowrecord"
	"path/filepath"
	"sort"
	"testing"
)

// Independent big.Int oracle is shared with N10's fixture, never production Sort.
func TestDecimalTopKStrategies(t *testing.T) {
	for _, ps := range [][2]int32{{1, 0}, {38, 0}, {38, 38}} {
		for _, stable := range []bool{false, true} {
			for _, k := range []int{0, 1, 63, 64, 65, 1000000} {
				t.Run(fmt.Sprintf("p%d-s%d-stable%v-k%d", ps[0], ps[1], stable, k), func(t *testing.T) {
					rec := decimalResidentRecord(t, ps[0], ps[1])
					defer rec.Release()
					for _, multi := range []bool{false, true} {
						for _, direction := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
							desc, first := direction[0], direction[1]
							payload, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeNested: true, NativeDecimal128: true}, rec)
							if e != nil {
								t.Fatal(e)
							}
							// Narrow key input has different batch boundaries from payload replay.
							index := int(rec.NumCols()) - 1
							kr := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{rec.Schema().Field(index), rec.Schema().Field(rec.Schema().FieldIndices("seq")[0])}, nil), []arrow.Array{rec.Column(index), rec.Column(rec.Schema().FieldIndices("seq")[0])}, rec.NumRows())
							key, e := arrowrecord.NewSourceWithOptions(arrowrecord.Options{NativeDecimal128: true}, kr)
							kr.Release()
							if e != nil {
								t.Fatal(e)
							}
							p := &nestedTopKObserved{Source: payload, stable: stable, batchRows: 13}
							ks := &nestedTopKObserved{Source: key, stable: true, forbidden: !stable, batchRows: 7}
							opts := engine.DefaultOptions()
							opts.MemoryLimitBytes = 64 << 20
							opts.PreparedCacheBytes = 0
							s := NewSession(opts)
							keys := []physical.SortKey{{Column: "amount", Descending: desc, NullsLast: !first}}
							if multi {
								keys = append(keys, physical.SortKey{Column: "seq", Descending: true})
							}
							top, e := physical.NewTopKSourceWithKeyInput(p, ks, k, keys, nil)
							if e != nil {
								t.Fatal(e)
							}
							df, e := s.FromSource(top).Collect(context.Background())
							if e != nil {
								t.Fatal(e)
							}
							order := decimalResidentOrder(ps[0], desc, first)
							if multi {
								sort.SliceStable(order, func(i, j int) bool {
									a, b := order[i], order[j]
									an, bn := a%7 == 0, b%7 == 0
									if an != bn {
										return an == first
									}
									if !an {
										cmp := decimalResidentWant(a, ps[0]).Cmp(decimalResidentWant(b, ps[0]))
										if cmp != 0 {
											if desc {
												return cmp > 0
											}
											return cmp < 0
										}
									}
									return a > b
								})
							}
							n := k
							if n > 65 {
								n = 65
							}
							decimalResidentCheck(t, df, ps[0], ps[1], order[:n])
							if stable {
								if p.opens > 1 || ks.opens > 1 || (k > 0 && (p.opens != 1 || ks.opens != 1)) {
									t.Fatal("replay route", p.opens, ks.opens)
								}
							} else if p.opens > 1 || ks.opens != 0 || (k > 0 && p.opens != 1) {
								t.Fatal("onepass route", p.opens, ks.opens)
							}
							if p.opens != p.closes || ks.opens != ks.closes {
								t.Fatal("unclosed source")
							}
							payload.Close()
							key.Close()
							s.Close()
							decimalResidentCheck(t, df, ps[0], ps[1], order[:n])
							df.Close()
							if s.Engine().Memory().Used() != 0 {
								t.Fatal("leak")
							}
						}

					}
				})
			}
		}
	}
}

func TestDecimalTopKNativeParquetRoundtrip(t *testing.T) {
	opts := engine.DefaultOptions()
	opts.MemoryLimitBytes = 64 << 20
	opts.PreparedCacheBytes = 0
	opts.Workers = 3
	s := NewSession(opts)
	defer s.Close()
	r := decimalResidentRecord(t, 38, 38)
	defer r.Release()
	q, e := s.ScanArrowWithOptions(ArrowScanOptions{NativeNested: true, NativeDecimal128: true}, r)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "input.parquet")
	if _, e = q.SinkParquet(context.Background(), path, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	read, e := s.ScanParquetWithOptions(path, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	top := read.WithColumns(expr.Add(expr.Col("amount"), expr.Decimal128(decimal128.FromI64(0), 1, 0)).Alias("amount")).TopK(64, Desc("amount"))
	want := decimalResidentOrder(38, true, false)[:64]
	df, e := top.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	decimalResidentCheck(t, df, 38, 38, want)
	df.Close()
	if _, e = top.SinkIPC(context.Background(), filepath.Join(t.TempDir(), "top.arrow"), IPCSinkOptions{}); e != nil {
		t.Fatal(e)
	}
	out := filepath.Join(t.TempDir(), "top.parquet")
	if _, e = top.SinkParquet(context.Background(), out, ParquetSinkOptions{RowGroupRows: 17}); e != nil {
		t.Fatal(e)
	}
	again, e := s.ScanParquetWithOptions(out, ParquetScanOptions{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	df, e = again.Collect(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	decimalResidentCheck(t, df, 38, 38, want)
	df.Close()
	if s.Engine().Memory().Used() != 0 {
		t.Fatal("IO retained")
	}
}
