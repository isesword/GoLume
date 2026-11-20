package parquet

import (
	"context"
	"errors"
	"fmt"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"testing"
)

func TestN11RootDecimalPhysicalAcceptance(t *testing.T) {
	for _, tc := range []struct {
		typ   pq.Type
		width int
		p, s  int32
	}{
		{pq.Types.Int32, -1, 1, 0}, {pq.Types.Int32, -1, 9, 9}, {pq.Types.Int64, -1, 18, 4},
		{pq.Types.ByteArray, -1, 38, 0}, {pq.Types.FixedLenByteArray, 16, 38, 38}, {pq.Types.FixedLenByteArray, 32, 38, 2},
	} {
		for _, nullable := range []bool{false, true} {
			for _, dict := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/%d/%d/%v/%v", tc.typ, tc.p, tc.s, nullable, dict), func(t *testing.T) {
					values := decimalValues(tc.p)
					path := writeDecimalFixture(t, tc.typ, tc.width, tc.p, tc.s, nullable, decimalGroupRows, values, pq.WithDictionaryDefault(dict))
					src, e := OpenFileWithOptions(path, Options{NativeNested: true})
					if e != nil {
						t.Fatal(e)
					}
					for _, project := range []bool{false, true} {
						var selected source.Source = src
						if project {
							selected, _, e = src.ApplyPushdown(context.Background(), source.PushdownRequest{Projection: []string{"amount"}})
							if e != nil {
								t.Fatal(e)
							}
						}
						for _, size := range []int{1, 2, 63, 64, 65, 257} {
							opts := testReadOptions()
							opts.BatchRows = size
							opts.BatchBytes = 16384
							opts.MemoryManager = memory.NewBudgetManager(1 << 20)
							st, e := selected.(source.BatchStreamProvider).NewBatchStream(context.Background(), opts)
							if e != nil {
								t.Fatal(e)
							}
							b := memory.NewBatch()
							g, row, total := 1, 0, 0
							for {
								ok, e := st.Next(context.Background(), b)
								if e != nil {
									t.Fatal(e)
								}
								if !ok {
									break
								}
								v := b.Decimals["amount"]
								f, _ := selected.Schema().Field("amount")
								if v == nil || len(v.Values) != b.Rows || v.Precision != tc.p || v.Scale != tc.s {
									t.Fatal("exact rows/metadata", b.Rows, v)
								}
								if e := v.ValidateField(f, b.Rows); e != nil {
									t.Fatal(e)
								}
								for j := 0; j < b.Rows; j++ {
									if row == decimalGroupRows[g] {
										g++
										row = 0
									}
									valid := !nullable || g != 3 && row%3 != 1
									if v.IsValid(j) != valid || valid && v.Values[j] != values[row%len(values)] {
										t.Fatalf("g=%d row=%d j=%d", g, row, j)
									}
									row++
									total++
								}
							}
							if total != 143 {
								t.Fatal(total)
							}
							if e := st.Close(); e != nil {
								t.Fatal(e)
							}
							if opts.MemoryManager.Used() != 0 {
								t.Fatal("leak")
							}
						}
					}
				})
			}
		}
	}
}

type n11TraceManager struct {
	memory.Manager
	calls int
	fail  int
	peak  int64
	trace []int64
}

func (m *n11TraceManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.trace = append(m.trace, n)
	if m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	return r, e
}
func TestN11RootDecimalReserveLifecycle(t *testing.T) {
	path := writeDecimalFixture(t, pq.Types.ByteArray, -1, 38, 2, true, []int{73, 3, 67}, decimalValues(38), pq.WithDictionaryDefault(false))
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	run := func(limit int64, fail int, mode string) (*n11TraceManager, error) {
		m := &n11TraceManager{Manager: memory.NewBudgetManager(limit), fail: fail}
		o := testReadOptions()
		o.BatchRows = 65
		o.BatchBytes = 16384
		o.MemoryManager = m
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		st, e := src.NewBatchStream(ctx, o)
		if e != nil {
			if m.Used() != 0 {
				t.Fatal("init leak")
			}
			return m, e
		}
		b := memory.NewBatch()
		for {
			ok, err := st.Next(ctx, b)
			if err != nil {
				e = err
				_, again := st.Next(ctx, b)
				if !errors.Is(again, err) {
					t.Fatal("not sticky", err, again)
				}
				break
			}
			if !ok {
				break
			}
			if mode == "close" {
				break
			}
			if mode == "cancel" {
				cancel()
			}
		}
		st.Close()
		st.Close()
		if m.Used() != 0 {
			t.Fatal("close leak", m.Used())
		}
		return m, e
	}
	baseline, e := run(1<<20, 0, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("trace=%v peak=%d", baseline.trace, baseline.peak)
	for i := 1; i <= baseline.calls; i++ {
		m, e := run(1<<20, i, "")
		if !errors.Is(e, memory.ErrBudgetExceeded) || m.calls < i {
			t.Fatalf("ordinal %d: %v", i, e)
		}
	}
	if _, e = run(baseline.peak, 0, ""); e != nil {
		t.Fatal("exact", e)
	}
	if _, e = run(baseline.peak-1, 0, ""); !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("minus one", e)
	}
	if _, e = run(1<<20, 0, "close"); e != nil {
		t.Fatal(e)
	}
	if _, e = run(1<<20, 0, "cancel"); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

func TestN11RootDecimalActualPageEncoding(t *testing.T) {
	path := writeDecimalFixture(t, pq.Types.ByteArray, -1, 38, 2, false, []int{3}, decimalValues(38), pq.WithDictionaryDefault(false), pq.WithEncoding(pq.Encodings.DeltaByteArray))
	rewriteDecimalFooter(t, path, func(m *metadata.FileMetaData) {
		f := m.FileMetaData.Schema[1]
		reflect.ValueOf(f.Type).Elem().SetInt(int64(pq.Types.FixedLenByteArray))
		w := int32(17)
		f.TypeLength = &w
		c := m.FileMetaData.RowGroups[0].Columns[0].MetaData
		reflect.ValueOf(&c.Type).Elem().SetInt(int64(pq.Types.FixedLenByteArray))
		for i := range c.Encodings {
			if int64(c.Encodings[i]) == int64(pq.Encodings.DeltaByteArray) {
				reflect.ValueOf(&c.Encodings[i]).Elem().SetInt(int64(pq.Encodings.Plain))
			}
		}
	})
	src, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	o := testReadOptions()
	o.BatchBytes = 16384
	o.MemoryManager = memory.NewBudgetManager(1 << 20)
	st, e := src.NewBatchStream(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	ok, e := st.Next(context.Background(), b)
	if ok || e == nil {
		t.Fatal("unsupported actual encoding accepted")
	}
	if b.Rows != 0 || len(b.Decimals) != 0 || o.MemoryManager.Used() != 0 {
		t.Fatal("partial output/leak")
	}
	st.Close()
}

func TestN11RootDecimalOverflowNoPartialRow(t *testing.T) {
	for _, typ := range []pq.Type{pq.Types.Int32, pq.Types.Int64, pq.Types.ByteArray, pq.Types.FixedLenByteArray} {
		width := -1
		if typ == pq.Types.FixedLenByteArray {
			width = 16
		}
		// First coefficient succeeds, second exceeds declared precision. No prefix
		// may escape a non-budget data error, even after a row was committed internally.
		values := decimalValues(3)
		path := writeDecimalFixture(t, typ, width, 2, 1, false, []int{3}, []memory.Decimal128{values[1], values[3]}, pq.WithDictionaryDefault(false))
		src, e := OpenFileWithOptions(path, Options{NativeNested: true})
		if e != nil {
			t.Fatal(e)
		}
		o := testReadOptions()
		o.BatchRows = 3
		o.BatchBytes = 16384
		o.MemoryManager = memory.NewBudgetManager(1 << 20)
		st, e := src.NewBatchStream(context.Background(), o)
		if e != nil {
			t.Fatal(e)
		}
		b := memory.NewBatch()
		ok, e := st.Next(context.Background(), b)
		if ok || e == nil {
			t.Fatal("overflow accepted", typ)
		}
		if b.Rows != 0 || len(b.Decimals) != 0 || o.MemoryManager.Used() != 0 {
			t.Fatal("partial row or leak", typ)
		}
		_, again := st.Next(context.Background(), b)
		if !errors.Is(again, e) {
			t.Fatal("nonsticky", e, again)
		}
		st.Close()
	}
}
