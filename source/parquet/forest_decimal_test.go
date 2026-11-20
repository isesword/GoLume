package parquet

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	am "github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForestDecimalPhysicalBasics(t *testing.T) {
	for _, tc := range []struct {
		kind  pq.Type
		width int
		p, s  int32
	}{{pq.Types.Int32, 0, 9, 0}, {pq.Types.Int64, 0, 18, 9}, {pq.Types.ByteArray, 0, 38, 38}, {pq.Types.FixedLenByteArray, 16, 38, 0}} {
		for _, nullable := range []bool{false, true} {
			t.Run(tc.kind.String()+map[bool]string{false: "/required", true: "/nullable"}[nullable], func(t *testing.T) {
				vals := decimalValues(tc.p)
				path := writeDecimalFixture(t, tc.kind, tc.width, tc.p, tc.s, nullable, []int{0, 65, 3}, vals)
				s, e := OpenFileWithOptions(path, Options{NativeNested: true})
				if e != nil {
					t.Fatal(e)
				}
				m := memory.NewBudgetManager(8 << 20)
				st, e := source.OpenBatchStream(context.Background(), s, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 7, MemoryManager: m})
				if e != nil {
					t.Fatal(e)
				}
				defer st.Close()
				b := memory.NewBatch()
				seen := 0
				for {
					ok, e := st.Next(context.Background(), b)
					if e != nil {
						t.Fatal(e)
					}
					if !ok {
						break
					}
					v := b.Decimals["amount"]
					if v == nil || len(v.Values) != b.Rows || v.Precision != tc.p || v.Scale != tc.s {
						t.Fatal("domain/metadata")
					}
					if e := v.Validate(b.Rows); e != nil {
						t.Fatal(e)
					}
					for i := 0; i < b.Rows; i++ {
						row := seen + i
						if row >= 65 {
							row -= 65
						}
						valid := !nullable || row%3 != 1
						if v.IsValid(i) != valid || valid && v.Values[i] != vals[row%len(vals)] {
							t.Fatal("physical coefficient/null oracle", seen+i)
						}
					}
					seen += b.Rows
				}
				if seen != 68 {
					t.Fatal(seen)
				}
				st.Close()
				if m.Used() != 0 {
					t.Fatal("retained", m.Used())
				}
			})
		}
	}
}

func TestForestDecimalCheckedSignExtension(t *testing.T) {
	for _, negative := range []bool{false, true} {
		data := make([]byte, 32768)
		want := decimal128.FromI64(1)
		data[len(data)-1] = 1
		if negative {
			for i := range data {
				data[i] = 255
			}
			want = decimal128.FromI64(-1)
		}
		got, e := forestDecimalFromBigEndian(data, 38, nil)
		if e != nil || got != want {
			t.Fatal(got, e)
		}
		calls := 0
		_, e = forestDecimalFromBigEndian(data, 38, func() error {
			calls++
			if calls == 4 {
				return context.Canceled
			}
			return nil
		})
		if !errors.Is(e, context.Canceled) {
			t.Fatal("mid-extension cancel", calls, e)
		}
		if n := testing.AllocsPerRun(100, func() {
			_, e := forestDecimalFromBigEndian(data, 38, nil)
			if e != nil {
				panic(e)
			}
		}); n != 0 {
			t.Fatal("conversion allocation", n)
		}
		data[4097] ^= 1
		if _, e := forestDecimalFromBigEndian(data, 38, nil); e == nil {
			t.Fatal("invalid sign extension")
		}
	}
	if _, e := forestDecimalFromBigEndian(nil, 38, nil); e == nil {
		t.Fatal("empty coefficient")
	}
}

func TestForestDecimalPrefixCommit(t *testing.T) {
	// A variable-width sibling crosses the target after one row. The pending
	// next String must not consume the next fixed Decimal before prefix flush.
	sc := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}, {Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true}, {Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, sc)
	defer rb.Release()
	for i := 0; i < 65; i++ {
		rb.Field(0).(*array.StringBuilder).Append(strings.Repeat("x", 700+i))
		lb := rb.Field(2).(*array.ListBuilder)
		lb.Append(true)
		for j := 0; j < i%3*20; j++ {
			lb.ValueBuilder().(*array.Int64Builder).Append(int64(i*100 + j))
		}
		d := rb.Field(1).(*array.Decimal128Builder)
		if i%7 == 0 {
			d.AppendNull()
		} else {
			d.Append(decimal128.FromI64(int64(10000 + i)))
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "prefix.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := pqarrow.NewFileWriter(sc, f, pq.NewWriterProperties(pq.WithDataPageSize(128)), pqarrow.DefaultWriterProps())
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Write(rec); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	s, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := memory.NewBudgetManager(4 << 20)
	st, e := source.OpenBatchStream(context.Background(), s, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 64, BatchBytes: 128 << 10, BatchPayloadTargetBytes: 1200, MemoryManager: m})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	seen, pages := 0, 0
	for {
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		pages++
		d := b.Decimals["amount"]
		if len(d.Values) != b.Rows || b.Rows != 1 {
			t.Fatal("prefix domain", len(d.Values), b.Rows)
		}
		for i := 0; i < b.Rows; i++ {
			valid := seen%7 != 0
			if d.IsValid(i) != valid || valid && d.Values[i] != decimal128.FromI64(int64(10000+seen)) {
				t.Fatal("fixed sibling advanced before commit", seen)
			}
			if len(b.Strings["text"].ValueBytes(i)) != 700+seen {
				t.Fatal("string row sync", seen)
			}
			lv := b.Int64Lists["items"]
			lo, hi := int(lv.Offsets[i]), int(lv.Offsets[i+1])
			if hi-lo != seen%3*20 {
				t.Fatal("list row sync", seen)
			}
			for j := lo; j < hi; j++ {
				if lv.Values.Values[j] != int64(seen*100+j-lo) {
					t.Fatal("list value sync", seen)
				}
			}
			seen++
		}
	}
	st.Close()
	if seen != 65 || pages != 65 || m.Used() != 0 {
		t.Fatal(seen, pages, m.Used())
	}
}

type forestDecimalRejectTrace struct {
	memory.Manager
	denied   int
	peak     int64
	failures [][2]int64
}

func (m *forestDecimalRejectTrace) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	r, e := m.Manager.Reserve(ctx, n)
	if m.Used() > m.peak {
		m.peak = m.Used()
	}
	if errors.Is(e, memory.ErrBudgetExceeded) {
		m.denied++
		m.failures = append(m.failures, [2]int64{m.Used(), n})
	}
	return r, e
}
func TestForestDecimalManagerPrefixRetry(t *testing.T) {
	// Real manager admission failures, not the logical target, must flush a
	// committed prefix and retry the pending repeated row without advancing Decimal.
	sc := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}, {Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true}, {Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}}, nil)
	rb := array.NewRecordBuilder(am.DefaultAllocator, sc)
	defer rb.Release()
	for i := 0; i < 65; i++ {
		rb.Field(0).(*array.StringBuilder).Append(strings.Repeat("x", 700+i))
		lb := rb.Field(2).(*array.ListBuilder)
		lb.Append(true)
		for j := 0; j < i%3*20; j++ {
			lb.ValueBuilder().(*array.Int64Builder).Append(int64(i*100 + j))
		}
		d := rb.Field(1).(*array.Decimal128Builder)
		if i%7 == 0 {
			d.AppendNull()
		} else {
			d.Append(decimal128.FromI64(int64(10000 + i)))
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "prefix.parquet")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	w, e := pqarrow.NewFileWriter(sc, f, pq.NewWriterProperties(pq.WithDataPageSize(128)), pqarrow.DefaultWriterProps())
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Write(rec); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	s, e := OpenFileWithOptions(path, Options{NativeNested: true})
	if e != nil {
		t.Fatal(e)
	}
	m := &forestDecimalRejectTrace{Manager: memory.NewBudgetManager(64 << 10)}
	st, e := source.OpenBatchStream(context.Background(), s, source.ReadOptions{Kernels: kernel.New(kernel.ModeScalar), BatchRows: 64, BatchBytes: 128 << 10, BatchPayloadTargetBytes: 1 << 20, MemoryManager: m})
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	b := memory.NewBatch()
	seen, pages := 0, 0
	for {
		ok, e := st.Next(context.Background(), b)
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			break
		}
		pages++
		t.Logf("page=%d rows=%d priorRows=%d denials=%d used=%d", pages, b.Rows, seen, m.denied, m.Used())
		d := b.Decimals["amount"]
		if len(d.Values) != b.Rows || b.Rows < 1 {
			t.Fatal("prefix domain", len(d.Values), b.Rows)
		}
		for i := 0; i < b.Rows; i++ {
			valid := seen%7 != 0
			if d.IsValid(i) != valid || valid && d.Values[i] != decimal128.FromI64(int64(10000+seen)) {
				t.Fatal("fixed sibling advanced before commit", seen)
			}
			if len(b.Strings["text"].ValueBytes(i)) != 700+seen {
				t.Fatal("string row sync", seen)
			}
			lv := b.Int64Lists["items"]
			lo, hi := int(lv.Offsets[i]), int(lv.Offsets[i+1])
			if hi-lo != seen%3*20 {
				t.Fatal("list row sync", seen)
			}
			for j := lo; j < hi; j++ {
				if lv.Values.Values[j] != int64(seen*100+j-lo) {
					t.Fatal("list value sync", seen)
				}
			}
			seen++
		}
	}
	st.Close()
	t.Logf("manager peak=%d denied=%d failures=%v", m.peak, m.denied, m.failures)
	if seen != 65 || pages < 2 || m.denied == 0 || m.Used() != 0 {
		t.Fatal(seen, pages, m.Used())
	}
}
