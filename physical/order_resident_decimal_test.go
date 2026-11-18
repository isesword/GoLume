package physical

import (
	"context"
	"errors"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
	"gocolumn/scheduler"
	"gocolumn/source"
	"math/big"
	"runtime"
	"strings"
	"testing"
)

func n10Input(t *testing.T) (memory.Schema, *memory.Batch) {
	s, b := n8Input(t, 8)
	f, e := memory.Decimal128Field("decimal", true, 38, 9)
	if e != nil {
		t.Fatal(e)
	}
	s = memory.NewSchema(append(s.Fields, f)...)
	texts := []string{"-99999999999999999999999999999999999999", "9007199254740993", "9007199254740992", "99999999999999999999999999999999999999"}
	v := &memory.Decimal128Vector{Precision: 38, Scale: 9, Validity: memory.Bitmap{7}}
	for _, text := range texts {
		n, _ := new(big.Int).SetString(text, 10)
		v.Values = append(v.Values, decimal128.FromBigInt(n))
	}
	b.Decimals["decimal"] = v
	return s, b
}
func TestN10DecimalResidentExactParallel(t *testing.T) {
	pool := scheduler.NewPool(3)
	defer pool.Close()
	for _, workers := range []int{1, 3} {
		for _, desc := range []bool{false, true} {
			for _, last := range []bool{false, true} {
				s, b := n10Input(t)
				input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, b}}}
				m := memory.NewBudgetManager(8 << 20)
				o := SortSourceOptions{Workers: workers}
				if workers > 1 {
					o.Executor = pool
				}
				sorted, e := NewSortSourceWithOptions(input, []SortKey{{Column: "decimal", Descending: desc, NullsLast: last}}, m, o)
				if e != nil {
					t.Fatal(e)
				}
				stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
					t.Fatal(ok, e)
				}
				rows := []int{0, 0, 2, 2, 1, 1}
				if desc {
					rows = []int{1, 1, 2, 2, 0, 0}
				}
				if last {
					rows = append(rows, 3, 3)
				} else {
					rows = append([]int{3, 3}, rows...)
				}
				nestedGatherCheck(t, out, 8, rows)
				v := out.Decimals["decimal"]
				for i, r := range rows {
					if v.IsValid(i) != (r != 3) || (r != 3 && v.Values[i] != b.Decimals["decimal"].Values[r]) {
						t.Fatal("decimal payload", i, r)
					}
				}
				if v.Precision != 38 || v.Scale != 9 || input.closed != 1 {
					t.Fatal("metadata/lifetime")
				}
				stream.Close()
				if len(v.Values) != 0 || m.Used() != 0 {
					t.Fatal("borrow lifetime", m.Used())
				}
			}
		}
	}
}
func n10Run(t *testing.T, m memory.Manager) error {
	s, b := n10Input(t)
	input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, b}}}
	sorted, e := NewSortSource(input, []SortKey{{Column: "decimal"}}, m)
	if e != nil {
		return e
	}
	stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
	if e != nil {
		return e
	}
	_, e = stream.Next(context.Background(), memory.NewBatch())
	stream.Close()
	return e
}
func TestN10EveryReserveExact(t *testing.T) {
	m := &n8Manager{Manager: memory.NewBudgetManager(0)}
	if e := n10Run(t, m); e != nil {
		t.Fatal(e)
	}
	t.Logf("trace=%v peak=%d", m.trace, m.peak)
	for fail := 1; fail <= m.calls; fail++ {
		a := &n8Manager{Manager: memory.NewBudgetManager(0), fail: fail}
		if e := n10Run(t, a); !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(fail, e)
		}
		if a.Used() != 0 {
			t.Fatal("leak", fail, a.Used())
		}
	}
	for _, limit := range []int64{m.peak, m.peak - 1} {
		a := &n8Manager{Manager: memory.NewBudgetManager(limit)}
		e := n10Run(t, a)
		if limit == m.peak && e != nil || limit < m.peak && !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(limit, e)
		}
		if a.Used() != 0 {
			t.Fatal("leak", a.Used())
		}
	}
}
func TestN10DecimalBatchValidation(t *testing.T) {
	for _, kind := range []string{"precision", "scale", "short", "validity", "coefficient", "opaque"} {
		t.Run(kind, func(t *testing.T) {
			s, b := n10Input(t)
			_, bad := n10Input(t)
			v := bad.Decimals["decimal"]
			switch kind {
			case "precision":
				v.Precision = 37
			case "scale":
				v.Scale = 8
			case "short":
				v.Values = v.Values[:3]
			case "validity":
				v.Validity = memory.Bitmap{}
			case "coefficient":
				v.Values[0] = decimal128.New(0x7fffffffffffffff, 0xffffffffffffffff)
			case "opaque":
				delete(bad.Decimals, "decimal")
			}
			input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b, bad}}}
			m := memory.NewBudgetManager(8 << 20)
			sorted, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			if ok, e := stream.Next(context.Background(), memory.NewBatch()); ok || e == nil {
				t.Fatal(ok, e)
			}
			stream.Close()
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
		})
	}
	s, b := n10Input(t)
	b.Decimals["decimal"].Values[3] = decimal128.New(0x7fffffffffffffff, 0xffffffffffffffff)
	input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
	if e := validateResidentDecimalBatch(context.Background(), s, b); e != nil {
		t.Fatal("NULL coefficient rejected", e)
	}
	s.Fields[len(s.Fields)-1].DecimalScale = 39
	input.schema = memory.NewSchema(s.Fields...)
	if _, e := NewSortSource(input, []SortKey{{Column: "sortkey"}}, nil); e == nil {
		t.Fatal("bad empty schema accepted")
	}
}

type n10TrackManager struct {
	memory.Manager
	last int64
}

func (m *n10TrackManager) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.last = n
	return m.Manager.Reserve(ctx, n)
}

type n10CancelContext struct {
	context.Context
	stage  string
	m      *n10TrackManager
	checks int
	sticky bool
}

func (c *n10CancelContext) Err() error {
	if c.sticky {
		return context.Canceled
	}
	var pcs [40]uintptr
	n := runtime.Callers(2, pcs[:])
	fs := runtime.CallersFrames(pcs[:n])
	first, more := fs.Next()
	match := (c.stage == "validate" && strings.Contains(first.Function, "validateResidentDecimalBatch")) || (c.stage == "backfill" && strings.Contains(first.Function, "externalSortAccumulator).appendValidity"))
	if c.stage == "copy" && strings.Contains(first.Function, "externalSortGrowBuffer") && c.m.last >= 131072 {
		for more {
			f, next := fs.Next()
			more = next
			if strings.Contains(f.Function, "newResidentExtendedSorted") {
				match = true
			}
		}
	}
	if match {
		c.checks++
		target := 3
		if c.stage == "validate" {
			target = 5
		}
		if c.checks == target {
			c.sticky = true
			return context.Canceled
		}
	}
	return c.Context.Err()
}
func TestN10ValidationAndOwnedCopyCancellation(t *testing.T) {
	for _, stage := range []string{"validate", "copy"} {
		t.Run(stage, func(t *testing.T) {
			f, _ := memory.Decimal128Field("d", true, 38, 0)
			s := memory.NewSchema(f)
			b := memory.NewBatch()
			b.Rows = 16384
			v := &memory.Decimal128Vector{Precision: 38, Values: make([]memory.Decimal128, b.Rows)}
			for i := range v.Values {
				v.Values[i] = decimal128.FromI64(int64(b.Rows - i))
			}
			b.Decimals["d"] = v
			input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
			m := &n10TrackManager{Manager: memory.NewBudgetManager(8 << 20)}
			ctx := &n10CancelContext{Context: context.Background(), stage: stage, m: m}
			sorted, e := NewSortSource(input, []SortKey{{Column: "d"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(ctx, sorted, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			wantChecks := 3
			if stage == "validate" {
				wantChecks = 5
			}
			if ok, e := stream.Next(ctx, memory.NewBatch()); ok || !errors.Is(e, context.Canceled) || ctx.checks != wantChecks {
				t.Fatal(ok, e, ctx.checks)
			}
			stream.Close()
			if m.Used() != 0 || v.Values[0] != decimal128.FromI64(16384) {
				t.Fatal("owner/source lifetime", m.Used())
			}
		})
	}
}
func TestN10MixedDictionaryOpaqueAndDecimalPayload(t *testing.T) {
	for _, opaque := range []bool{false, true} {
		s, b := n9MixedFixture(t)
		if !opaque {
			delete(b.Opaque, "opaque")
			s = memory.NewSchema(s.Fields[:len(s.Fields)-1]...)
		}
		f, _ := memory.Decimal128Field("d", true, 9, 2)
		s = memory.NewSchema(append(s.Fields, f)...)
		b.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(20), decimal128.FromI64(-10), decimal128.FromI64(30), decimal128.FromI64(0)}, Precision: 9, Scale: 2}
		input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
		m := memory.NewBudgetManager(8 << 20)
		sorted, e := NewSortSource(input, []SortKey{{Column: "d"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		rows := []int{1, 3, 0, 2}
		out := memory.NewBatch()
		seen := 0
		for {
			ok, e := stream.Next(context.Background(), out)
			if e != nil {
				t.Fatal(e)
			}
			if !ok {
				break
			}
			nestedGatherCheck(t, out, 8, rows[seen:seen+out.Rows])
			v := out.Decimals["d"]
			for i := 0; i < out.Rows; i++ {
				if v.Values[i] != b.Decimals["d"].Values[rows[seen+i]] {
					t.Fatal("decimal")
				}
			}
			seen += out.Rows
		}
		stream.Close()
		if seen != 4 || m.Used() != 0 {
			t.Fatal(seen, m.Used())
		}
	}
}

func TestN10MultipleDecimalKeysAndZeroAllocCompare(t *testing.T) {
	s, b := n10Input(t)
	v := b.Decimals["decimal"]
	a := decimal128.FromI64(9007199254740993)
	z := decimal128.FromI64(9007199254740992)
	v.Values = []memory.Decimal128{a, a, z, z}
	v.Validity = nil
	f, _ := memory.Decimal128Field("small", false, 9, 2)
	s = memory.NewSchema(append(s.Fields, f)...)
	b.Decimals["small"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(100), decimal128.FromI64(200), decimal128.FromI64(100), decimal128.FromI64(100)}, Precision: 9, Scale: 2}
	keys := []SortKey{{Column: "decimal"}, {Column: "small", Descending: true}, {Column: "sortkey"}}
	cmp, e := newBatchComparator(b, s, keys)
	if e != nil {
		t.Fatal(e)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for i := 0; i < 4; i++ {
			for j := 0; j < 4; j++ {
				_ = cmp.less(i, j)
			}
		}
	}); allocs != 0 {
		t.Fatal("compare allocated", allocs)
	}
	for _, payloadOnly := range []bool{false, true} {
		use := keys
		want := []int{3, 2, 1, 0}
		if payloadOnly {
			use = []SortKey{{Column: "sortkey"}}
			want = []int{3, 1, 2, 0}
		}
		input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
		m := memory.NewBudgetManager(8 << 20)
		sorted, e := NewSortSource(input, use, m)
		if e != nil {
			t.Fatal(e)
		}
		stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		out := memory.NewBatch()
		if ok, e := stream.Next(context.Background(), out); !ok || e != nil {
			t.Fatal(ok, e)
		}
		nestedGatherCheck(t, out, 8, want)
		for i, r := range want {
			if out.Decimals["small"].Values[i] != b.Decimals["small"].Values[r] || out.Decimals["decimal"].Values[i] != v.Values[r] {
				t.Fatal("metadata-key order")
			}
		}
		stream.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}
func TestN10ZeroOneRowsValidateBeforeComparator(t *testing.T) {
	for _, rows := range []int{0, 1} {
		f, _ := memory.Decimal128Field("d", true, 9, 2)
		s := memory.NewSchema(f)
		b := memory.NewBatch()
		b.Rows = rows
		b.Decimals["d"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Precision: 9, Scale: 1}
		input := &n9Source{n8Source: n8Source{schema: s, batches: []*memory.Batch{b}}}
		m := memory.NewBudgetManager(1 << 20)
		sorted, e := NewSortSource(input, []SortKey{{Column: "d"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := stream.Next(context.Background(), memory.NewBatch()); ok || e == nil {
			t.Fatal(rows, ok, e)
		}
		stream.Close()
		if m.Used() != 0 {
			t.Fatal("leak", m.Used())
		}
	}
}

func TestN10NonNullableDecimalRejectsCustomSourceNULL(t *testing.T) {
	for _, rows := range []int{0, 1, 4} {
		for _, second := range []bool{false, true} {
			f, _ := memory.Decimal128Field("d", false, 9, 2)
			schema := memory.NewSchema(f)
			bad := memory.NewBatch()
			bad.Rows = rows
			bad.Decimals["d"] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Precision: 9, Scale: 2, Validity: make(memory.Bitmap, memory.WordsFor(rows))}
			batches := []*memory.Batch{bad}
			if second {
				good := memory.NewBatch()
				good.Rows = 1
				good.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(1)}, Precision: 9, Scale: 2}
				batches = append([]*memory.Batch{good}, batches...)
			}
			input := &n9Source{n8Source: n8Source{schema: schema, batches: batches}}
			m := memory.NewBudgetManager(8 << 20)
			sorted, e := NewSortSource(input, []SortKey{{Column: "d"}}, m)
			if e != nil {
				t.Fatal(e)
			}
			stream, e := source.OpenBatchStream(context.Background(), sorted, testReadOptions())
			if e != nil {
				t.Fatal(e)
			}
			ok, e := stream.Next(context.Background(), memory.NewBatch())
			stream.Close()
			if rows > 0 {
				if ok || e == nil || !strings.Contains(e.Error(), "non-nullable") {
					t.Fatal(rows, second, ok, e)
				}
			} else if e != nil {
				t.Fatal("zero-row nonnullable field rejected", e)
			}
			if m.Used() != 0 {
				t.Fatal("leak", m.Used())
			}
		}
	}
}

func TestN10LateDecimalValidityBackfillCancellation(t *testing.T) {
	f, _ := memory.Decimal128Field("d", true, 9, 2)
	s := memory.NewSchema(f)
	b := memory.NewBatch()
	b.Rows = 1
	b.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(1)}, Precision: 9, Scale: 2}
	m := &n10TrackManager{Manager: memory.NewBudgetManager(1 << 20)}
	w := &sortWorkspace{parent: m}
	a, e := newResidentSortAccumulator(context.Background(), s, w, b)
	if e != nil {
		t.Fatal(e)
	}
	ctx := &n10CancelContext{Context: context.Background(), stage: "backfill", m: m}
	var bits memory.Bitmap
	e = a.appendValidity(ctx, &bits, false, false, 64*8192)
	if !errors.Is(e, context.Canceled) || ctx.checks != 3 || len(bits) != 8193 || bits[0] != ^uint64(0) || bits[8192] != 0 {
		t.Fatal(e, ctx.checks, len(bits))
	}
	a.close()
	w.close()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
}

type n10FieldCancelContext struct {
	context.Context
	calls int
}

func (c *n10FieldCancelContext) Err() error {
	c.calls++
	if c.calls >= 3 {
		return context.Canceled
	}
	return nil
}
func TestN10EmptyWideDecimalFieldCancellation(t *testing.T) {
	fields := make([]memory.Field, 3)
	b := memory.NewBatch()
	for i := range fields {
		name := strings.Repeat("d", i+1)
		fields[i], _ = memory.Decimal128Field(name, true, 9, 2)
		b.Decimals[name] = &memory.Decimal128Vector{Precision: 9, Scale: 2}
	}
	ctx := &n10FieldCancelContext{Context: context.Background()}
	if e := validateResidentDecimalBatch(ctx, memory.NewSchema(fields...), b); !errors.Is(e, context.Canceled) || ctx.calls != 3 {
		t.Fatal(e, ctx.calls)
	}
}
