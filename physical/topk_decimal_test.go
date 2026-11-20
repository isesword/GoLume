package physical

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/memory"
	"gocolumn/source"
	"reflect"
	"sort"
	"testing"
	"unsafe"
)

func n12Run(t *testing.T, m memory.Manager, stable bool, k int) error {
	schema, b := n10Input(t)
	input := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b, b}}, stable: stable}
	keys := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b, b}}, stable: stable}
	top, e := NewTopKSourceWithKeyInput(input, keys, k, []SortKey{{Column: "decimal", NullsLast: true}}, m)
	if e != nil {
		return e
	}
	st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
	if e != nil {
		return e
	}
	defer st.Close()
	out := memory.NewBatch()
	seen := 0
	want := []int{0, 0, 2, 2, 1, 1, 3, 3}
	if k < len(want) {
		want = want[:k]
	}
	for {
		ok, e := st.Next(context.Background(), out)
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		if seen+out.Rows > len(want) {
			t.Fatal("too many")
		}
		nestedGatherCheck(t, out, 8, want[seen:seen+out.Rows])
		v := out.Decimals["decimal"]
		for i := 0; i < out.Rows; i++ {
			r := want[seen+i]
			if v.IsValid(i) != (r != 3) || r != 3 && v.Values[i] != b.Decimals["decimal"].Values[r] {
				t.Fatal("decimal oracle")
			}
		}
		seen += out.Rows
	}
	if seen != len(want) {
		t.Fatal(seen)
	}
	if k == 0 {
		if input.opens != 0 || keys.opens != 0 {
			t.Fatal("K0 opened")
		}
	} else if stable {
		if input.opens != 1 || keys.opens != 1 {
			t.Fatal("replay route")
		}
	} else if input.opens != 1 || keys.opens != 0 {
		t.Fatal("volatile route")
	}
	return nil
}
func TestN12DecimalBothStrategies(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, k := range []int{0, 1, 3, 1000000} {
			m := memory.NewBudgetManager(8 << 20)
			if e := n12Run(t, m, stable, k); e != nil {
				t.Fatal(stable, k, e)
			}
			if m.Used() != 0 {
				t.Fatal(m.Used())
			}
		}
	}
}
func TestN12DecimalEveryReserveExact(t *testing.T) {
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprint(stable), func(t *testing.T) {
			m := &n8Manager{Manager: memory.NewBudgetManager(0)}
			if e := n12Run(t, m, stable, 3); e != nil {
				t.Fatal(e)
			}
			t.Logf("calls=%d peak=%d", m.calls, m.peak)
			for fail := 1; fail <= m.calls; fail++ {
				a := &n8Manager{Manager: memory.NewBudgetManager(0), fail: fail}
				if e := n12Run(t, a, stable, 3); !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(fail, e)
				}
				if a.Used() != 0 {
					t.Fatal("failure leak", fail, a.Used())
				}
			}
			for _, limit := range []int64{m.peak, m.peak - 1} {
				a := &n8Manager{Manager: memory.NewBudgetManager(limit)}
				e := n12Run(t, a, stable, 3)
				if limit == m.peak && e != nil || limit < m.peak && !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal(limit, e)
				}
				if a.Used() != 0 {
					t.Fatal("exact leak")
				}
			}
		})
	}
}
func TestN12DecimalFixedExcludedAndCellSize(t *testing.T) {
	b := memory.NewBatch()
	b.Decimals["d"] = &memory.Decimal128Vector{Values: []memory.Decimal128{decimal128.FromI64(1)}, Precision: 1}
	if _, ok := topKFixedColumns(b); ok {
		t.Fatal("Decimal routed to legacy fixed reuse")
	}
	if unsafe.Sizeof(sortCell{}) != 64 {
		t.Fatal("legacy sortCell size changed", unsafe.Sizeof(sortCell{}))
	}
}

func n12Flat(n int) (memory.Schema, *memory.Batch) {
	f, _ := memory.Decimal128Field("d", true, 38, 9)
	b := memory.NewBatch()
	b.Rows = n
	v := &memory.Decimal128Vector{Precision: 38, Scale: 9}
	for i := 0; i < n; i++ {
		v.Values = append(v.Values, decimal128.FromI64(int64(n-i)))
	}
	b.Decimals["d"] = v
	return memory.NewSchema(f), b
}
func TestN12DecimalBadLaterAndEmpty(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, phase := range []string{"key", "payload"} {
			for _, empty := range []bool{false, true} {
				schema, good := n12Flat(4)
				_, bad := n12Flat(4)
				bad.Decimals["d"].Values[0] = decimal128.New(0x7fffffffffffffff, ^uint64(0))
				if empty {
					bad.Rows = 0
				}
				p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{good, good}}, stable: stable}
				k := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{good, good}}, stable: stable}
				if phase == "key" && stable {
					k.batches[1] = bad
				} else {
					p.batches[1] = bad
				}
				m := memory.NewBudgetManager(8 << 20)
				top, e := NewTopKSourceWithKeyInput(p, k, 1, []SortKey{{Column: "d", Descending: true}}, m)
				if e != nil {
					t.Fatal(e)
				}
				st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
				if e != nil {
					t.Fatal(e)
				}
				if ok, e := st.Next(context.Background(), memory.NewBatch()); ok || e == nil {
					t.Fatal(stable, phase, empty, ok, e)
				}
				st.Close()
				if m.Used() != 0 {
					t.Fatal("bad later leak")
				}
			}
		}
	}
}
func TestN12DecimalKeyMetadataAndNullPoison(t *testing.T) {
	schema, b := n12Flat(4)
	for _, other := range []memory.Field{{Name: "d", Type: memory.TypeDecimal}, {Name: "d", Type: memory.TypeInt64}, {Name: "d", Type: memory.TypeDecimal, DecimalPrecision: 38, DecimalScale: 8}} {
		p := &n9Source{n8Source: n8Source{schema: schema}}
		k := &n9Source{n8Source: n8Source{schema: memory.NewSchema(other)}}
		if _, e := NewTopKSourceWithKeyInput(p, k, 1, []SortKey{{Column: "d"}}, nil); e == nil {
			t.Fatal("mismatched key admitted", other)
		}
	}
	b.Decimals["d"].Validity = memory.Bitmap{14}
	b.Decimals["d"].Values[0] = decimal128.New(0x7fffffffffffffff, ^uint64(0))
	for _, stable := range []bool{false, true} {
		p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b}}, stable: stable}
		m := memory.NewBudgetManager(8 << 20)
		top, e := NewTopKSourceWithManager(p, 4, []SortKey{{Column: "d", NullsLast: true}}, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		out := memory.NewBatch()
		if ok, e := st.Next(context.Background(), out); !ok || e != nil {
			t.Fatal(ok, e)
		}
		if out.Decimals["d"].IsValid(3) {
			t.Fatal("NULL poison")
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("poison leak")
		}
	}
}
func TestN12DecimalPagedFailureAndCancel(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		schema, b := n12Flat(130)
		p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b}}}
		m := &n8Manager{Manager: memory.NewBudgetManager(64 << 20)}
		top, e := NewTopKSourceWithManager(p, 130, []SortKey{{Column: "d"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if _, ok := st.(*topKContainerSnapshotStream); !ok {
			t.Fatal("flat Decimal missed typed owner")
		}
		out := memory.NewBatch()
		if ok, e := st.Next(context.Background(), out); !ok || e != nil || out.Rows != 64 {
			t.Fatal(ok, e, out.Rows)
		}
		alias := out.Decimals["d"]
		for i, v := range alias.Values {
			if v != decimal128.FromI64(int64(i+1)) {
				t.Fatal("page oracle")
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		} else {
			m.fail = m.calls + 1
		}
		ok, e := st.Next(ctx, out)
		cancel()
		want := memory.ErrBudgetExceeded
		if cancelled {
			want = context.Canceled
		}
		if ok || !errors.Is(e, want) {
			t.Fatal(ok, e)
		}
		st.Close()
		if len(alias.Values) != 0 || m.Used() != 0 {
			t.Fatal("page cleanup")
		}
	}
}
func TestN12DecimalMultiKeyOracle(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, desc := range []bool{false, true} {
			for _, last := range []bool{false, true} {
				schema, b := n12Flat(65)
				g, _ := memory.Decimal128Field("g", true, 1, 0)
				schema = memory.NewSchema(append(schema.Fields, g)...)
				b.Decimals["g"] = &memory.Decimal128Vector{Precision: 1, Values: make([]memory.Decimal128, 65), Validity: make(memory.Bitmap, 2)}
				b.Decimals["d"].Validity = make(memory.Bitmap, 2)
				for i := 0; i < 65; i++ {
					b.Decimals["d"].Values[i] = decimal128.FromI64(int64(i%5 - 2))
					b.Decimals["g"].Values[i] = decimal128.FromI64(int64(i%3 - 1))
					if i%7 != 0 {
						memory.Set(b.Decimals["d"].Validity, i)
					}
					if i%11 != 0 {
						memory.Set(b.Decimals["g"].Validity, i)
					}
				}
				b.Int64s["id"] = &memory.Int64Vector{Values: make([]int64, 65)}
				for i := range b.Int64s["id"].Values {
					b.Int64s["id"].Values[i] = int64(i)
				}
				schema = memory.NewSchema(append(schema.Fields, memory.Field{Name: "id", Type: memory.TypeInt64})...)
				order := make([]int, 65)
				for i := range order {
					order[i] = i
				}
				sort.SliceStable(order, func(i, j int) bool {
					a, z := order[i], order[j]
					for _, name := range []string{"d", "g"} {
						v := b.Decimals[name]
						av, zv := v.IsValid(a), v.IsValid(z)
						if av != zv {
							return av == last
						}
						if !av {
							continue
						}
						c := v.Values[a].BigInt().Cmp(v.Values[z].BigInt())
						if c != 0 {
							if desc {
								return c > 0
							}
							return c < 0
						}
					}
					return false
				})
				p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b}}, stable: stable}
				m := memory.NewBudgetManager(8 << 20)
				top, e := NewTopKSourceWithManager(p, 17, []SortKey{{Column: "d", Descending: desc, NullsLast: last}, {Column: "g", Descending: desc, NullsLast: last}}, m)
				if e != nil {
					t.Fatal(e)
				}
				st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
				if e != nil {
					t.Fatal(e)
				}
				out := memory.NewBatch()
				if ok, e := st.Next(context.Background(), out); !ok || e != nil {
					t.Fatal(ok, e)
				}
				for i, v := range out.Int64s["id"].Values {
					if v != int64(order[i]) {
						t.Fatal("multi-key oracle", stable, desc, last, i, v, order[i])
					}
				}
				if out.Rows != 17 {
					t.Fatal(out.Rows)
				}
				st.Close()
				if m.Used() != 0 {
					t.Fatal("multi leak")
				}
			}
		}
	}
}

func TestN12DecimalAtomicReplacementKeepsOldOwner(t *testing.T) {
	s, b := n10Input(t)
	m := &n8Manager{Manager: memory.NewBudgetManager(0)}
	stream := &topKContainerSnapshotStream{owner: &TopKSource{schema: s, input: &n9Source{n8Source: n8Source{schema: s}}, keys: []SortKey{{Column: "decimal"}}, k: 1}, manager: m, input: b}
	old, e := stream.snapshot(context.Background(), 0)
	if e != nil {
		t.Fatal(e)
	}
	if old.owner.view != nil || old.owner.viewLease != nil || old.batch != old.owner.batch {
		t.Fatal("key snapshot retained redundant output view")
	}
	held := m.Used()
	oldBatch := old.owner.batch
	oldArray := oldBatch.Arrays["array1"]
	oldKey := old.key.cells[0]
	m.calls = 0
	candidate, e := stream.snapshot(context.Background(), 3)
	if e != nil {
		t.Fatal(e)
	}
	calls := m.calls
	if m.Used() <= held || old.owner.batch != oldBatch || !reflect.DeepEqual(old.key.cells[0], oldKey) {
		t.Fatal("old released before new commit")
	}
	candidate.release()
	if m.Used() != held {
		t.Fatal("new release lost old charge")
	}
	for ordinal := 1; ordinal <= calls; ordinal++ {
		m.calls = 0
		m.fail = ordinal
		_, e = stream.snapshot(context.Background(), 3)
		if !errors.Is(e, memory.ErrBudgetExceeded) {
			t.Fatal(ordinal, e)
		}
		if m.Used() != held || old.owner.batch != oldBatch || oldArray.Child == nil {
			t.Fatal("old invalidated", ordinal, m.Used(), held)
		}
	}
	m.fail = 0
	old.release()
	if m.Used() != 0 {
		t.Fatal("leak", m.Used())
	}
}

func TestN12DecimalNonwinnerPayloadAlwaysValidated(t *testing.T) {
	for _, stable := range []bool{false, true} {
		schema, good := n12Flat(4)
		_, bad := n12Flat(4)
		schema = memory.NewSchema(append(schema.Fields, memory.Field{Name: "rank", Type: memory.TypeInt64})...)
		good.Int64s["rank"] = &memory.Int64Vector{Values: []int64{0, 1, 2, 3}}
		bad.Int64s["rank"] = &memory.Int64Vector{Values: []int64{100, 101, 102, 103}}
		bad.Decimals["d"].Values[3] = decimal128.New(0x7fffffffffffffff, ^uint64(0))
		p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{good, bad}}, stable: stable}
		ks := memory.NewSchema(memory.Field{Name: "rank", Type: memory.TypeInt64})
		k1, k2 := memory.NewBatch(), memory.NewBatch()
		k1.Rows = 4
		k2.Rows = 4
		k1.Int64s["rank"] = good.Int64s["rank"]
		k2.Int64s["rank"] = bad.Int64s["rank"]
		k := &n9Source{n8Source: n8Source{schema: ks, batches: []*memory.Batch{k1, k2}}, stable: stable}
		m := memory.NewBudgetManager(8 << 20)
		top, e := NewTopKSourceWithKeyInput(p, k, 1, []SortKey{{Column: "rank"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(context.Background(), top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := st.Next(context.Background(), memory.NewBatch()); ok || e == nil {
			t.Fatal("invalid unselected payload escaped", stable, ok, e)
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("nonwinner leak")
		}
	}
}
func TestN12DecimalValidationMidCancel(t *testing.T) {
	for _, stable := range []bool{false, true} {
		schema, b := n12Flat(16384)
		p := &n9Source{n8Source: n8Source{schema: schema, batches: []*memory.Batch{b}}, stable: stable}
		m := memory.NewBudgetManager(8 << 20)
		ctx := &n10CancelContext{Context: context.Background(), stage: "validate"}
		top, e := NewTopKSourceWithManager(p, 1, []SortKey{{Column: "d"}}, m)
		if e != nil {
			t.Fatal(e)
		}
		st, e := source.OpenBatchStream(ctx, top, testReadOptions())
		if e != nil {
			t.Fatal(e)
		}
		if ok, e := st.Next(ctx, memory.NewBatch()); ok || !errors.Is(e, context.Canceled) || ctx.checks != 5 {
			t.Fatal(stable, ok, e, ctx.checks)
		}
		st.Close()
		if m.Used() != 0 {
			t.Fatal("validation cancel leak")
		}
	}
}
func TestN12DecimalCellsNoAllocation(t *testing.T) {
	schema, b := n12Flat(4)
	b.Decimals["d"].Values = []memory.Decimal128{decimal128.New(-1, 0), decimal128.New(-1, ^uint64(0)), decimal128.New(0, 0), decimal128.New(1, 0)}
	if n := testing.AllocsPerRun(1000, func() {
		a, e := snapshotCell(b, schema, "d", 0)
		if e != nil {
			panic(e)
		}
		z, e := snapshotCell(b, schema, "d", 1)
		if e != nil {
			panic(e)
		}
		if compareCells(a, z, true) >= 0 {
			panic("signed limb order")
		}
	}); n != 0 {
		t.Fatal("cell allocation", n)
	}
}
