package physical

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

type listStructCompactFault struct {
	base           *memory.BudgetManager
	calls, fail    int
	requests, live []int64
}

func (m *listStructCompactFault) Reserve(ctx context.Context, n int64) (memory.Reservation, error) {
	m.calls++
	m.requests = append(m.requests, n)
	m.live = append(m.live, m.base.Used())
	if m.fail > 0 && m.calls == m.fail {
		return nil, memory.ErrBudgetExceeded
	}
	return m.base.Reserve(ctx, n)
}
func (m *listStructCompactFault) Used() int64  { return m.base.Used() }
func (m *listStructCompactFault) Limit() int64 { return m.base.Limit() }
func listStructCompactFixture(t *testing.T, nested bool) (memory.Schema, *memory.Batch) {
	t.Helper()
	b := memory.NewBatch()
	b.Rows = 3
	fields := []memory.Field{{Name: "id", Type: memory.TypeInt64}}
	b.Int64s["id"] = &memory.Int64Vector{Values: []int64{0, 1, 2}}
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("list%d", i)
		f, e := memory.ListStructField(name, true, true, memory.Field{Name: "code", Type: memory.TypeInt64})
		if e != nil {
			t.Fatal(e)
		}
		values := memory.NewBatch()
		values.Rows = 3
		values.Int64s["code"] = &memory.Int64Vector{Values: []int64{int64(i * 100), int64(i*100 + 1), int64(i*100 + 2)}}
		v := &memory.StructListVector{Offsets: []uint32{0, 1, 2, 3}, Values: &memory.StructVector{Rows: 3, Fields: f.Children[0].Children, Values: values, Encoding: memory.EncodingView}, Encoding: memory.EncodingView}
		if nested && i == 0 {
			child := memory.NewBatch()
			child.Rows = 3
			child.StructLists[name] = v
			sf := memory.StructField("outer", false, f)
			b.Structs["outer"] = &memory.StructVector{Rows: 3, Fields: sf.Children, Values: child, Encoding: memory.EncodingView}
			fields = append(fields, sf)
		} else {
			b.StructLists[name] = v
			fields = append(fields, f)
		}
	}
	schema, e := memory.NewSchemaWithStructListsChecked(fields...)
	if e != nil {
		t.Fatal(e)
	}
	return schema, b
}
func listStructCompactCheck(t *testing.T, b *memory.Batch, nested bool) {
	t.Helper()
	if b.Rows != 2 || len(b.Int64s["id"].Values) != 2 || b.Int64s["id"].Values[0] != 0 || b.Int64s["id"].Values[1] != 2 {
		t.Fatal("selected scalar rows")
	}
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("list%d", i)
		var v *memory.StructListVector
		if nested && i == 0 {
			v = b.Structs["outer"].Values.StructLists[name]
		} else {
			v = b.StructLists[name]
		}
		if v == nil || v.Len() != 2 || v.Values.Values.Int64s["code"].Values[0] != int64(i*100) || v.Values.Values.Int64s["code"].Values[1] != int64(i*100+2) {
			t.Fatal("selected owned list", i)
		}
	}
}

// Calibrate the complete first owner's actual admission trace independently.
// Nested owner includes its root metadata AND its child List allocation.
func listStructFirstOwnerTrace(t *testing.T, src *memory.Batch, nested bool) ([]int64, int64) {
	t.Helper()
	m := &listStructCompactFault{base: memory.NewBudgetManager(0)}
	rows := []int{0, 2}
	var charge int64
	if nested {
		v := src.Structs["outer"]
		f := memory.Field{Name: "outer", Type: memory.TypeStruct, Nullable: true, Encoding: memory.EncodingView, Children: v.Fields}
		o, e := memory.TakeStructListsStructRowsOwned(context.Background(), m, v, f, rows)
		if e != nil {
			t.Fatal(e)
		}
		if o.Vector.Values.StructLists["list0"].Values.Values.Int64s["code"].Values[1] != 2 {
			t.Fatal("first nested owner incomplete")
		}
		charge = m.Used()
		if charge != o.Bytes {
			t.Fatal("nested owner charge")
		}
		o.Close()
	} else {
		v := src.StructLists["list0"]
		f := memory.Field{Name: "list0", Type: memory.TypeList, Nullable: true, Encoding: memory.EncodingView, Children: []memory.Field{{Name: "item", Type: memory.TypeStruct, Nullable: true, Encoding: memory.EncodingView, Children: v.Values.Fields}}}
		o, e := memory.TakeStructListRowsOwned(context.Background(), m, v, f, memory.StructListParentDomain{}, rows)
		if e != nil {
			t.Fatal(e)
		}
		if o.Vector.Values.Values.Int64s["code"].Values[1] != 2 {
			t.Fatal("first list owner incomplete")
		}
		charge = m.Used()
		if charge != o.Bytes {
			t.Fatal("list owner charge")
		}
		o.Close()
	}
	if m.Used() != 0 {
		t.Fatal("calibration leak")
	}
	t.Logf("complete first owner nested=%v reserve trace=%v charge=%d", nested, m.requests, charge)
	return append([]int64(nil), m.requests...), charge
}

func TestListStructCompactSecondOwnerFailure(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested%v/warm%v", nested, warm), func(t *testing.T) {
				_, src := listStructCompactFixture(t, nested)
				firstTrace, firstCharge := listStructFirstOwnerTrace(t, src, nested)
				secondOrdinal := len(firstTrace) + 2
				m := &listStructCompactFault{base: memory.NewBudgetManager(0)}
				w := compactWorkspace{manager: m, bounded: true}
				defer w.release()
				ctx := context.Background()
				rows := []int{0, 2}
				mask := memory.Bitmap{5}
				var old *memory.Batch
				var flatCharge int64
				if warm {
					var e error
					old, e = w.compact(ctx, src, rows, mask, kernel.New(kernel.ModeScalar))
					if e != nil {
						t.Fatal(e)
					}
					listStructCompactCheck(t, old, nested)
					if w.extended == nil {
						t.Fatal("missing first extended owner")
					}
					flatCharge = w.lease.Bytes()
				}
				m.calls = 0
				m.requests = nil
				m.live = nil
				m.fail = secondOrdinal // setup plus all admissions of the complete first owner.
				out, e := w.compact(ctx, src, rows, mask, kernel.New(kernel.ModeScalar))
				if out != nil || !errors.Is(e, memory.ErrBudgetExceeded) {
					t.Fatal("second owner denial", out, e)
				}
				if m.calls != secondOrdinal || len(m.live) != secondOrdinal {
					t.Fatal("ordinal mismatch", m.calls, secondOrdinal)
				}
				for i, n := range firstTrace {
					if m.requests[i+1] != n {
						t.Fatal("first owner trace mismatch", m.requests, firstTrace)
					}
				}
				if m.live[secondOrdinal-1] != flatCharge+m.requests[0]+firstCharge {
					t.Fatalf("complete first owner not charged: ordinal%d requests%v live%v firstCharge%d flat%d", secondOrdinal, m.requests, m.live, firstCharge, flatCharge)
				}

				if m.Used() != flatCharge || w.extended != nil {
					t.Fatalf("failed candidate leaked: used%d flat%d", m.Used(), flatCharge)
				}
				if warm && (len(old.StructLists) != 0 || len(old.Structs) != 0) {
					t.Fatal("old extended output still published after next compact")
				}
				// Unlike expression owner replacement, compact output is borrowed until Next:
				// prior nested maps were invalidated before constructing this failed candidate.
				m.fail = 0
				retry, e := w.compact(ctx, src, rows, mask, kernel.New(kernel.ModeScalar))
				if e != nil {
					t.Fatal(e)
				}
				listStructCompactCheck(t, retry, nested)
				w.release()
				if m.Used() != 0 {
					t.Fatal("retry close leak")
				}
				t.Logf("second top-level owner denied at Reserve%d; complete first owner=%d; prior flat retained=%d; new setup/first owner released; retry valid", secondOrdinal, firstCharge, flatCharge)
			})
		}
	}
}

func TestListStructFilterNextSecondOwnerFailure(t *testing.T) {
	schema, b := listStructCompactFixture(t, true)
	firstTrace, firstCharge := listStructFirstOwnerTrace(t, b, true)
	secondOrdinal := len(firstTrace) + 2
	input, e := inmemory.New(schema, b, b)
	if e != nil {
		t.Fatal(e)
	}
	filtered, e := NewFilterSource(input, expr.Not(expr.EQ(expr.Col("id"), expr.I64(1))))
	if e != nil {
		t.Fatal(e)
	}
	other := memory.NewBudgetManager(0)
	opts := testReadOptions()
	opts.MemoryManager = other
	stream, e := filtered.(interface {
		NewBatchStream(context.Context, source.ReadOptions) (source.BatchStream, error)
	}).NewBatchStream(context.Background(), opts)
	if e != nil {
		t.Fatal(e)
	}
	defer stream.Close()
	fs := stream.(*filterBatchStream)
	m := &listStructCompactFault{base: memory.NewBudgetManager(0)}
	fs.output.manager = m
	dst := memory.NewBatch()
	ok, e := stream.Next(context.Background(), dst)
	if e != nil || !ok {
		t.Fatal("first Next", e)
	}
	listStructCompactCheck(t, dst, true)
	m.calls = 0
	m.requests = nil
	m.live = nil
	m.fail = secondOrdinal
	flatCharge := fs.output.lease.Bytes()
	ok, e = stream.Next(context.Background(), dst)
	if ok || !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("second Next", ok, e)
	}
	if m.calls != secondOrdinal || m.live[secondOrdinal-1] != flatCharge+m.requests[0]+firstCharge {
		t.Fatal("second owner not admitted to failure point")
	}
	if len(dst.StructLists) != 0 || len(dst.Structs) != 0 || len(dst.Int64s) != 0 {
		t.Fatal("failed Next exposed old or partial columns")
	}
	if m.Used() != 0 || other.Used() != 0 {
		t.Fatalf("terminal failure owners leaked compact%d other%d", m.Used(), other.Used())
	}
	ok, e = stream.Next(context.Background(), dst)
	if ok || !errors.Is(e, memory.ErrBudgetExceeded) {
		t.Fatal("terminal error identity", ok, e)
	}
	stream.Close()
	if m.Used() != 0 || other.Used() != 0 {
		t.Fatal("close leak")
	}
}
