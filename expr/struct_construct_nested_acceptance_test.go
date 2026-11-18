package expr

import (
	"context"
	"encoding/json"
	"errors"
	"gocolumn/memory"
	"runtime"
	"strings"
	"testing"
)

func n7MixedInputs(t *testing.T) ([]*scalarRegister, []int, []memory.Field) {
	t.Helper()
	rs, rb := recursiveBFixture(t, 2, 4)
	ls, lb := listStructExprFixture(t, memory.TypeInt64, 4)
	ns, nb := nestedListExprFixture(t, true, memory.TypeInt64, 4)
	r := &scalarRegister{typ: memory.TypeStruct, strct: rb.Structs["root"], validity: rb.Structs["root"].Validity}
	l := &scalarRegister{typ: memory.TypeList, structList: lb.StructLists["xs"], validity: lb.StructLists["xs"].Validity}
	a := &scalarRegister{typ: memory.TypeList, array: nb.Arrays["xs"], validity: nb.Arrays["xs"].Validity}
	scalar := &scalarRegister{typ: memory.TypeInt64, i64: []int64{1, 2, 3, 4}}
	fields := []memory.Field{rs.Fields[0], ls.Fields[0], ns.Fields[0], rs.Fields[0], {Name: "oldscalar", Type: memory.TypeInt64}}
	for i, n := range []string{"recursive", "records", "array", "duplicate", "oldscalar"} {
		fields[i].Name = n
	}
	return []*scalarRegister{r, l, a, scalar}, []int{0, 1, 2, 0, 3}, fields
}
func n7Output(m *structGetAuditManager) *scalarRegister {
	return &scalarRegister{typ: memory.TypeStruct, acquire: func(n int64) (memory.Reservation, error) { return m.Reserve(context.Background(), n) }}
}
func n7Run(out *scalarRegister, regs []*scalarRegister, inputs []int, fields []memory.Field) error {
	return evalStructFrom(out, regs, inputs, fields, 4, nil)
}

func TestN7MixedEveryAdmissionFailureRetryAndExactBudget(t *testing.T) {
	regs, inputs, fields := n7MixedInputs(t)
	calibration := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	out := n7Output(calibration)
	if err := n7Run(out, regs, inputs, fields); err != nil {
		t.Fatal(err)
	}
	retained := calibration.Used()
	calibration.calls = 0
	calibration.peak = retained
	if err := n7Run(out, regs, inputs, fields); err != nil {
		t.Fatal(err)
	}
	calls, peak := calibration.calls, calibration.peak
	if calls < 7 || peak <= retained {
		t.Fatalf("insufficient mixed trace calls%d retained%d peak%d", calls, retained, peak)
	}
	out.structFromReservation.Release()
	for ordinal := 1; ordinal <= calls; ordinal++ {
		m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
		r := n7Output(m)
		if err := n7Run(r, regs, inputs, fields); err != nil {
			t.Fatal(err)
		}
		old, lease, used := r.strct, r.structFromReservation, m.Used()
		before, _ := json.Marshal(old)
		m.calls = 0
		m.fail = ordinal
		err := n7Run(r, regs, inputs, fields)
		after, _ := json.Marshal(r.strct)
		if !errors.Is(err, memory.ErrBudgetExceeded) || r.strct != old || r.structFromReservation != lease || m.Used() != used || string(before) != string(after) {
			t.Fatalf("ordinal%d err%v used%d want%d", ordinal, err, m.Used(), used)
		}
		m.calls = 0
		m.fail = 0
		if err = n7Run(r, regs, inputs, fields); err != nil {
			t.Fatalf("retry%d %v", ordinal, err)
		}
		if m.Used() != r.structFromBytes {
			t.Fatal("retry ledger")
		}
		r.structFromReservation.Release()
		if m.Used() != 0 {
			t.Fatal("fault cleanup")
		}
	}
	for _, limit := range []int64{peak, peak - 1} {
		m := &structGetAuditManager{base: memory.NewBudgetManager(limit)}
		r := n7Output(m)
		if err := n7Run(r, regs, inputs, fields); err != nil {
			t.Fatalf("warm limit%d: %v", limit, err)
		}
		old, used := r.strct, m.Used()
		err := n7Run(r, regs, inputs, fields)
		if limit == peak && err != nil {
			t.Fatal("measured exact denied", err)
		}
		if limit == peak-1 && (!errors.Is(err, memory.ErrBudgetExceeded) || r.strct != old || m.Used() != used) {
			t.Fatal("exact-minus-one", err)
		}
		r.structFromReservation.Release()
		if m.Used() != 0 {
			t.Fatal("budget cleanup")
		}
	}
}

func TestN7RepeatedChildIndependentOwnerCharges(t *testing.T) {
	regs, inputs, fields := n7MixedInputs(t)
	m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	out := n7Output(m)
	if err := n7Run(out, regs, inputs, fields); err != nil {
		t.Fatal(err)
	}
	defer out.structFromReservation.Release()
	owners := out.structFromReservation.(*structConstructOwners)
	// metadata + flat + recursive + StructList + Array + repeated recursive.
	if len(owners.leases) != 6 {
		t.Fatalf("owner count %d", len(owners.leases))
	}
	if owners.leases[2].Bytes() <= 0 || owners.leases[2].Bytes() != owners.leases[5].Bytes() {
		t.Fatal("repeated occurrence not independently charged")
	}
	var sum int64
	for _, l := range owners.leases {
		sum += l.Bytes()
	}
	if sum != m.Used() || sum != out.structFromBytes {
		t.Fatal("aggregate ledger")
	}
	a := out.strct.Values.Structs["recursive"].Values.Structs["node.中"].Values.Int64s["v"]
	b := out.strct.Values.Structs["duplicate"].Values.Structs["node.中"].Values.Int64s["v"]
	src := regs[0].strct.Values.Structs["node.中"].Values.Int64s["v"]
	before, source := b.Values[1], src.Values[1]
	a.Values[1] = 123456
	if b.Values[1] != before || src.Values[1] != source || &a.Values[0] == &b.Values[0] {
		t.Fatal("duplicate child aliases")
	}
}

func n7StringInput(bytes int) (*scalarRegister, memory.Field) {
	values := memory.NewBatch()
	values.Rows = 4
	data := []byte(strings.Repeat("x", bytes*4))
	offsets := []uint32{0, uint32(bytes), uint32(bytes * 2), uint32(bytes * 3), uint32(bytes * 4)}
	values.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data}
	f := memory.StructField("child", true, memory.Field{Name: "s", Type: memory.TypeString})
	v := &memory.StructVector{Rows: 4, Fields: f.Children, Values: values, Encoding: memory.EncodingView}
	return &scalarRegister{typ: memory.TypeStruct, strct: v}, f
}
func TestN7RegisterVectorActiveDomainsAndHiddenStructure(t *testing.T) {
	in, f := n7StringInput(4)
	in.strct.Validity = memory.Bitmap{3}
	in.validity = memory.Bitmap{6}
	active := memory.Bitmap{7}
	m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	out := n7Output(m)
	if err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{f}, 4, active); err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 4; row++ {
		if out.strct.IsValid(row) != (row < 3) || out.strct.Values.Structs["child"].IsValid(row) != (row == 1) {
			t.Fatal("mask domain", row)
		}
	}
	old, used := out.strct, m.Used()
	in.strct.Values.Strings["s"].Offsets[4] = 999999
	err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{f}, 4, active)
	if err == nil || out.strct != old || m.Used() != used {
		t.Fatal("inactive malformed storage hidden", err)
	}
	out.structFromReservation.Release()
	if m.Used() != 0 {
		t.Fatal("mask cleanup")
	}
}

func TestN7StickyCancellationInsideLongChildCopy(t *testing.T) {
	in, f := n7StringInput(256 << 10)
	m := &structGetAuditManager{base: memory.NewBudgetManager(0)}
	out := n7Output(m)
	if err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{f}, 4, nil); err != nil {
		t.Fatal(err)
	}
	old, used := out.strct, m.Used()
	before, _ := json.Marshal(old)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	copyChecks := 0
	out.checkContext = func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var pcs [32]uintptr
		n := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, ".appendRecursiveString") {
				copyChecks++
				if copyChecks == 3 {
					cancel()
				}
				break
			}
			if !more {
				break
			}
		}
		return ctx.Err()
	}
	err := evalStructFrom(out, []*scalarRegister{in}, []int{0}, []memory.Field{f}, 4, nil)
	after, _ := json.Marshal(out.strct)
	if !errors.Is(err, context.Canceled) || copyChecks != 3 || out.strct != old || m.Used() != used || string(before) != string(after) {
		t.Fatalf("copy cancel %v checkpoints%d ledger%d/%d", err, copyChecks, m.Used(), used)
	}
	if !errors.Is(out.checkContext(), context.Canceled) {
		t.Fatal("nonsticky cancellation")
	}
	out.structFromReservation.Release()
	if m.Used() != 0 {
		t.Fatal("copy cleanup")
	}
}
