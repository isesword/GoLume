package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"reflect"
	"testing"
)

func TestCivilComposeReviewBoundaryAndPrecedence(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, "")), FoldReject)
	for _, tc := range []struct {
		day         int32
		clock, want int64
		err         error
	}{
		{-106752, 763145224192, math.MinInt64, nil},
		{-106752, 763145224191, 0, ErrCivilRange},
		{106751, 85636854775807, math.MaxInt64, nil},
		{106751, 85636854775808, 0, ErrCivilRange},
		{-1, 86399999999999, -1, nil},
		{0, 0, 0, nil},
		{math.MinInt32, 0, 0, ErrCivilRange},
		{math.MaxInt32, 0, 0, ErrCivilRange},
	} {
		got, err := r.ResolveDateTime(context.Background(), tc.day, tc.clock)
		if !errors.Is(err, tc.err) || err == nil && got != tc.want {
			t.Fatal(tc, got, err)
		}
	}
	for _, bad := range []int64{-1, 86400e9, math.MinInt64, math.MaxInt64} {
		if _, err := r.ResolveDateTime(context.Background(), 0, bad); !errors.Is(err, ErrCivilClock) {
			t.Fatal(bad, err)
		}
	}
	// ResolveDateTime inherits the same configuration/context priority, before
	// clock validation. Clock classification must not broaden the old helper.
	if civilRowError(ErrCivilClock) {
		t.Fatal("old civil row classification expanded")
	}
	var zero CivilTimeResolver
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		r    CivilTimeResolver
		ctx  context.Context
		want error
	}{
		{r, nil, ErrCivilConfiguration}, {zero, context.Background(), ErrCivilConfiguration},
		{zero, ctx, context.Canceled}, {r, ctx, context.Canceled},
	} {
		if _, err := tc.r.ResolveDateTime(tc.ctx, 0, -1); !errors.Is(err, tc.want) {
			t.Fatal(err, tc.want)
		}
	}
	// Backward transition makes two candidates for this exact clock, with only
	// the earlier candidate representable. Do not filter range before choosing.
	z := civilTestZone(t, civilTestData('2', []int32{1, 0}, []int32{1, 0}, []int64{9223372036}, []byte{1}, ""))
	for _, policy := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
		got, err := civilTestResolver(t, z, policy).ResolveDateTime(context.Background(), 106751, 85636999999999)
		switch policy {
		case FoldReject:
			if !errors.Is(err, ErrCivilFold) {
				t.Fatal(err)
			}
		case FoldLater:
			if !errors.Is(err, ErrCivilRange) {
				t.Fatal(err)
			}
		default:
			if err != nil || got != 9223372035999999999 {
				t.Fatal(got, err)
			}
		}
	}
}

func TestCivilComposeReviewComputedInputsOwnership(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	r := civilTestResolver(t, z, FoldReject)
	date, clock := DateInZone(Col("ts"), z), TimeOfDayInZone(Col("ts"), z)
	p, err := CompileExpressions(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp, Nullable: true}), []NamedExpr{
		ComposeCivilTimestamp(date, clock, r).Alias("joined"), date.Alias("date"), clock.Alias("clock"),
		TryComposeCivilTimestamp(date, clock, r).Alias("try"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.SupportsSelection() {
		t.Fatal("unexpected new selected fast path")
	}
	for _, index := range []int{0, 3} {
		in := p.instructions[p.outputs[index].root]
		if !reflect.DeepEqual(instructionInputs(in), []int{in.A, in.B}) {
			t.Fatal("missing ordered A/B inputs")
		}
		if in.storageOwner == p.instructions[in.A].storageOwner || in.storageOwner == p.instructions[in.B].storageOwner {
			t.Fatal("in-place clobber possible")
		}
		n, e := fixedRegisterBytes(in, 65)
		if e != nil || n != 65*8+2*8 {
			t.Fatal("Timestamp accounting", n, e)
		}
	}
	values := []int64{civilTestUTC(t, "2024-03-10T07:00:00.000000001Z"), -1, civilTestUTC(t, "2024-07-01T12:34:56.123456789Z")}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		m := memory.NewBudgetManager(0)
		w, e := NewWorkspace(context.Background(), p, m)
		if e != nil {
			t.Fatal(e)
		}
		for _, n := range []int{130, 1, 65, 0, 130} {
			b := memory.NewBatch()
			b.Reset(n)
			v := &memory.Int64Vector{Values: make([]int64, n), Validity: make(memory.Bitmap, memory.WordsFor(n))}
			for i := range v.Values {
				v.Values[i] = values[i%3]
				if i%5 != 0 {
					memory.Set(v.Validity, i)
				}
			}
			b.Int64s["ts"] = v
			out, e := w.Evaluate(context.Background(), b, kernel.New(mode))
			if e != nil {
				t.Fatal(e)
			}
			for i := 0; i < n; i++ {
				valid := i%5 != 0
				for _, col := range []int{0, 3} {
					if out[col].Int64.IsValid(i) != valid || valid && out[col].Int64.Values[i] != values[i%3] {
						t.Fatal("computed children clobbered", mode, n, col, i)
					}
				}
				if out[1].Int32.IsValid(i) != valid || out[2].Int64.IsValid(i) != valid {
					t.Fatal("exported child validity")
				}
				if valid {
					wantClock := []int64{10800000000001, 68399999999999, 30896123456789}[i%3]
					if out[2].Int64.Values[i] != wantClock {
						t.Fatal("exported clock overwritten", i, out[2].Int64.Values[i], wantClock)
					}
				}
			}
			if m.Used() != w.MemoryBytes() {
				t.Fatal("accounting")
			}
		}
		w.Close()
		w.Close()
		if m.Used() != 0 {
			t.Fatal("leak")
		}
	}
}

func TestCivilComposeReviewDemandedStorage(t *testing.T) {
	r := civilTestResolver(t, civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, "")), FoldReject)
	output := func() scalarRegister {
		return scalarRegister{typ: memory.TypeTimestamp, i64: make([]int64, 1), ownedValidity: make(memory.Bitmap, 1)}
	}
	// Either NULL skips both payloads, including a missing counterpart. This is
	// operator demand, not a promise to suppress errors of evaluated child nodes.
	for _, nullDate := range []bool{false, true} {
		d := scalarRegister{typ: memory.TypeDate32}
		c := scalarRegister{typ: memory.TypeTime64}
		if nullDate {
			d.validity = memory.Bitmap{0}
		} else {
			c.validity = memory.Bitmap{0}
		}
		out := output()
		if e := evalCivilCompose(&out, &d, &c, r.state, true, 1, nil); e != nil || memory.IsSet(out.validity, 0) {
			t.Fatal("NULL demanded payload", e)
		}
	}
	d := scalarRegister{typ: memory.TypeDate32, i32: []int32{0}}
	c := scalarRegister{typ: memory.TypeTime64, i64: []int64{86400e9}}
	out := output()
	if e := evalCivilCompose(&out, &d, &c, r.state, true, 1, nil); e != nil || memory.IsSet(out.validity, 0) {
		t.Fatal("Try clock", e)
	}
	out = output()
	if e := evalCivilCompose(&out, &d, &c, r.state, false, 1, nil); !errors.Is(e, ErrCivilClock) {
		t.Fatal(e)
	}
	c.i64 = nil
	out = output()
	if e := evalCivilCompose(&out, &d, &c, r.state, true, 1, nil); e == nil || errors.Is(e, ErrCivilClock) || civilRowError(e) {
		t.Fatal("swallowed storage", e)
	}
	out = output()
	if e := evalCivilCompose(&out, &d, &c, r.state, true, 1, memory.Bitmap{0}); e != nil {
		t.Fatal("inactive storage", e)
	}
	d = scalarRegister{typ: memory.TypeDate32, constant: true, i64lit: int64(math.MaxInt32) + 1}
	c.i64 = []int64{0}
	out = output()
	if e := evalCivilCompose(&out, &d, &c, r.state, true, 1, nil); e == nil || civilRowError(e) || errors.Is(e, ErrCivilClock) {
		t.Fatal("malformed constant swallowed", e)
	}
}

func TestCivilComposeReviewTryDoesNotSuppressChildError(t *testing.T) {
	z := civilTestZone(t, civilTestData(0, []int32{0}, nil, nil, nil, ""))
	r := civilTestResolver(t, z, FoldReject)
	sc := memory.NewSchema(memory.Field{Name: "d", Type: memory.TypeDate32, Nullable: true})
	ex := TryComposeCivilTimestamp(Col("d"), TimeOfDayInZone(ParseCivilTimestamp(Str("bad"), r), z), r)
	bound, e := Bind(sc, ex)
	if e != nil || !bound.MayError {
		t.Fatal("child MayError lost", e)
	}
	p, e := Compile(sc, ex)
	if e != nil {
		t.Fatal(e)
	}
	b := memory.NewBatch()
	b.Reset(1)
	b.Int32s["d"] = &memory.Int32Vector{Values: []int32{0}, Validity: memory.Bitmap{0}}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar)); !errors.Is(e, ErrCivilSyntax) {
		t.Fatal("NULL date/Try suppressed child error", e)
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("leak")
	}
}
