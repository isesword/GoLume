package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"testing"
)

func civilFormatReviewValues(t *testing.T, z TimeZone, values []int64) []string {
	t.Helper()
	p, e := Compile(memory.NewSchema(memory.Field{Name: "ts", Type: memory.TypeTimestamp}), FormatCivilTimestamp(Col("ts"), z))
	if e != nil {
		t.Fatal(e)
	}
	if p.SupportsSelection() {
		t.Fatal("String result unexpectedly admitted selected fast path")
	}
	b := memory.NewBatch()
	b.Reset(len(values))
	b.Int64s["ts"] = &memory.Int64Vector{Values: values}
	m := memory.NewBudgetManager(0)
	w, e := NewWorkspace(context.Background(), p, m)
	if e != nil {
		t.Fatal(e)
	}
	out, e := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if e != nil {
		t.Fatal(e)
	}
	result := make([]string, len(values))
	for i := range result {
		if !out[0].String.IsValid(i) {
			t.Fatal("valid input formatted NULL")
		}
		result[i] = string(out[0].String.ValueBytes(i))
		if len(result[i]) != 29 {
			t.Fatal("not fixed 29 bytes", result[i])
		}
		for _, c := range []byte(result[i]) {
			if c >= 128 {
				t.Fatal("not ASCII")
			}
		}
	}
	w.Close()
	if m.Used() != 0 {
		t.Fatal("retained formatter memory", m.Used())
	}
	return result
}

func TestCivilFormatReviewFixedTextAndLoaderUTC(t *testing.T) {
	z, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	// The ordinary loader handle is valid here although inverse civil resolvers
	// deliberately reject its missing raw-rule provenance.
	if _, e := NewCivilTimeResolver(context.Background(), z, CivilResolveOptions{Fold: FoldReject, Gap: GapReject}); !errors.Is(e, ErrCivilConfiguration) {
		t.Fatal("unexpected inverse provenance", e)
	}
	got := civilFormatReviewValues(t, z, []int64{0, 1, -1, 120000000, math.MinInt64, math.MaxInt64})
	want := []string{"1970-01-01T00:00:00.000000000", "1970-01-01T00:00:00.000000001", "1969-12-31T23:59:59.999999999", "1970-01-01T00:00:00.120000000", "1677-09-21T00:12:43.145224192", "2262-04-11T23:47:16.854775807"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatal(i, got[i], want[i])
		}
	}
}

func TestCivilFormatReviewExtremeOffsets(t *testing.T) {
	// Local date/time expectations were computed independently using Python's
	// Gregorian datetime and floor(ns/1e9)+signed offset. Fractions are unchanged.
	for _, tc := range []struct {
		offset int32
		want   [2]string
	}{
		{math.MaxInt32, [2]string{"1745-10-10T03:26:50.145224192", "2330-05-01T03:01:23.854775807"}},
		{math.MinInt32, [2]string{"1609-09-02T20:58:35.145224192", "2194-03-23T20:33:08.854775807"}},
	} {
		z := civilTestZone(t, civilTestData(0, []int32{tc.offset}, nil, nil, nil, ""))
		got := civilFormatReviewValues(t, z, []int64{math.MinInt64, math.MaxInt64})
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatal(tc.offset, i, got[i], tc.want[i])
			}
		}
	}
}

func TestCivilFormatReviewFoldNotInstantSerialization(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	got := civilFormatReviewValues(t, z, []int64{civilTestUTC(t, "2024-11-03T05:30:00Z"), civilTestUTC(t, "2024-11-03T06:30:00Z")})
	for _, text := range got {
		if text != "2024-11-03T01:30:00.000000000" {
			t.Fatal(text)
		}
	}
	if got[0] != got[1] {
		t.Fatal("fold wall times should coincide")
	}
	r := civilTestResolver(t, z, FoldReject)
	for _, text := range got {
		if _, e := r.Parse(context.Background(), text); !errors.Is(e, ErrCivilFold) {
			t.Fatal("formatted fold text must remain ambiguous", e)
		}
	}
}

func TestCivilFormatReviewNullActiveStorage(t *testing.T) {
	z, e := LoadTimeZone("UTC")
	if e != nil {
		t.Fatal(e)
	}
	// Prepared fixed buffers and sufficient test-owned payload capacity isolate
	// demanded-storage semantics. This is not a query-budget admission test.
	out := scalarRegister{typ: memory.TypeString, offsets: make([]uint32, 3), ownedValidity: make(memory.Bitmap, 1), data: make([]byte, 0, 64), checkContext: func() error { return nil }}
	in := scalarRegister{typ: memory.TypeTimestamp, i64: []int64{-1}, validity: memory.Bitmap{3}}
	if e := evalCivilFormat(&out, &in, z.state, 2, memory.Bitmap{1}); e != nil {
		t.Fatal("inactive storage demanded", e)
	}
	if len(out.data) != 29 || string(out.data) != "1969-12-31T23:59:59.999999999" || out.offsets[0] != 0 || out.offsets[1] != 29 || out.offsets[2] != 29 || out.validity[0] != 1 {
		t.Fatal("active output", out.offsets, out.validity, string(out.data))
	}
	in.i64 = nil
	in.validity = memory.Bitmap{0}
	if e := evalCivilFormat(&out, &in, z.state, 2, memory.Bitmap{3}); e != nil {
		t.Fatal("NULL storage demanded", e)
	}
	if len(out.data) != 0 || out.offsets[0] != 0 || out.offsets[1] != 0 || out.offsets[2] != 0 || out.validity[0] != 0 {
		t.Fatal("NULL appended stale bytes", out.offsets, out.validity)
	}
	in.validity = memory.Bitmap{3}
	if e := evalCivilFormat(&out, &in, z.state, 2, memory.Bitmap{3}); e == nil {
		t.Fatal("demanded malformed storage accepted")
	}
}
