package expr

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
	"time"
)

// Independent composition probe: a computed Date32 register is consumed by the
// new expression, and its result is converted back to the same civil date.
func TestCivilDateReviewComputedRoundTrip(t *testing.T) {
	z := timezoneFixture(t, "new_york")
	r := civilTestResolver(t, z, FoldReject)
	sc := memory.NewSchema(memory.Field{Name: "date", Type: memory.TypeDate32, Nullable: true}, memory.Field{Name: "instant", Type: memory.TypeTimestamp, Nullable: true})
	p, err := CompileExpressions(sc, []NamedExpr{
		DateAtMidnight(DateInZone(Col("instant"), z), r).Alias("computed"),
		DateInZone(DateAtMidnight(Col("date"), r), z).Alias("roundtrip"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.SupportsSelection() {
		t.Fatal("new operation unexpectedly enabled selection fast path")
	}
	b := memory.NewBatch()
	b.Reset(3)
	b.Int32s["date"] = &memory.Int32Vector{Values: []int32{19792, 19793, 2147483647}, Validity: memory.Bitmap{3}}
	b.Int64s["instant"] = &memory.Int64Vector{Values: []int64{civilTestUTC(t, "2024-03-10T12:00:00Z"), civilTestUTC(t, "2024-03-11T12:00:00Z"), 0}, Validity: memory.Bitmap{3}}
	manager := memory.NewBudgetManager(0)
	w, err := NewWorkspace(context.Background(), p, manager)
	if err != nil {
		t.Fatal(err)
	}
	out, err := w.Evaluate(context.Background(), b, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{civilTestUTC(t, "2024-03-10T05:00:00Z"), civilTestUTC(t, "2024-03-11T04:00:00Z")}
	for i := 0; i < 2; i++ {
		if !out[0].Int64.IsValid(i) || out[0].Int64.Values[i] != want[i] {
			t.Fatal("computed Date32", i, out[0].Int64)
		}
		if !out[1].Int32.IsValid(i) || out[1].Int32.Values[i] != int32(19792+i) {
			t.Fatal("roundtrip", i, out[1].Int32)
		}
	}
	if out[0].Int64.IsValid(2) || out[1].Int32.IsValid(2) {
		t.Fatal("NULL lost")
	}
	if p.OutputField(0).Type != memory.TypeTimestamp || p.OutputField(1).Type != memory.TypeDate32 {
		t.Fatal("logical metadata")
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("retained workspace", manager.Used())
	}
}

func TestCivilDateReviewFoldBeforeRange(t *testing.T) {
	// Each synthetic backwards transition repeats local midnight. One candidate
	// is representable as Timestamp, the other isn't. Policies choose from BOTH,
	// rather than dropping the out-of-range candidate before fold selection.
	for _, tc := range []struct {
		name                    string
		day                     int32
		offsets                 []int32
		transition, early, late string
		earlyErr, lateErr       error
	}{
		{"upper", 106752, []int32{3600, 0}, "2262-04-11T23:30:00Z", "2262-04-11T23:00:00Z", "2262-04-12T00:00:00Z", nil, ErrCivilRange},
		{"lower", -106752, []int32{0, -3600}, "1677-09-21T00:30:00Z", "1677-09-21T00:00:00Z", "1677-09-21T01:00:00Z", ErrCivilRange, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trans, err := time.Parse(time.RFC3339, tc.transition)
			if err != nil {
				t.Fatal(err)
			}
			z := civilTestZone(t, civilTestData('2', tc.offsets, tc.offsets, []int64{trans.Unix()}, []byte{1}, ""))
			// Independently verify both hard-coded UTC instants really name midnight
			// on the intended civil date, including the instant outside ns range.
			for _, text := range []string{tc.early, tc.late} {
				instant, err := time.Parse(time.RFC3339, text)
				if err != nil {
					t.Fatal(err)
				}
				local := instant.In(z.state.location)
				y, m, d := local.Date()
				h, mi, s := local.Clock()
				epoch := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400
				if epoch != int64(tc.day) || h != 0 || mi != 0 || s != 0 {
					t.Fatal("invalid independent fixture", text, local)
				}
			}
			for _, policy := range []CivilFoldPolicy{FoldReject, FoldEarlier, FoldLater} {
				r := civilTestResolver(t, z, policy)
				got, err := r.ResolveDate(context.Background(), tc.day)
				wantErr, wantText := tc.earlyErr, tc.early
				if policy == FoldReject {
					wantErr = ErrCivilFold
				} else if policy == FoldLater {
					wantErr, wantText = tc.lateErr, tc.late
				}
				if wantErr != nil {
					if !errors.Is(err, wantErr) {
						t.Fatalf("policy %v: %v, want %v", policy, err, wantErr)
					}
				} else if err != nil || got != civilTestUTC(t, wantText) {
					t.Fatalf("policy %v: got %d, %v", policy, got, err)
				}
			}
		})
	}
}
