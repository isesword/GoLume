package expr

import (
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"testing"
)

// These probes exercise the existing dense literal representation. They do not
// add, require or claim compact-literal optimization for zoned extraction.
func TestISOCalendarLiteralDenseReuse(t *testing.T) {
	for _, tc := range []struct {
		zone, instant string
		want          [4]int64
	}{
		{"new_york", "2021-01-04T00:00:00Z", [4]int64{2020, 53, 7, -18000}},
		{"paris", "1900-01-01T00:00:00Z", [4]int64{1900, 1, 1, 561}},
		{"utc", "1969-12-31T23:59:59.999999999Z", [4]int64{1970, 1, 3, 0}},
	} {
		t.Run(tc.zone, func(t *testing.T) {
			zone := timezoneFixture(t, tc.zone)
			parts := []func(Expr, TimeZone) ZonedExtractExpr{ISOWeekYearInZone, ISOWeekInZone, ISOWeekdayInZone, UTCOffsetInZone}
			var outputs []NamedExpr
			for i, part := range parts {
				outputs = append(outputs, part(Timestamp(civilTestUTC(t, tc.instant)), zone).Alias(fmt.Sprintf("value%d", i)))
			}
			for i, part := range parts {
				outputs = append(outputs, part(Null(memory.TypeTimestamp), zone).Alias(fmt.Sprintf("null%d", i)))
			}
			p, err := CompileExpressions(memory.NewSchema(), outputs)
			if err != nil {
				t.Fatal(err)
			}
			timestampLeaves := 0
			for _, in := range p.instructions {
				if in.Code == scalarLiteralI64 && in.Type == memory.TypeTimestamp {
					timestampLeaves++
					if in.compactLiteral {
						t.Fatal("unexpected compact zoned literal path")
					}
				}
			}
			if timestampLeaves != 1 {
				t.Fatal("shared Timestamp literal missing", timestampLeaves)
			}
			for i := 0; i < 8; i++ {
				field := p.OutputField(i)
				if field.Type != memory.TypeInt64 || field.Nullable != (i >= 4) {
					t.Fatal("literal output metadata", i, field)
				}
			}
			for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
				manager := memory.NewBudgetManager(0)
				w, err := NewWorkspace(context.Background(), p, manager)
				if err != nil {
					t.Fatal(err)
				}
				for _, n := range []int{0, 1, 63, 64, 65, 130, 1} {
					b := memory.NewBatch()
					b.Reset(n)
					out, err := w.Evaluate(context.Background(), b, kernel.New(mode))
					if err != nil {
						t.Fatal(mode, n, err)
					}
					for col, value := range out {
						for row := 0; row < n; row++ {
							valid := col < 4
							if value.Int64.IsValid(row) != valid {
								t.Fatal("literal validity", mode, n, col, row)
							}
							want := int64(0)
							if valid {
								want = tc.want[col]
							}
							if value.Int64.Values[row] != want {
								t.Fatal("literal value", mode, n, col, row, value.Int64.Values[row], want)
							}
						}
					}
					if manager.Used() != w.MemoryBytes() {
						t.Fatal("dense literal accounting", manager.Used(), w.MemoryBytes())
					}
				}
				w.Close()
				if manager.Used() != 0 {
					t.Fatal("dense literal leak", manager.Used())
				}
			}
		})
	}
}
