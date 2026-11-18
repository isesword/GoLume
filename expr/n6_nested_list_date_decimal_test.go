package expr

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// This N6 regression feeds two owned ListGet levels into published date
// operations alongside D1 Decimal operations in one guarded reusable program.
// Its expected dates, offsets and coefficients do not call production helpers.
func TestN6NestedListDateDecimalCompositionGuardsCSEAndReuse(t *testing.T) {
	raw, err := os.ReadFile("testdata/timezone/new_york.tzif.b64")
	if err != nil {
		t.Fatal(err)
	}
	data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	zone, err := TimeZoneFromTZData("b33-new-york", data)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewCivilTimeResolver(context.Background(), zone, CivilResolveOptions{Fold: FoldReject, Gap: GapReject})
	if err != nil {
		t.Fatal(err)
	}
	aField, err := memory.Decimal128Field("a", false, 6, 2)
	if err != nil {
		t.Fatal(err)
	}
	bField, err := memory.Decimal128Field("b", false, 6, 2)
	if err != nil {
		t.Fatal(err)
	}
	root, err := memory.NestedListField("xs", false, memory.ListInt64Field("item", false, false))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := memory.NewSchemaWithNestedListsChecked(aField, bField,
		memory.Field{Name: "guard", Type: memory.TypeInt64}, root)
	if err != nil {
		t.Fatal(err)
	}
	get := func(name string) Expr {
		index := map[string]int64{"year": 0, "month": 1, "day": 2}[name]
		return ListGet(ListGet(Col("xs"), I64(0)), I64(index))
	}
	cond := GT(Col("guard"), I64(0))
	round := If(cond, RoundDecimal128(Col("a"), 3, 1, DecimalRoundHalfEven), NullDecimal128(3, 1))
	divide := If(cond, DivideRoundDecimal128(Col("a"), Col("b"), 5, 2, DecimalRoundHalfEven), NullDecimal128(5, 2))
	date := If(cond, DateFromYMD(get("year"), get("month"), get("day")), Null(memory.TypeDate32))
	midnight := DateAtMidnight(date, resolver)
	expressions := []NamedExpr{Named(round, "round"), Named(round, "round_copy"), Named(divide, "divide"), Named(divide, "divide_copy"), Named(date, "date"), Named(date, "date_copy"), Named(midnight, "midnight"), Named(HourInZone(midnight, zone), "hour"), Named(UTCOffsetInZone(midnight, zone), "offset")}
	p, err := CompileExpressions(schema, expressions)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]int{{0, 1}, {2, 3}, {4, 5}} {
		if p.outputs[pair[0]].root != p.outputs[pair[1]].root {
			t.Fatalf("CSE pair %v not shared", pair)
		}
	}
	if p.outputs[0].root == p.outputs[2].root || p.outputs[0].root == p.outputs[4].root || p.outputs[2].root == p.outputs[4].root {
		t.Fatal("distinct operation identities collapsed")
	}
	wantMidnight := []int64{time.Date(2024, 3, 10, 5, 0, 0, 0, time.UTC).UnixNano(), time.Date(2024, 3, 11, 4, 0, 0, 0, time.UTC).UnixNano()}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			manager := memory.NewBudgetManager(0)
			w, err := NewWorkspace(context.Background(), p, manager)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for _, n := range []int{0, 1, 63, 64, 65, 513, 2, 130, 0} {
				batch := memory.NewBatch()
				batch.Rows = n
				for _, name := range []string{"guard", "year", "month", "day"} {
					batch.Int64s[name] = &memory.Int64Vector{Values: make([]int64, n)}
				}
				for _, name := range []string{"a", "b"} {
					batch.Decimals[name] = &memory.Decimal128Vector{Values: make([]memory.Decimal128, n), Precision: 6, Scale: 2}
				}
				for row := 0; row < n; row++ {
					batch.Int64s["guard"].Values[row] = 1
					batch.Int64s["year"].Values[row] = 2024
					batch.Int64s["month"].Values[row] = 3
					batch.Int64s["day"].Values[row] = 10 + int64(row%2)
					batch.Decimals["a"].Values[row] = decimal128.FromI64(125)
					batch.Decimals["b"].Values[row] = decimal128.FromI64(200)
					if row%3 == 0 {
						batch.Int64s["guard"].Values[row] = 0
						batch.Int64s["month"].Values[row] = 99                       // Invalid calendar row is not demanded.
						batch.Decimals["a"].Values[row] = decimal128.FromI64(999999) // Would overflow rounded target.
						batch.Decimals["b"].Values[row] = decimal128.FromI64(0)      // Would divide by zero.
					}
				}
				nums := make([]int64, 0, 3*n)
				for row := 0; row < n; row++ {
					nums = append(nums, batch.Int64s["year"].Values[row], batch.Int64s["month"].Values[row], batch.Int64s["day"].Values[row])
				}
				for _, name := range []string{"year", "month", "day"} {
					delete(batch.Int64s, name)
				}
				outerOffsets, innerOffsets := make([]uint32, n+1), make([]uint32, n+1)
				for i := 0; i <= n; i++ {
					outerOffsets[i] = uint32(i)
					innerOffsets[i] = uint32(3 * i)
				}
				leaf := &memory.ArrayNode{Kind: memory.TypeInt64, Int64: &memory.Int64Vector{Values: nums}}
				middle := &memory.ArrayNode{Kind: memory.TypeList, Offsets: innerOffsets, Child: leaf}
				batch.Arrays["xs"] = &memory.ArrayNode{Kind: memory.TypeList, Offsets: outerOffsets, Child: middle}
				out, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
				if err != nil {
					t.Fatalf("batch %d: %v", n, err)
				}
				for row := 0; row < n; row++ {
					valid := row%3 != 0
					for col := 0; col < 4; col++ {
						v := out[col].Decimal
						if v.IsValid(row) != valid {
							t.Fatalf("decimal validity batch%d row%d col%d", n, row, col)
						}
						coefficient, precision, scale := int64(12), int32(3), int32(1)
						if col >= 2 {
							coefficient, precision, scale = 62, 5, 2
						}
						if v.Precision != precision || v.Scale != scale || valid && v.Values[row] != decimal128.FromI64(coefficient) {
							t.Fatalf("decimal result batch%d row%d col%d", n, row, col)
						}
					}
					for _, col := range []int{4, 5} {
						if out[col].Type != memory.TypeDate32 || out[col].Int32.IsValid(row) != valid || valid && out[col].Int32.Values[row] != 19792+int32(row%2) {
							t.Fatalf("date batch%d row%d col%d", n, row, col)
						}
					}
					for _, col := range []int{6, 7, 8} {
						if out[col].Int64.IsValid(row) != valid {
							t.Fatalf("temporal validity batch%d row%d col%d", n, row, col)
						}
					}
					if valid && (out[6].Type != memory.TypeTimestamp || out[6].Int64.Values[row] != wantMidnight[row%2] || out[7].Int64.Values[row] != 0 || out[8].Int64.Values[row] != []int64{-18000, -14400}[row%2]) {
						t.Fatalf("midnight/zoned result batch%d row%d", n, row)
					}
				}
				if manager.Used() != w.MemoryBytes() {
					t.Fatalf("accounting batch%d used%d workspace%d", n, manager.Used(), w.MemoryBytes())
				}
			}
			w.Close()
			if manager.Used() != 0 {
				t.Fatal("workspace Close leaked")
			}
		})
	}
}
