package logical

import (
	"gocolumn/memory"
	"math"
	"strings"
	"testing"
)

func TestVarianceLogicalContract(t *testing.T) {
	if WindowNTile != 15 || WindowVarPop != 16 || WindowVarSamp != 17 {
		t.Fatal("enum compatibility")
	}
	frames := []RowsFrame{
		{Start: RowBound{Kind: RowPreceding, Offset: 3}, End: RowBound{Kind: RowCurrent}},
		{Start: RowBound{Kind: RowFollowing, Offset: 1}, End: RowBound{Kind: RowFollowing, Offset: 3}},
		{Start: RowBound{Kind: RowPreceding, Offset: 3}, End: RowBound{Kind: RowPreceding, Offset: 1}},
		{Start: RowBound{Kind: RowUnboundedPreceding}, End: RowBound{Kind: RowCurrent}},
	}
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64, memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64} {
		for _, function := range []WindowFunction{WindowVarPop, WindowVarSamp} {
			for _, frame := range frames {
				e := WindowExpr{function, "v", frame, "variance"}
				got, err := inferWindowField(memory.NewSchema(memory.Field{Name: "v", Type: typ}), e)
				if err != nil || got.Type != memory.TypeFloat64 || !got.Nullable || got.Encoding != memory.EncodingPlain || got.ArrowType != "" || len(got.Children) != 0 {
					t.Fatal(typ, e, got, err)
				}
				if !strings.Contains(windowExprString(e), "var_") {
					t.Fatal("Explain", e)
				}
				if err := ValidateWindowConfiguration(ConfiguredWindowExpr{Expr: e}); err != nil {
					t.Fatal(err)
				}
				if err := ValidateWindowConfiguration(ConfiguredWindowExpr{Expr: e, Buckets: 1}); err == nil {
					t.Fatal("irrelevant buckets")
				}
			}
		}
	}
	schema := memory.NewSchema(memory.Field{Name: "v", Type: memory.TypeInt64})
	bad := []RowsFrame{
		{Start: RowBound{Kind: RowCurrent}, End: RowBound{Kind: RowUnboundedFollowing}},
		{Start: RowBound{Kind: RowUnboundedPreceding}, End: RowBound{Kind: RowFollowing, Offset: 1}},
		{Start: RowBound{Kind: RowPreceding, Offset: -1}, End: RowBound{Kind: RowCurrent}},
		{Start: RowBound{Kind: RowFollowing, Offset: 2}, End: RowBound{Kind: RowFollowing, Offset: 1}},
		{Start: RowBound{Kind: RowPreceding, Offset: math.MaxInt64}, End: RowBound{Kind: RowFollowing, Offset: math.MaxInt64}},
		{Start: RowBound{Kind: RowCurrent, Offset: 1}, End: RowBound{Kind: RowFollowing, Offset: 2}},
	}
	for _, frame := range bad {
		if _, err := inferWindowField(schema, WindowExpr{WindowVarPop, "v", frame, "bad"}); err == nil {
			t.Fatal("accepted", frame)
		}
	}
	for _, typ := range []memory.DataType{memory.TypeString, memory.TypeBool, memory.TypeTimestamp, memory.TypeDecimal} {
		if _, err := inferWindowField(memory.NewSchema(memory.Field{Name: "v", Type: typ}), WindowExpr{WindowVarSamp, "v", frames[0], "bad"}); err == nil {
			t.Fatal("accepted type", typ)
		}
	}
}
