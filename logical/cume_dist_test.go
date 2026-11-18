package logical

import (
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestCumeDistLogicalContract(t *testing.T) {
	if WindowPercentRank != 13 || WindowCumeDist != 14 {
		t.Fatal("enum compatibility")
	}
	ex := WindowExpr{WindowCumeDist, "", RowsFrame{Start: RowBound{Kind: RowFollowing, Offset: 9}, End: RowBound{Kind: RowPreceding, Offset: 7}}, "cd"}
	field, e := inferWindowField(memory.NewSchema(), ex)
	if e != nil || field.Type != memory.TypeFloat64 || field.Nullable || field.Name != "cd" {
		t.Fatal("metadata", field, e)
	}
	if !strings.Contains(windowExprString(ex), "cume_dist()") {
		t.Fatal("explain")
	}
	ex.Column = "v"
	if _, e = inferWindowField(memory.NewSchema(), ex); e == nil {
		t.Fatal("unexpected input column")
	}
}
