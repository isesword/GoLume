package logical

import (
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestPercentRankLogicalContract(t *testing.T) {
	if WindowBackwardFill != 12 || WindowPercentRank != 13 {
		t.Fatal("window enum compatibility")
	}
	// An existing unkeyed four-field literal still compiles.
	ex := WindowExpr{WindowPercentRank, "", RowsFrame{Start: RowBound{Kind: RowFollowing, Offset: 8}, End: RowBound{Kind: RowPreceding, Offset: 9}}, "p"}
	f, e := inferWindowField(memory.NewSchema(), ex)
	if e != nil || f.Type != memory.TypeFloat64 || f.Nullable || f.Name != "p" {
		t.Fatal("field", f, e)
	}
	if !strings.Contains(windowExprString(ex), "percent_rank()") {
		t.Fatal("explain")
	}
	ex.Column = "unexpected"
	if _, e := inferWindowField(memory.NewSchema(), ex); e == nil {
		t.Fatal("accepted input column")
	}
}
