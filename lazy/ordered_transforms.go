package lazy

import "gocolumn/logical"

// Shift returns the value periods rows earlier in the window ordering. Negative
// periods read later rows. Exposed positions are NULL; partition boundaries are
// never crossed. Use WithWindows with an explicit ordering specification.
func Shift(column string, periods int64, alias string) WindowExpr {
	return offsetWindow(logical.WindowShift, column, periods, alias)
}

// Diff subtracts the value periods rows earlier from the current value. Either
// missing endpoint produces NULL. Negative periods compare with later rows.
func Diff(column string, periods int64, alias string) WindowExpr {
	return offsetWindow(logical.WindowDiff, column, periods, alias)
}

func offsetWindow(kind logical.WindowFunction, column string, periods int64, alias string) WindowExpr {
	bound := logical.RowBound{Kind: logical.RowCurrent}
	if periods > 0 {
		bound = logical.RowBound{Kind: logical.RowPreceding, Offset: periods}
	}
	if periods < 0 {
		bound = logical.RowBound{Kind: logical.RowFollowing, Offset: -periods}
	} // MinInt64 remains negative and fails schema validation.
	return logical.WindowExpr{Function: kind, Column: column, Frame: logical.RowsFrame{Start: bound, End: bound}, Alias: alias}
}
