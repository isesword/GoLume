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

// ForwardFill replaces NULL with the nearest preceding non-NULL value in each
// ordered partition. Leading NULLs remain NULL; NaN is a non-NULL value.
func ForwardFill(column, alias string) WindowExpr {
	return offsetWindow(logical.WindowForwardFill, column, 0, alias)
}

// BackwardFill replaces NULL with the nearest following non-NULL value in each
// ordered partition. Trailing NULLs remain NULL; NaN is a non-NULL value. Ordered
// execution requires a positive memory budget and fails when lookahead exceeds it.
func BackwardFill(column, alias string) WindowExpr {
	return offsetWindow(logical.WindowBackwardFill, column, 0, alias)
}
