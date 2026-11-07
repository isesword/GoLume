package expr

// FillNull replaces NULL values using the existing Coalesce type and lazy
// branch-evaluation contract. NaN is a valid floating value and is unchanged.
func FillNull(value, replacement Expr) Expr { return Coalesce(value, replacement) }

// FillNaN replaces floating NaNs while retaining NULL values. Non-floating
// comparable values have no NaNs and pass through unchanged. Branch binding
// follows If's existing common-type rules; use an explicit replacement cast
// when a particular output width is required.
func FillNaN(value, replacement Expr) Expr {
	return If(IsNull(value), value, If(EQ(value, value), value, replacement))
}
