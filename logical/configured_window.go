package logical

import "fmt"

// ConfiguredWindowExpr pairs one window expression with its function-specific configuration.
// Buckets must be positive for NTILE and zero for every other function.
type ConfiguredWindowExpr struct {
	Expr    WindowExpr
	Buckets int64
}

// ConfiguredWindow retains positional configuration without changing Window's public shape.
type ConfiguredWindow struct {
	Input Plan
	Spec  WindowSpec
	Exprs []ConfiguredWindowExpr
}

func (*ConfiguredWindow) planNode() {}

// WindowExpressions returns an independent expression slice in configuration order.
func (n *ConfiguredWindow) WindowExpressions() []WindowExpr {
	out := make([]WindowExpr, len(n.Exprs))
	for i, e := range n.Exprs {
		out[i] = e.Expr
	}
	return out
}

// ValidateWindowConfiguration rejects missing, irrelevant and non-canonical parameters.
func ValidateWindowConfiguration(e ConfiguredWindowExpr) error {
	if e.Expr.Function == WindowNTile {
		if e.Buckets <= 0 || e.Expr.Column != "" || e.Expr.Frame != (RowsFrame{}) {
			return fmt.Errorf("NTILE requires positive buckets, no column and a zero frame")
		}
	} else if e.Buckets != 0 {
		return fmt.Errorf("buckets are only valid for NTILE")
	}
	return nil
}
