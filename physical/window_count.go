package physical

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// validateCountWindow mirrors the logical count contract for callers that
// construct physical sources directly, including empty-input execution.
func validateCountWindow(schema memory.Schema, expression WindowExpression, output memory.Field) error {
	if expression.Alias == "" || output.Name != expression.Alias || output.Type != memory.TypeInt64 || output.Nullable || output.Encoding != memory.EncodingPlain {
		return fmt.Errorf("window count output field disagrees with expression")
	}
	if expression.Function == WindowCountRows {
		if expression.Column != "" {
			return fmt.Errorf("window count rows %q does not accept an input column", expression.Alias)
		}
	} else {
		if expression.Column == "" {
			return fmt.Errorf("window count %q requires an input column", expression.Alias)
		}
		if _, ok := schema.Field(expression.Column); !ok {
			return fmt.Errorf("window column %q not found", expression.Column)
		}
	}
	start, err := countWindowBoundOrder(expression.Frame.Start)
	if err != nil {
		return fmt.Errorf("window count %q: %w", expression.Alias, err)
	}
	end, err := countWindowBoundOrder(expression.Frame.End)
	if err != nil {
		return fmt.Errorf("window count %q: %w", expression.Alias, err)
	}
	if start > end {
		return fmt.Errorf("window count %q: frame start is after frame end", expression.Alias)
	}
	return nil
}

// Keep the existing logical ROWS ordering, including sentinel endpoints and
// zero preceding/following offsets, without converting through platform int.
func countWindowBoundOrder(bound WindowBound) (int64, error) {
	switch bound.Kind {
	case WindowUnboundedPreceding:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("unbounded preceding cannot have an offset")
		}
		return math.MinInt64, nil
	case WindowPreceding:
		if bound.Offset < 0 {
			return 0, fmt.Errorf("preceding offset must be non-negative")
		}
		return -bound.Offset, nil
	case WindowCurrentRow:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("current row cannot have an offset")
		}
		return 0, nil
	case WindowFollowing:
		if bound.Offset < 0 {
			return 0, fmt.Errorf("following offset must be non-negative")
		}
		return bound.Offset, nil
	case WindowUnboundedFollowing:
		if bound.Offset != 0 {
			return 0, fmt.Errorf("unbounded following cannot have an offset")
		}
		return math.MaxInt64, nil
	default:
		return 0, fmt.Errorf("unknown frame boundary %d", bound.Kind)
	}
}
