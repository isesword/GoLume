package physical

import (
	"fmt"
	"gocolumn/memory"
)

// Native decimal scan, range transport and Arrow export are implemented first.
// Reordering/compacting/expression operators must not silently drop the new
// bindings or reinterpret coefficients until their typed paths are supported.
func rejectNativeDecimalExecution(schema memory.Schema) error {
	for _, field := range schema.Fields {
		if field.Type == memory.TypeDecimal && field.DecimalPrecision != 0 {
			return fmt.Errorf("native decimal execution is not supported yet for column %q; scan, range transport and Arrow export preserve exact decimal128 values", field.Name)
		}
	}
	return nil
}
