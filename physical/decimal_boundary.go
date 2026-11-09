package physical

import (
	"fmt"
	"gocolumn/memory"
)

// Native decimal scan, projection, null checks, conditional selection and row
// compaction preserve exact coefficients. Join, ordering and window operators
// still require dedicated typed paths before accepting decimal payloads.
func rejectNativeDecimalExecution(schema memory.Schema) error {
	for _, field := range schema.Fields {
		if field.Type == memory.TypeDecimal && field.DecimalPrecision != 0 {
			return fmt.Errorf("native decimal execution is not supported yet by this operator for column %q; scan, projection, filter transport and Arrow export preserve exact decimal128 values", field.Name)
		}
	}
	return nil
}
