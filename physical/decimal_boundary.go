package physical

import (
	"fmt"
	"gocolumn/memory"
)

// Native decimal scan, projection, null checks, conditional selection and row
// compaction preserve exact coefficients. Join, ordering and window operators
// still require dedicated typed paths before accepting decimal payloads.
func rejectNativeDecimalExecution(schema memory.Schema) error {
	if schema.HasNestedLists() {
		return fmt.Errorf("two-level array execution unsupported by this operator")
	}
	if schema.HasStructLists() {
		return fmt.Errorf("StructList execution is unsupported by this operator; use scan/projection/filter/owned Collect/IPC")
	}
	return rejectSortDecimalExecution(schema, true)
}

func rejectSortDecimalExecution(schema memory.Schema, allowStructList bool) error {
	if schema.HasNestedLists() && !allowStructList {
		return fmt.Errorf("two-level array execution is not supported by this operator; use scan/projection/filter/owned Collect/IPC")
	}
	if schema.HasStructLists() && !allowStructList {
		return fmt.Errorf("StructList sort requires bounded spill execution")
	}
	for _, field := range schema.Fields {
		if field.Type == memory.TypeDecimal && field.DecimalPrecision != 0 {
			return fmt.Errorf("native decimal execution is not supported yet by this operator for column %q; scan, projection, filter transport and Arrow export preserve exact decimal128 values", field.Name)
		}
	}
	return nil
}
