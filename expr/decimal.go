package expr

import (
	"fmt"
	"gocolumn/memory"
)

func validateDecimalColumn(batch *memory.Batch, name string, field *memory.Field) error {
	v, err := batch.Decimal(name)
	if err != nil {
		return err
	}
	if batch.Rows < 0 || len(v.Values) < batch.Rows {
		return fmt.Errorf("decimal column %q is shorter than batch", name)
	}
	view := *v
	view.Values = view.Values[:batch.Rows]
	if err := view.ValidateField(*field, batch.Rows); err != nil {
		return fmt.Errorf("decimal column %q: %w", name, err)
	}
	return nil
}
