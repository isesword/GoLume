package physical

import (
	"context"
	"gocolumn/memory"
)

// Native Decimal is validated even in empty or nonwinner batches. Opaque roots
// retain their separate transport contract; only actual typed containers are
// subject to the native nested-Decimal boundary.
func validateTopKDecimalInput(ctx context.Context, schema memory.Schema, b *memory.Batch) error {
	if err := validateResidentTypedDecimalBoundary(ctx, schema, b); err != nil {
		return err
	}
	if residentHasNativeDecimal(schema.Fields) {
		return validateResidentDecimalBatch(ctx, schema, b)
	}
	return ctx.Err()
}
