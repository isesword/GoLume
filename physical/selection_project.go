package physical

import (
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

// supportsSelectedProject negotiates a private, fixed-width pipeline edge.
// Unknown producers, mixed/nested schemas, and non-row-local programs keep the
// established materialized boundary. The selection never escapes physical.
func supportsSelectedProject(input source.Source, named []expr.NamedExpr, includeInput bool, program expr.Program) bool {
	// Retaining the input already requires gathering essentially the full batch,
	// and expanding passthroughs increases expression metadata even for all-pass
	// batches. Keep that established zero-copy/passthrough path in this stage.
	if includeInput {
		return false
	}
	if _, ok := input.(*FilterSource); !ok || !program.SupportsSelection() {
		return false
	}
	for _, f := range input.Schema().Fields {
		if f.Type == memory.TypeDecimal && f.ValidateDecimal128() == nil {
			continue
		}
		if !selectionFixedWidth(f.Type) {
			return false
		}
	}
	// Selection-aware evaluation pays off by avoiding columns, not by replacing
	// a full-width SIMD compaction with equivalent per-column gathers.
	required := make(map[string]struct{})
	for _, n := range named {
		for _, name := range expr.Columns(n.Expr) {
			required[name] = struct{}{}
		}
	}
	return len(required) < len(input.Schema().Fields)
}

func selectionFixedWidth(t memory.DataType) bool {
	switch t {
	case memory.TypeBool, memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeDate32, memory.TypeDate64,
		memory.TypeTime32, memory.TypeTime64, memory.TypeTimestamp, memory.TypeDuration:
		return true
	}
	return false
}
