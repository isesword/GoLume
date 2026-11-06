package expr

import "gocolumn/memory"

// Type returns the logical scalar root type. Multi-output programs have no
// scalar root; inspect their output fields instead.
func (p Program) Type() memory.DataType { return p.typ }

// Nullable reports whether the scalar result can be NULL.
func (p Program) Nullable() bool { return p.nullable }

// InstructionCount includes instructions in every guarded evaluation region.
func (p Program) InstructionCount() int {
	n := len(p.instructions)
	for _, in := range p.instructions {
		for _, region := range in.Regions {
			n += region.InstructionCount()
		}
	}
	return n
}

// ResultField returns an independent, complete scalar result descriptor.
func (p Program) ResultField() memory.Field { return cloneBoundField(p.result) }

// OutputCount returns the number of named outputs.
func (p Program) OutputCount() int { return len(p.outputs) }

// OutputName returns a named output alias, or an empty string for an invalid index.
func (p Program) OutputName(index int) string {
	if index < 0 || index >= len(p.outputs) {
		return ""
	}
	return p.outputs[index].Name
}

// OutputField returns an independent named output descriptor, or an empty
// field for an index outside the program's outputs.
func (p Program) OutputField(index int) memory.Field {
	if index < 0 || index >= len(p.outputs) {
		return memory.Field{}
	}
	return cloneBoundField(p.outputs[index].Field)
}

// OutputFields returns independent descriptors for every named output.
func (p Program) OutputFields() []memory.Field {
	fields := make([]memory.Field, len(p.outputs))
	for i := range fields {
		fields[i] = p.OutputField(i)
	}
	return fields
}
