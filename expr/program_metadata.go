package expr

import "gocolumn/memory"

// Type returns the logical scalar root type. Multi-output programs have no
// scalar root; inspect their output fields instead.
func (p Program) Type() memory.DataType { return p.typ }

// Nullable reports whether the scalar result can be NULL.
func (p Program) Nullable() bool { return p.nullable }

// InstructionCount includes instructions in every guarded evaluation region.
// Repeated uses of shared region code count separately, preserving logical
// evaluation complexity. Counts that exceed the range of int saturate at MaxInt.
func (p Program) InstructionCount() int {
	counts := make(map[*instruction]int)
	var count func(Program) int
	count = func(p Program) int {
		if len(p.instructions) == 0 {
			return 0
		}
		key := &p.instructions[0]
		if n, ok := counts[key]; ok {
			return n
		}
		n := len(p.instructions)
		const maxInt = int(^uint(0) >> 1)
		for _, in := range p.instructions {
			for _, region := range in.Regions {
				child := count(region)
				if child > maxInt-n {
					n = maxInt
				} else {
					n += child
				}
			}
		}
		counts[key] = n
		return n
	}
	return count(p)
}

// visitInstructions walks immutable program storage rather than its expanded
// region tree. Compilation may share a complete instruction slice across many
// domains; the first instruction identifies that independently allocated slice.
func (p Program) visitInstructions(visit func(instruction) bool) bool {
	seen := make(map[*instruction]struct{})
	var walk func(Program) bool
	walk = func(p Program) bool {
		if len(p.instructions) == 0 {
			return true
		}
		key := &p.instructions[0]
		if _, ok := seen[key]; ok {
			return true
		}
		seen[key] = struct{}{}
		for _, in := range p.instructions {
			if !visit(in) {
				return false
			}
			for _, region := range in.Regions {
				if !walk(region) {
					return false
				}
			}
		}
		return true
	}
	return walk(p)
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
