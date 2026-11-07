package expr

import "gocolumn/memory"

// planStorage assigns physical owners independently of logical DAG registers.
// Inputs remain live throughout their consuming instruction: kernels are never
// asked to execute in place. Every exported root is live through evaluation.
// Each guarded region is compiled separately and owns an independent plan.
func (p *Program) planStorage() {
	n := len(p.instructions)
	last := make([]int, n)
	for i := range last {
		last[i] = i
	}
	for i, in := range p.instructions {
		for _, input := range instructionInputs(in) {
			if input >= 0 && input < i {
				last[input] = i
			}
		}
	}
	if p.root >= 0 && p.root < n {
		last[p.root] = n
	}
	for _, out := range p.outputs {
		last[out.root] = n
	}
	// Release lists and type-specific free lists make allocation linear in the
	// instruction count. Slot types are exact, including temporal units; no
	// unsafe reinterpretation or aliasing of borrowed columns is involved.
	release := make([][]int, n)
	free := make(map[memory.DataType][]int)
	for i := range p.instructions {
		in := &p.instructions[i]
		in.storageOwner = i + 1
		if reusableInstruction(*in) {
			typ := instructionResultType(*in)
			available := free[typ]
			if len(available) > 0 {
				in.storageOwner = available[len(available)-1] + 1
				free[typ] = available[:len(available)-1]
			}
			if last[i] < n {
				release[last[i]] = append(release[last[i]], i)
			}
		}
		// Only subsequent instructions may overwrite values consumed here.
		for _, dead := range release[i] {
			typ := instructionResultType(p.instructions[dead])
			free[typ] = append(free[typ], p.instructions[dead].storageOwner-1)
		}
	}
}

func instructionInputs(in instruction) []int {
	switch in.Code {
	case scalarBinary, scalarCompare, scalarStringMatch, codeAnd, codeOr:
		return []int{in.A, in.B}
	case scalarCast, scalarUnary, scalarJSONDecode, codeNot:
		return []int{in.A}
	case scalarBoolean:
		if in.Op == OpAnd || in.Op == OpOr {
			return []int{in.A, in.B}
		}
		return []int{in.A}
	case scalarTernary:
		return []int{in.A, in.B, in.C}
	case scalarConditional:
		return []int{in.A}
	default:
		return nil
	}
}

func reusableInstruction(in instruction) bool {
	// Borrowed columns, nested regions, and instruction-specific caches retain
	// dedicated owners. Variable-width results are deliberately not pooled.
	if in.Code == scalarColumn || in.Code == scalarConditional || in.Code == scalarJSONDecode || in.LiteralString != "" {
		return false
	}
	if in.Code == codeStringMatch || (in.Code == codeCmpScalar && (in.Type == memory.TypeString || in.Type == memory.TypeBinary)) || in.Code == codeCmpExtract {
		return false
	}
	switch instructionResultType(in) {
	case memory.TypeBool, memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeDate32, memory.TypeDate64,
		memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
		return true
	default:
		return false
	}
}

func storageIndex(instructions []instruction, index int) int {
	if owner := instructions[index].storageOwner; owner != 0 {
		return owner - 1
	}
	// Private executor tests may construct unplanned instructions directly.
	return index
}

func (w *evaluationState) ownedRegister(index int) *scalarRegister {
	return &w.owned[storageIndex(w.program, index)]
}
