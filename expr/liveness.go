package expr

import "gocolumn/memory"

// planStorage assigns physical owners independently of logical DAG registers.
// Inputs remain live throughout their consuming instruction: kernels are never
// asked to execute in place. Every exported root is live through evaluation.
// Each guarded region is compiled separately and owns an independent plan.
// Physical owners are dense, with first introductions in increasing order,
// so workspace metadata never needs holes for dead logical registers.
func (p *Program) planStorage() {
	p.planLiterals()
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
	type storageClass struct {
		typ     memory.DataType
		compact bool
	}
	class := func(in instruction) storageClass { return storageClass{instructionResultType(in), in.compactLiteral} }
	free := make(map[storageClass][]int)
	ownerClass := make([]storageClass, 0, n)
	owners := 0
	for i := range p.instructions {
		in := &p.instructions[i]
		in.storageOwner = 0
		if reusableInstruction(*in) {
			typ := class(*in)
			available := free[typ]
			// A scalar can reuse an already admitted dense owner without
			// filling its vector. The reverse would need unplanned storage.
			if len(available) == 0 && typ.compact {
				typ.compact = false
				available = free[typ]
			}
			if len(available) > 0 {
				in.storageOwner = available[len(available)-1] + 1
				free[typ] = available[:len(available)-1]
			}
			if last[i] < n {
				release[last[i]] = append(release[last[i]], i)
			}
		}
		if in.storageOwner == 0 {
			ownerClass = append(ownerClass, class(*in))
			owners++
			in.storageOwner = owners
		}
		// Only subsequent instructions may overwrite values consumed here.
		for _, dead := range release[i] {
			typ := ownerClass[p.instructions[dead].storageOwner-1]
			free[typ] = append(free[typ], p.instructions[dead].storageOwner-1)
		}
	}
}

func instructionInputs(in instruction) []int {
	switch in.Code {
	case scalarBinary, scalarCompare, scalarStringMatch, codeAnd, codeOr:
		return []int{in.A, in.B}
	case scalarCast, scalarTryCast, scalarUnary, scalarJSONDecode, codeNot:
		return []int{in.A}
	case scalarBoolean:
		if in.Op == OpAnd || in.Op == OpOr {
			return []int{in.A, in.B}
		}
		return []int{in.A}
	case scalarTernary:
		return []int{in.A, in.B, in.C}
	case scalarConditional, scalarInSet:
		return []int{in.A}
	default:
		return nil
	}
}

func reusableInstruction(in instruction) bool {
	// Borrowed columns, nested regions, and instruction-specific caches retain
	// dedicated owners. Variable-width results are deliberately not pooled.
	if in.Code == scalarColumn || in.Code == scalarConditional || in.Code == scalarInSet || in.Code == scalarJSONDecode || in.LiteralString != "" {
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

// storageOwnerCount is independent of logical register count. Planned owners
// are dense and introduced in increasing order; unplanned private programs use
// their logical instruction indices as dedicated owners.
func storageOwnerCount(instructions []instruction) int {
	owners := 0
	for i := range instructions {
		owners = max(owners, storageIndex(instructions, i)+1)
	}
	return owners
}

// borrowedRegisterCount counts input views separately from physical owners.
// Guarded scratch programs can reuse an owner's typed capacities across arms;
// their input slices must never overwrite those owned buffer headers.
func borrowedRegisterCount(instructions []instruction) int {
	count := 0
	for _, in := range instructions {
		if in.Code == scalarColumn {
			count++
		}
	}
	return count
}
