package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math/bits"
	"unsafe"
)

const (
	regionIf uint8 = iota + 1
	regionCase
	regionCoalesce
	regionIn
)

func (c *scalarCompiler) compileConditional(bound *BoundExpr) (instruction, error) {
	if _, ok := bound.Expr.(InExpr); ok {
		if set := compileLiteralIn(bound); set != nil {
			root, _, err := c.compileBound(bound.Children[0])
			return set.instruction(root), err
		}
	}
	in := instruction{Code: scalarConditional, Type: bound.Field.Type, A: -1, B: -1, C: -1}
	children := bound.Children
	switch bound.Expr.(type) {
	case IfExpr:
		in.Kind = regionIf
	case CaseExpr:
		in.Kind = regionCase
	case CoalesceExpr:
		in.Kind = regionCoalesce
	case InExpr:
		in.Kind = regionIn
	}
	if in.Kind == regionIf || in.Kind == regionIn {
		root, _, err := c.compileBound(children[0])
		if err != nil {
			return in, err
		}
		in.A = root
		children = children[1:]
	}
	for _, child := range children {
		region, err := c.compileRegion(child)
		if err != nil {
			return in, err
		}
		in.Regions = append(in.Regions, region)
	}
	return in, nil
}

// compileRegion reuses immutable standalone code, never a register or execution
// state from another evaluation domain. Every cache miss starts with an empty
// register memo and finishes storage planning before publishing the program.
// Thus shared nested branches form a program DAG without hoisting partial work.
func (c *scalarCompiler) compileRegion(bound *BoundExpr) (Program, error) {
	if c.regions == nil {
		c.regions = make(map[int]Program)
	}
	key := c.structures.expression(bound)
	if region, ok := c.regions[key]; ok {
		return region, nil
	}
	compiler := &scalarCompiler{schema: c.schema, memo: make(map[int]int), structures: c.structures, regions: c.regions}
	root, info, err := compiler.compileBound(bound)
	if err != nil {
		return Program{}, err
	}
	region := Program{instructions: compiler.instructions, root: root, typ: info.Type, nullable: info.Nullable, complexity: len(compiler.instructions)}
	region.planStorage()
	c.regions[key] = region
	return region, nil
}

func (w *evaluationState) applyActive(index int, r *scalarRegister, rows int) error {
	if w.activeRows == nil {
		return nil
	}
	owned := w.ownedRegister(index)
	if err := scalarResize(&owned.ownedValidity, memory.WordsFor(rows), &owned.buffers[0], w.acquire); err != nil {
		return err
	}
	validity := r.validity
	dst := owned.ownedValidity
	for i := range dst {
		word := ^uint64(0)
		if validity != nil {
			word = validity[i]
		}
		dst[i] = word & w.activeRows[i]
	}
	r.validity = dst
	if r.typ == memory.TypeBool {
		for i := range r.Truth {
			r.Truth[i] &= dst[i]
		}
	}
	return nil
}

func (w *evaluationState) evalRegion(out *scalarRegister, index int, program Program, batch *memory.Batch, kernels kernel.Set) (*scalarRegister, error) {
	if memory.CountBitmap(out.active) == 0 {
		return nil, nil
	}
	slot := index
	if len(out.regions) == 1 {
		slot = 0
	}
	child := out.regions[slot]
	child.ctx = w.ctx
	child.manager = w.manager
	child.reserveHook = w.reserveHook
	if err := evaluateInstructionsActive(program.instructions, batch, child, kernels, out.active); err != nil {
		return nil, err
	}
	return &child.regs[program.root], nil
}

func (w *evaluationState) evaluateConditional(out *scalarRegister, in instruction, batch *memory.Batch, kernels kernel.Set) error {
	rows := batch.Rows
	stream := streamingConditional(in)
	regionCount := len(in.Regions)
	if stream {
		regionCount = 1
	}
	if len(out.regions) != regionCount {
		// Metadata is owned by the parent; each region separately admits its typed
		// register capacities only when a non-empty demand reaches it.
		size, err := scalarBytes(regionCount, unsafe.Sizeof((*evaluationState)(nil))+unsafe.Sizeof(evaluationState{}))
		if err != nil {
			return err
		}
		lease, err := w.acquire(size)
		if err != nil {
			return err
		}
		next := make([]*evaluationState, regionCount)
		for i := range next {
			next[i] = &evaluationState{regionScratch: stream}
		}
		for _, old := range out.regions {
			_ = old.Close()
		}
		if out.regionReservation != nil {
			out.regionReservation.Release()
		}
		out.regions = next
		out.regionReservation = lease
	}
	if err := scalarResize(&out.active, memory.WordsFor(rows), &out.buffers[15], w.acquire); err != nil {
		return err
	}
	if err := scalarResize(&out.choices, rows, &out.buffers[16], w.acquire); err != nil {
		return err
	}
	for row := range out.choices {
		out.choices[row] = -1
	}
	if stream {
		out.validity = out.ownedValidity
		clear(out.validity)
		if out.typ == memory.TypeBool {
			clear(out.Truth)
		}
	}
	merge := func(value *scalarRegister) error {
		if !stream || value == nil {
			return nil
		}
		for word, pending := range out.active {
			for pending != 0 {
				row := word*64 + bits.TrailingZeros64(pending)
				pending &= pending - 1
				if isValidRegister(value, row) {
					if err := copyConditionalRow(out, value, row); err != nil {
						return err
					}
					memory.Set(out.validity, row)
				}
			}
		}
		return nil
	}
	evalValue := func(branch int) error {
		value, err := w.evalRegion(out, branch, in.Regions[branch], batch, kernels)
		if err != nil {
			return err
		}
		return merge(value)
	}
	setRemaining := func() {
		clear(out.active)
		for row := 0; row < rows; row++ {
			if out.choices[row] < 0 && (w.activeRows == nil || memory.IsSet(w.activeRows, row)) {
				memory.Set(out.active, row)
			}
		}
	}
	switch in.Kind {
	case regionIf:
		condition := &w.regs[in.A]
		for branch := 0; branch < 2; branch++ {
			clear(out.active)
			for row := 0; row < rows; row++ {
				if w.activeRows != nil && !memory.IsSet(w.activeRows, row) {
					continue
				}
				then := isValidRegister(condition, row) && memory.IsSet(condition.Truth, row)
				if then == (branch == 0) {
					memory.Set(out.active, row)
					out.choices[row] = branch
				}
			}
			if err := evalValue(branch); err != nil {
				return err
			}
		}
	case regionCase:
		for branch := 0; branch < len(in.Regions)-1; branch += 2 {
			setRemaining()
			condition, err := w.evalRegion(out, branch, in.Regions[branch], batch, kernels)
			if err != nil {
				return err
			}
			if condition == nil {
				continue
			}
			for row := 0; row < rows; row++ {
				if memory.IsSet(out.active, row) && isValidRegister(condition, row) && memory.IsSet(condition.Truth, row) {
					out.choices[row] = branch + 1
				} else {
					out.active[row>>6] &^= uint64(1) << uint(row&63)
				}
			}
			if err := evalValue(branch + 1); err != nil {
				return err
			}
		}
		last := len(in.Regions) - 1
		setRemaining()
		for row := 0; row < rows; row++ {
			if memory.IsSet(out.active, row) {
				out.choices[row] = last
			}
		}
		if err := evalValue(last); err != nil {
			return err
		}
	case regionCoalesce:
		for branch, program := range in.Regions {
			setRemaining()
			value, err := w.evalRegion(out, branch, program, batch, kernels)
			if err != nil {
				return err
			}
			if value == nil {
				continue
			}
			for row := 0; row < rows; row++ {
				if memory.IsSet(out.active, row) && isValidRegister(value, row) {
					out.choices[row] = branch
					if stream {
						if err := copyConditionalRow(out, value, row); err != nil {
							return err
						}
						memory.Set(out.validity, row)
					}
				}
			}
		}
	case regionIn:
		needle := &w.regs[in.A]
		clear(out.Truth)
		out.validity = out.ownedValidity
		fillValidity(out.validity, needle.validity, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(needle, row) {
				out.choices[row] = 0
			}
		}
		for branch, program := range in.Regions {
			setRemaining()
			candidate, err := w.evalRegion(out, branch, program, batch, kernels)
			if err != nil {
				return err
			}
			if candidate == nil {
				continue
			}
			for row := 0; row < rows; row++ {
				if !memory.IsSet(out.active, row) {
					continue
				}
				if !isValidRegister(candidate, row) {
					out.validity[row>>6] &^= uint64(1) << uint(row&63)
					continue
				}
				equal, err := compareRegisters(needle, candidate, row, OpEQ)
				if err != nil {
					return err
				}
				if equal {
					memory.Set(out.Truth, row)
					memory.Set(out.validity, row)
					out.choices[row] = branch
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid expression region kind %d", in.Kind)
	}
	if stream {
		return nil
	}
	out.validity = out.ownedValidity
	clear(out.validity)
	if out.typ == memory.TypeBool {
		clear(out.Truth)
	}
	if familyOf(out.typ) == familyString {
		out.data = out.data[:0]
		out.offsets[0] = 0
	}
	for row, choice := range out.choices {
		if choice >= 0 {
			value := &out.regions[choice].regs[in.Regions[choice].root]
			if isValidRegister(value, row) {
				if err := copyConditionalRow(out, value, row); err != nil {
					return err
				}
				memory.Set(out.validity, row)
			}
		}
		if familyOf(out.typ) == familyString {
			out.offsets[row+1] = uint32(len(out.data))
		}
	}
	return nil
}

func copyConditionalRow(out, value *scalarRegister, row int) error {
	if out.typ == memory.TypeBool {
		if memory.IsSet(value.Truth, row) {
			memory.Set(out.Truth, row)
		}
		return nil
	}
	switch out.typ {
	case memory.TypeInt8:
		v, e := registerInt64(value, row)
		out.i8[row] = int8(v)
		return e
	case memory.TypeInt16:
		v, e := registerInt64(value, row)
		out.i16[row] = int16(v)
		return e
	case memory.TypeUint8:
		v, e := registerUint64(value, row)
		out.u8[row] = uint8(v)
		return e
	case memory.TypeUint16:
		v, e := registerUint64(value, row)
		out.u16[row] = uint16(v)
		return e
	case memory.TypeFloat32:
		v, e := registerFloat64(value, row)
		out.f32[row] = float32(v)
		return e
	}
	switch familyOf(out.typ) {
	case familyI32:
		v, e := registerInt64(value, row)
		out.i32[row] = int32(v)
		return e
	case familyI64:
		v, e := registerInt64(value, row)
		out.i64[row] = v
		return e
	case familyU32:
		v, e := registerUint64(value, row)
		out.u32[row] = uint32(v)
		return e
	case familyU64:
		v, e := registerUint64(value, row)
		out.u64[row] = v
		return e
	case familyF64:
		v, e := registerFloat64(value, row)
		out.f64[row] = v
		return e
	case familyString:
		v, e := registerString(value, row)
		if e != nil {
			return e
		}
		return appendScalarBytes(out, v)
	default:
		return fmt.Errorf("conditional result type %s has no merge kernel", out.typ)
	}
}

// streamingConditional allows a single execution-owned scratch state only when
// every arm has fixed-width, cache-free instructions. Nested regions and variable
// payloads retain their dedicated workspaces and ordered gather contract.
func streamingConditional(in instruction) bool {
	if in.Kind != regionIf && in.Kind != regionCase && in.Kind != regionCoalesce {
		return false
	}
	if !fixedConditionalType(in.Type) {
		return false
	}
	for _, p := range in.Regions {
		for _, child := range p.instructions {
			if !fixedConditionalType(instructionResultType(child)) || (child.Code != scalarColumn && !reusableInstruction(child)) {
				return false
			}
		}
	}
	return true
}
func fixedConditionalType(typ memory.DataType) bool {
	switch typ {
	case memory.TypeBool, memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
		memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64,
		memory.TypeFloat32, memory.TypeFloat64, memory.TypeDate32, memory.TypeDate64,
		memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration:
		return true
	}
	return false
}
