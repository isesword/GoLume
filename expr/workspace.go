package expr

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"math"
	"unsafe"
)

func instructionResultType(in instruction) memory.DataType {
	switch in.Code {
	case codeConstTrue, codeCmpScalar, codeAnd, codeOr, codeNot, codeIsNull, codeIsNotNull, codeStringMatch, codeCmpExtract:
		return memory.TypeBool
	}
	return in.Type
}
func fixedRegisterBytes(in instruction, rows int) (int64, error) {
	if in.Code == scalarColumn && in.Type != memory.TypeBool {
		return 0, nil
	}
	n := int64(memory.WordsFor(rows)) * 8
	typ := instructionResultType(in)
	width := int64(0)
	switch typ {
	case memory.TypeBool:
		n *= 2
	case memory.TypeInt8, memory.TypeUint8:
		width = 1
	case memory.TypeInt16, memory.TypeUint16:
		width = 2
	case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32:
		width = 4
	case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		width = 8
	case memory.TypeString, memory.TypeBinary:
		width = 4
		n += 4
	}
	if int64(rows) > (math.MaxInt64-n)/max(width, 1) {
		return 0, fmt.Errorf("expression workspace size overflow")
	}
	n += int64(rows) * width
	if int64(len(in.LiteralString)) > math.MaxInt64-n {
		return 0, fmt.Errorf("expression literal size overflow")
	}
	return n + int64(len(in.LiteralString)), nil
}
func instructionWorkspaceBytes(instructions []instruction, rows int) (int64, error) {
	if err := validWorkspaceRows(rows); err != nil {
		return 0, err
	}
	n, err := scalarBytes(len(instructions), 2*unsafe.Sizeof(scalarRegister{}))
	if err != nil {
		return 0, err
	}
	for _, in := range instructions {
		size, err := fixedRegisterBytes(in, rows)
		if err != nil {
			return 0, err
		}
		if size > math.MaxInt64-n {
			return 0, fmt.Errorf("expression workspace size overflow")
		}
		n += size
	}
	return n, nil
}

// prepareProgram admits a complete replacement before allocating any of it.
// Dynamic payloads retain their own reservations and are not guessed here.
func (w *evaluationState) prepareProgram(instructions []instruction, rows int) error {
	if err := validWorkspaceRows(rows); err != nil {
		return err
	}
	if len(instructions) == 0 {
		return fmt.Errorf("empty expression program")
	}
	if len(w.program) == len(instructions) && &w.program[0] == &instructions[0] && rows <= w.fixedRows {
		return nil
	}
	n, err := instructionWorkspaceBytes(instructions, rows)
	if err != nil {
		return err
	}
	var lease memory.Reservation = scalarNoopReservation{}
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if w.manager != nil {
		lease, err = w.manager.Reserve(ctx, n)
	} else if w.reserveHook != nil {
		lease, err = w.reserveHook(n)
	}
	if err != nil {
		return fmt.Errorf("reserve expression workspace: %w", err)
	}
	next := evaluationState{}
	// Admission already owns these exact fixed capacities; individual slots use
	// no-op tokens until a later dynamically charged replacement is needed.
	if err = next.ensureRegisters(len(instructions)); err == nil {
		for i, in := range instructions {
			r := &next.owned[i]
			r.typ = instructionResultType(in)
			r.acquire = next.acquire
			r.reserve = next.reserve
			r.checkContext = next.checkContext
			if in.Code != scalarColumn || in.Type == memory.TypeBool {
				err = prepareScalarBuffers(r, rows)
				if err != nil {
					break
				}
			}
			if in.LiteralString != "" {
				r.literal = make([]byte, len(in.LiteralString))
				copy(r.literal, in.LiteralString)
				r.literalKey = in.LiteralString
			}
		}
	}
	if err != nil {
		_ = next.Close()
		lease.Release()
		return err
	}
	manager, hook := w.manager, w.reserveHook
	_ = w.Close()
	*w = next
	w.manager = manager
	w.reserveHook = hook
	w.ctx = ctx
	w.staticReservation = lease
	w.program = instructions
	w.fixedRows = rows
	for i := range w.owned {
		r := &w.owned[i]
		r.dictionary.reserve = w.acquire
		r.acquire = w.acquire
		r.reserve = w.reserve
		r.checkContext = w.checkContext
	}
	return nil
}

// SetReserveHook supplies an admission owner for a zero-value workspace. Unlike
// the former predicate-only hook it covers every owned allocation.
func (w *evaluationState) SetReserveHook(fn func(int64) (memory.Reservation, error)) {
	w.reserveHook = fn
}

// Release drops private executor state.
func (w *evaluationState) Release() { _ = w.Close() }

// Register returns an immutable Boolean register view.
func (w *evaluationState) Register(i int) BoolVector {
	r := &w.regs[i]
	return BoolVector{Truth: r.Truth, Validity: r.validity}
}

// MemoryBytes counts retained private register and nested payload capacities.
func (w *evaluationState) MemoryBytes() int64 {
	if w == nil {
		return 0
	}
	n := int64(cap(w.regs)+cap(w.owned))*int64(unsafe.Sizeof(scalarRegister{})) + int64(cap(w.outputs))*int64(unsafe.Sizeof(ScalarValue{}))
	for _, r := range w.owned[:cap(w.owned)] {
		if r.exportReservation != nil {
			n += r.exportReservation.Bytes()
		}
		if r.strList != nil {
			n += int64(cap(r.strList.Offsets)) * 4
			if child := r.strList.Values; child != nil {
				n += int64(cap(child.Offsets))*4 + int64(cap(child.Data))
			}
		}
		if r.strct != nil {
			n += int64(cap(r.strct.Validity))*8 + fieldCapacityBytes(r.strct.Fields) + ownedBatchCapacityBytes(r.strct.Values)
		}
		n += int64(cap(r.i8) + cap(r.u8) + cap(r.data) + cap(r.literal) + cap(r.dictionary.matches))
		n += int64(cap(r.choices)) * int64(unsafe.Sizeof(int(0)))
		n += int64(cap(r.regions)) * int64(unsafe.Sizeof((*evaluationState)(nil))+unsafe.Sizeof(evaluationState{}))
		for _, region := range r.regions {
			n += region.MemoryBytes()
		}
		n += int64(cap(r.i16)+cap(r.u16)) * 2
		n += int64(cap(r.i32)+cap(r.u32)+cap(r.f32)+cap(r.offsets)) * 4
		n += int64(cap(r.i64)+cap(r.u64)+cap(r.f64)+cap(r.ownedValidity)+cap(r.Truth)+cap(r.active)+cap(r.listValidity)) * 8
		// Native column storage is borrowed, not counted as workspace payload.
	}
	return n
}

func fieldCapacityBytes(fields []memory.Field) int64 {
	n := int64(cap(fields)) * int64(unsafe.Sizeof(memory.Field{}))
	for _, field := range fields {
		n += fieldCapacityBytes(field.Children)
	}
	return n
}
