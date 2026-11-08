package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
)

// planLiterals selects the internal representation per logical literal, not
// per use. A shared literal stays dense if any consumer requires a vector.
// Only total literal leaves qualify: casts, arithmetic and guarded regions
// are never folded or moved across their original evaluation domain. BOOL and
// NULL retain the existing bitmap/vector representation.
func (p *Program) planLiterals() {
	for i := range p.instructions {
		in := &p.instructions[i]
		in.compactLiteral = in.Code == scalarLiteralI64 || in.Code == scalarLiteralF64 || in.Code == scalarLiteralString
	}
	for _, in := range p.instructions {
		if acceptsLiteralScalars(in) {
			continue
		}
		for _, child := range instructionInputs(in) {
			if child >= 0 && child < len(p.instructions) {
				p.instructions[child].compactLiteral = false
			}
		}
	}
}

// Keep this whitelist closed. Dense arithmetic and math consumers retain their
// SIMD kernels until they have native scalar-operand implementations. Other
// consumers below read values through registerInt64/Float64/String, including
// guarded merges and IN comparisons. Export is a separate materialization.
func acceptsLiteralScalars(in instruction) bool {
	switch in.Code {
	case scalarCast, scalarTryCast, scalarCompare, scalarStringMatch,
		scalarTernary, scalarJSONDecode, scalarConditional, scalarInSet:
		return true
	case scalarUnary:
		switch in.Op {
		case OpLength, OpByteLength, OpYear, OpMonth, OpDay, OpHour, OpMinute, OpSecond,
			OpTrim, OpTrimStart, OpTrimEnd, OpLower, OpUpper:
			return true
		}
	case scalarBoolean:
		return in.Op == OpIsNull || in.Op == OpIsNotNull
	case scalarBinary:
		return in.Op == OpConcat || in.Op == OpFind || in.Op == OpSplit
	}
	return false
}

func prepareInstructionBuffers(r *scalarRegister, in instruction, rows int) error {
	if !in.compactLiteral {
		return prepareScalarBuffers(r, rows)
	}
	// Demand is per row even when the value is scalar. Keep validity separate
	// from the immutable payload so IF/CASE masks never modify shared code.
	if err := scalarResize(&r.ownedValidity, memory.WordsFor(rows), &r.buffers[0], r.acquire); err != nil {
		return err
	}
	if in.Code == scalarLiteralString {
		// One admitted copy supports []byte readers without per-row conversions.
		if err := scalarResize(&r.literal, len(in.String), &r.buffers[13], r.acquire); err != nil {
			return err
		}
		if r.literalKey != in.String {
			copy(r.literal, in.String)
			r.literalKey = in.String
		}
	}
	return nil
}

// materializeLiteral preserves the public dense ScalarValue contract. Owners
// never alias input vectors or immutable program payloads. Dynamic buffers are
// admitted before allocation and retained on retryable budget failures. Once
// complete, duplicate roots borrow the same materialized view for this batch.
func (w *evaluationState) materializeLiteral(index, rows int) (ScalarValue, error) {
	value := &w.regs[index]
	owned := w.ownedRegister(index)
	if err := prepareScalarBuffers(owned, rows); err != nil {
		return ScalarValue{}, err
	}
	if err := w.checkContext(); err != nil {
		return ScalarValue{}, err
	}
	owned.validity = value.validity
	switch familyOf(value.typ) {
	case familyI32:
		for row := range rows {
			owned.i32[row] = int32(value.i64lit)
		}
	case familyI64:
		for row := range rows {
			owned.i64[row] = value.i64lit
		}
	case familyF64:
		for row := range rows {
			owned.f64[row] = value.f64lit
		}
	case familyString:
		count := rows
		if value.validity != nil {
			count = memory.CountBitmap(value.validity)
		}
		if len(value.strlit) != 0 && uint64(count) > math.MaxUint32/uint64(len(value.strlit)) {
			return ScalarValue{}, fmt.Errorf("string literal result exceeds 4 GiB")
		}
		size := int(uint64(len(value.strlit)) * uint64(count))
		if err := scalarResize(&owned.data, size, &owned.buffers[12], w.acquire); err != nil {
			return ScalarValue{}, err
		}
		offset := 0
		owned.offsets[0] = 0
		for row := range rows {
			if row&4095 == 0 {
				if err := w.checkContext(); err != nil {
					return ScalarValue{}, err
				}
			}
			if isValidRegister(value, row) {
				offset += copy(owned.data[offset:], value.strlit)
			}
			owned.offsets[row+1] = uint32(offset)
		}
	default:
		return ScalarValue{}, fmt.Errorf("unsupported compact literal type %s", value.typ)
	}
	if err := w.checkContext(); err != nil {
		return ScalarValue{}, err
	}
	owned.constant = false
	*value = *owned
	return scalarValue(value, rows), nil
}
