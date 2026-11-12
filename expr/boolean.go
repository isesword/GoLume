package expr

import (
	"bytes"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
)

func validateBoundColumn(batch *memory.Batch, name string, typ memory.DataType) error {
	switch typ {
	case memory.TypeList:
		if v := batch.Int64Lists[name]; v != nil {
			_, err := predicateColumnValidity(name, batch.Rows, v.Len(), v.Validity)
			return err
		}
		v, err := batch.StringList(name)
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("column %q has nil vector", name)
		}
		_, err = predicateColumnValidity(name, batch.Rows, v.Len(), v.Validity)
		return err
	case memory.TypeStruct:
		v, err := batch.Struct(name)
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("column %q has nil vector", name)
		}
		_, err = predicateColumnValidity(name, batch.Rows, v.Len(), v.Validity)
		return err
	default:
		_, err := columnValidity(batch, name, typ)
		return err
	}
}

func evalSpecializedBoolean(out *scalarRegister, in instruction, batch *memory.Batch, w *evaluationState, kernels kernel.Set) error {
	rows := batch.Rows
	out.validity = out.ownedValidity[:memory.WordsFor(rows)]
	clear(out.Truth)
	cache := &out.dictionary
	cache.reserveErr = nil
	switch in.Code {
	case codeConstTrue:
		fillAll(out.Truth, rows)
		fillAll(out.validity, rows)
	case codeCmpScalar, codeStringMatch, codeCmpExtract:
		validity, err := columnValidity(batch, in.Column, in.Type)
		if err != nil {
			return err
		}
		fillValidity(out.validity, validity, rows)
		if in.RightColumn != "" {
			right, err := columnValidity(batch, in.RightColumn, in.Type)
			if err != nil {
				return err
			}
			andValidity(out.validity, right, rows)
		}
		switch in.Code {
		case codeCmpScalar:
			err = evalKernelComparison(batch, in, out.literal, out.Truth, kernels, cache)
		case codeStringMatch:
			err = evalStringMatch(batch, in, out.literal, out.Truth, kernels, cache)
		case codeCmpExtract:
			err = evalExtractComparison(batch, in, out.literal, out.Truth, kernels, cache)
		}
		if err != nil {
			return err
		}
	case codeIsNull, codeIsNotNull:
		validity, err := columnValidity(batch, in.Column, in.Type)
		if err != nil {
			return err
		}
		fillAll(out.validity, rows)
		for row := 0; row < rows; row++ {
			valid := validity == nil || memory.IsSet(validity, row)
			if (in.Code == codeIsNotNull) == valid {
				memory.Set(out.Truth, row)
			}
		}
	case codeAnd, codeOr, codeNot:
		in.Op = OpAnd
		if in.Code == codeOr {
			in.Op = OpOr
		}
		if in.Code == codeNot {
			in.Op = OpNot
		}
		return evalBooleanInstruction(out, in, rows, w, kernels)
	}
	if cache.reserveErr != nil {
		return cache.reserveErr
	}
	return nil
}

func evalBooleanInstruction(out *scalarRegister, in instruction, rows int, w *evaluationState, kernels kernel.Set) error {
	if in.A < 0 || in.A >= len(w.regs) {
		return fmt.Errorf("invalid boolean input register")
	}
	a := w.regs[in.A]
	out.validity = out.ownedValidity[:memory.WordsFor(rows)]
	clear(out.Truth)
	if in.Op == OpIsNull || in.Op == OpIsNotNull {
		fillAll(out.validity, rows)
		for row := 0; row < rows; row++ {
			if isValidRegister(a, row) == (in.Op == OpIsNotNull) {
				memory.Set(out.Truth, row)
			}
		}
		return nil
	}
	if in.Op == OpNot {
		kernels.BoolNot(a.Truth, a.validity, out.Truth, out.validity, rows)
		return nil
	}
	if in.B < 0 || in.B >= len(w.regs) {
		return fmt.Errorf("invalid boolean right register")
	}
	b := w.regs[in.B]
	switch in.Op {
	case OpAnd:
		kernels.BoolAnd(a.Truth, a.validity, b.Truth, b.validity, out.Truth, out.validity, rows)
		return nil
	case OpOr:
		kernels.BoolOr(a.Truth, a.validity, b.Truth, b.validity, out.Truth, out.validity, rows)
		return nil
	}
	// Predicate outputs use an explicit validity bitmap, including all-valid
	// inputs, so the same value can be consumed by kernels and projected.
	fillValidity(out.validity, a.validity, rows)
	andValidity(out.validity, b.validity, rows)
	if evalNumericRegisterComparison(out, a, b, in.Op, rows, kernels) {
		return nil
	}
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			continue
		}
		var truth bool
		var err error
		switch in.Op {
		case OpContains, OpStartsWith, OpEndsWith, OpRegexMatch:
			av, e := registerString(a, row)
			if e != nil {
				return e
			}
			bv, e := registerString(b, row)
			if e != nil {
				return e
			}
			switch in.Op {
			case OpContains:
				truth = bytes.Contains(av, bv)
			case OpStartsWith:
				truth = bytes.HasPrefix(av, bv)
			case OpEndsWith:
				truth = bytes.HasSuffix(av, bv)
			case OpRegexMatch:
				truth = in.Regex.Match(av)
			}
		default:
			truth, err = compareRegisters(a, b, row, in.Op)
		}
		if err != nil {
			return err
		}
		if truth {
			memory.Set(out.Truth, row)
		}
	}
	return nil
}

func compareRegisters(a, b *scalarRegister, row int, op Op) (bool, error) {
	if a.typ == memory.TypeDecimal || b.typ == memory.TypeDecimal {
		if a.decimalType == nil || b.decimalType == nil {
			return false, fmt.Errorf("decimal comparison requires native decimal128 metadata")
		}
		cmp := compareDecimal128(a.decimal[row], a.decimalType.DecimalScale, b.decimal[row], b.decimalType.DecimalScale)
		return compareI64(int64(cmp), 0, op), nil
	}
	af, bf := familyOf(a.typ), familyOf(b.typ)
	if af == familyString && bf == familyString {
		av, err := registerString(a, row)
		if err != nil {
			return false, err
		}
		bv, err := registerString(b, row)
		if err != nil {
			return false, err
		}
		return compareBytes(av, bv, op), nil
	}
	if a.typ == memory.TypeBool && b.typ == memory.TypeBool {
		av, bv := int64(0), int64(0)
		if memory.IsSet(a.Truth, row) {
			av = 1
		}
		if memory.IsSet(b.Truth, row) {
			bv = 1
		}
		return compareI64(av, bv, op), nil
	}
	if af == familyF64 && bf == familyF64 {
		av, err := registerFloat64(a, row)
		if err != nil {
			return false, err
		}
		bv, err := registerFloat64(b, row)
		if err != nil {
			return false, err
		}
		return compareF64(av, bv, op), nil
	}
	if af == familyF64 || bf == familyF64 {
		integer, floating := a, b
		reverse := false
		if af == familyF64 {
			integer, floating = b, a
			reverse = true
		}
		fv, err := registerFloat64(floating, row)
		if err != nil {
			return false, err
		}
		if math.IsNaN(fv) {
			return false, nil
		}
		var cmp int
		if family := familyOf(integer.typ); family == familyU32 || family == familyU64 {
			iv, err := registerUint64(integer, row)
			if err != nil {
				return false, err
			}
			cmp = compareUintFloat(iv, fv)
		} else {
			iv, err := registerInt64(integer, row)
			if err != nil {
				return false, err
			}
			cmp = compareIntFloat(iv, fv)
		}
		if reverse {
			cmp = -cmp
		}
		return compareI64(int64(cmp), 0, op), nil
	}
	au, bu := af == familyU32 || af == familyU64, bf == familyU32 || bf == familyU64
	if au && bu {
		av, err := registerUint64(a, row)
		if err != nil {
			return false, err
		}
		bv, err := registerUint64(b, row)
		if err != nil {
			return false, err
		}
		return compareU64(av, bv, op), nil
	}
	if !au && !bu {
		av, err := registerInt64(a, row)
		if err != nil {
			return false, err
		}
		bv, err := registerInt64(b, row)
		if err != nil {
			return false, err
		}
		return compareI64(av, bv, op), nil
	}
	var signed int64
	var unsigned uint64
	var err error
	if au {
		signed, err = registerInt64(b, row)
		if err == nil {
			unsigned, err = registerUint64(a, row)
		}
	} else {
		signed, err = registerInt64(a, row)
		if err == nil {
			unsigned, err = registerUint64(b, row)
		}
	}
	if err != nil {
		return false, err
	}
	cmp := 0
	if signed < 0 || uint64(signed) < unsigned {
		cmp = -1
	} else if uint64(signed) > unsigned {
		cmp = 1
	}
	if au {
		cmp = -cmp
	}
	return compareI64(int64(cmp), 0, op), nil
}

func (w *evaluationState) outputValue(index, rows int) (ScalarValue, error) {
	r := w.regs[index]
	if r.constant {
		return w.materializeLiteral(index, rows)
	}
	if w.program[index].Code == scalarColumn && (r.strView != nil || r.strDict != nil || r.strList != nil || r.intList != nil || r.strct != nil || r.opaque != nil) {
		return w.exportBorrowedValue(index, rows)
	}
	if r.typ == memory.TypeBool && w.program[index].Code != scalarColumn {
		owned := w.ownedRegister(index)
		if err := scalarResize(&owned.u32, rows, &owned.buffers[8], w.acquire); err != nil {
			return ScalarValue{}, err
		}
		for row := 0; row < rows; row++ {
			owned.u32[row] = 0
			if isValidRegister(r, row) && memory.IsSet(r.Truth, row) {
				owned.u32[row] = 1
			}
		}
		r.u32 = owned.u32
	}
	return scalarValue(r, rows), nil
}

func checkedFloatInt64(v float64, row int) (int64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < -0x1p63 || v >= 0x1p63 {
		return 0, fmt.Errorf("floating value overflows int64 at row %d", row)
	}
	return int64(v), nil
}
func checkedFloatUint64(v float64, row int) (uint64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v >= 0x1p64 {
		return 0, fmt.Errorf("floating value overflows uint64 at row %d", row)
	}
	return uint64(v), nil
}

// compareIntFloat and compareUintFloat compare mathematical values without
// rounding an integer through float64. NaN is handled by the caller.
func compareIntFloat(integer int64, floating float64) int {
	if floating >= 0x1p63 {
		return -1
	}
	if floating < -0x1p63 {
		return 1
	}
	truncated := int64(floating)
	if integer < truncated {
		return -1
	}
	if integer > truncated {
		return 1
	}
	exact := float64(truncated)
	if floating > exact {
		return -1
	}
	if floating < exact {
		return 1
	}
	return 0
}
func compareUintFloat(integer uint64, floating float64) int {
	if floating >= 0x1p64 {
		return -1
	}
	if floating < 0 {
		return 1
	}
	truncated := uint64(floating)
	if integer < truncated {
		return -1
	}
	if integer > truncated {
		return 1
	}
	exact := float64(truncated)
	if floating > exact {
		return -1
	}
	if floating < exact {
		return 1
	}
	return 0
}
