package kernel

import (
	"gocolumn/internal/hashmix"
	"gocolumn/memory"
	"math"
	"math/bits"
	"unicode/utf8"
)

func prepareComparison(rows int, out memory.Bitmap) int {
	words := memory.WordsFor(rows)
	if len(out) < words {
		panic("kernel: output bitmap too small")
	}
	clear(out[:words])
	return words
}

func validAt(validity memory.Bitmap, row int) bool {
	return validity == nil || memory.IsSet(validity, row)
}

func setSelected(out memory.Bitmap, row int, selected *int) {
	memory.Set(out, row)
	(*selected)++
}

func prepareIntegerArithmetic(length, left, right, out int, overflow memory.Bitmap) {
	if left != length || right != length || out < length {
		panic("kernel: integer arithmetic length mismatch")
	}
	if overflow != nil {
		words := memory.WordsFor(length)
		if len(overflow) < words {
			panic("kernel: overflow bitmap too small")
		}
		clear(overflow[:words])
	}
}

func clearInvalidFloat64(out []float64, validity memory.Bitmap) {
	if validity == nil {
		return
	}
	for wi := 0; wi < memory.WordsFor(len(out)); wi++ {
		base := wi * 64
		wordRows := min(64, len(out)-base)
		invalid := ^bitmapWord(validity, wi) & memory.TailMask(wordRows)
		for invalid != 0 {
			bit := bits.TrailingZeros64(invalid)
			out[base+bit] = 0
			invalid &= invalid - 1
		}
	}
}

func bitmapWord(bitmap memory.Bitmap, wi int) uint64 {
	if bitmap == nil {
		return ^uint64(0)
	}
	return bitmap[wi]
}

func scalarBoolAnd(aTruth, aValidity, bTruth, bValidity, outTruth, outValidity memory.Bitmap, rows int) {
	words := prepareBoolOutput(rows, outTruth, outValidity)
	for wi := 0; wi < words; wi++ {
		at, av := aTruth[wi], bitmapWord(aValidity, wi)
		bt, bv := bTruth[wi], bitmapWord(bValidity, wi)
		t := at & bt
		f := (av &^ at) | (bv &^ bt)
		outTruth[wi], outValidity[wi] = t, t|f
	}
	maskBoolTail(outTruth, outValidity, rows)
}

func scalarBoolOr(aTruth, aValidity, bTruth, bValidity, outTruth, outValidity memory.Bitmap, rows int) {
	words := prepareBoolOutput(rows, outTruth, outValidity)
	for wi := 0; wi < words; wi++ {
		at, av := aTruth[wi], bitmapWord(aValidity, wi)
		bt, bv := bTruth[wi], bitmapWord(bValidity, wi)
		t := at | bt
		f := (av &^ at) & (bv &^ bt)
		outTruth[wi], outValidity[wi] = t, t|f
	}
	maskBoolTail(outTruth, outValidity, rows)
}

func scalarBoolNot(truth, validity, outTruth, outValidity memory.Bitmap, rows int) {
	words := prepareBoolOutput(rows, outTruth, outValidity)
	for wi := 0; wi < words; wi++ {
		v := bitmapWord(validity, wi)
		outValidity[wi] = v
		outTruth[wi] = v &^ truth[wi]
	}
	maskBoolTail(outTruth, outValidity, rows)
}

func prepareBoolOutput(rows int, truth, validity memory.Bitmap) int {
	words := memory.WordsFor(rows)
	if len(truth) < words || len(validity) < words {
		panic("kernel: boolean output bitmap too small")
	}
	return words
}

func maskBoolTail(truth, validity memory.Bitmap, rows int) {
	if words := memory.WordsFor(rows); words > 0 {
		mask := memory.TailMask(rows)
		truth[words-1] &= mask
		validity[words-1] &= mask
	}
}

func scalarHash64(x uint64) uint64 {
	return hashmix.Mix64(x)
}

func scalarHash64Batch(keys, out []uint64) {
	if len(out) < len(keys) {
		panic("kernel: hash output too small")
	}
	for i, key := range keys {
		out[i] = scalarHash64(key)
	}
}

func unrolledHash64Batch(keys, out []uint64) {
	if len(out) < len(keys) {
		panic("kernel: hash output too small")
	}
	i := 0
	for ; i+4 <= len(keys); i += 4 {
		out[i] = hashmix.Mix64(keys[i])
		out[i+1] = hashmix.Mix64(keys[i+1])
		out[i+2] = hashmix.Mix64(keys[i+2])
		out[i+3] = hashmix.Mix64(keys[i+3])
	}
	for ; i < len(keys); i++ {
		out[i] = hashmix.Mix64(keys[i])
	}
}

func scalarHash128Batch(a, b []uint64, validMasks []uint8, out []uint64) {
	if len(a) != len(b) || len(out) < len(a) || (validMasks != nil && len(validMasks) != len(a)) {
		panic("kernel: hash128 length mismatch")
	}
	for i := range a {
		valid := uint8(3)
		if validMasks != nil {
			valid = validMasks[i] & 3
		}
		out[i] = hashmix.Pair(a[i], b[i], valid)
	}
}

func scalarInt64Scale(values []int64, validity memory.Bitmap, factor int64) int {
	switch factor {
	case 0:
		for row := range values {
			if validAt(validity, row) {
				values[row] = 0
			}
		}
		return -1
	case 1:
		return -1
	case -1:
		for row, value := range values {
			if !validAt(validity, row) {
				continue
			}
			if value == math.MinInt64 {
				return row
			}
			values[row] = -value
		}
		return -1
	}
	minValue, maxValue := math.MinInt64/factor, math.MaxInt64/factor
	if factor < 0 {
		minValue, maxValue = math.MaxInt64/factor, math.MinInt64/factor
	}
	for row, value := range values {
		if !validAt(validity, row) {
			continue
		}
		if value < minValue || value > maxValue {
			return row
		}
		values[row] = value * factor
	}
	return -1
}

func runeCountBytes(value []byte) int {
	count := 0
	for len(value) > 0 {
		_, width := utf8.DecodeRune(value)
		value = value[width:]
		count++
	}
	return count
}

func scalarCompact[T any](values []T, selection memory.Bitmap, out []T) int {
	return scalarCompactFrom(values, selection, out, 0)
}

func scalarCompactFrom[T any](values []T, selection memory.Bitmap, out []T, start int) int {
	if selection == nil {
		if len(out) < len(values)-start {
			panic("kernel: compact output too small")
		}
		return copy(out, values[start:])
	}
	written := 0
	for row := start; row < len(values); {
		for row < len(values) && !memory.IsSet(selection, row) {
			row++
		}
		start := row
		for row < len(values) && memory.IsSet(selection, row) {
			row++
		}
		if start == row {
			continue
		}
		count := row - start
		if written+count > len(out) {
			panic("kernel: compact output too small")
		}
		copy(out[written:written+count], values[start:row])
		written += count
	}
	return written
}

func scalarInt32Compact(values []int32, selection memory.Bitmap, out []int32) int {
	return scalarCompact(values, selection, out)
}

func scalarInt8Compact(values []int8, selection memory.Bitmap, out []int8) int {
	return scalarCompact(values, selection, out)
}

func scalarInt16Compact(values []int16, selection memory.Bitmap, out []int16) int {
	return scalarCompact(values, selection, out)
}

func scalarInt64Compact(values []int64, selection memory.Bitmap, out []int64) int {
	return scalarCompact(values, selection, out)
}

func scalarUint32Compact(values []uint32, selection memory.Bitmap, out []uint32) int {
	return scalarCompact(values, selection, out)
}

func scalarUint8Compact(values []uint8, selection memory.Bitmap, out []uint8) int {
	return scalarCompact(values, selection, out)
}

func scalarUint16Compact(values []uint16, selection memory.Bitmap, out []uint16) int {
	return scalarCompact(values, selection, out)
}

func scalarUint64Compact(values []uint64, selection memory.Bitmap, out []uint64) int {
	return scalarCompact(values, selection, out)
}

func scalarFloat64Compact(values []float64, selection memory.Bitmap, out []float64) int {
	return scalarCompact(values, selection, out)
}

func scalarFloat32Compact(values []float32, selection memory.Bitmap, out []float32) int {
	return scalarCompact(values, selection, out)
}
