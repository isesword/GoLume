package expr

import (
	"fmt"
	"math"
	"unicode"
	"unicode/utf8"
)

// evalStringCase materializes view, dictionary and literal inputs directly into
// the existing owned byte buffer. No per-row transformed string or temporary
// reservation is needed. Dictionary input remains a plain string result.
func evalStringCase(out, in *scalarRegister, op Op, rows int) error {
	offsets := ensureOffsets(out, rows)
	offsets[0] = 0
	out.data = out.data[:0]
	for row := 0; row < rows; row++ {
		if isValidRegister(out, row) {
			value, err := registerString(in, row)
			if err != nil {
				return err
			}
			if err := appendScalarCase(out, value, op); err != nil {
				return fmt.Errorf("%s at row %d: %w", op, row, err)
			}
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return nil
}

func appendScalarCase(out *scalarRegister, value []byte, op Op) error {
	// ASCII mappings preserve byte length. Find the prefix once so the common
	// path never decodes UTF-8 or calls a Unicode mapping function.
	ascii := 0
	for ascii < len(value) && value[ascii] < utf8.RuneSelf {
		ascii++
	}
	size := uint64(len(value))
	mapping := unicode.ToLower
	if op == OpUpper {
		mapping = unicode.ToUpper
	}
	if ascii < len(value) {
		// Admit the exact byte count before writing. RuneError is deliberately
		// encoded as U+FFFD for every invalid UTF-8 byte, matching strings.Map
		// as used by strings.ToLower/ToUpper, including width-changing cases.
		size = uint64(ascii)
		for tail := value[ascii:]; len(tail) > 0; {
			r, width := utf8.DecodeRune(tail)
			size += uint64(utf8.RuneLen(mapping(r)))
			tail = tail[width:]
		}
	}
	if size > uint64(int(^uint(0)>>1)) || uint64(len(out.data))+size > math.MaxUint32 {
		return fmt.Errorf("scalar string result exceeds 4 GiB")
	}
	if err := growScalarBytes(out, int(size)); err != nil {
		return err
	}
	start := len(out.data)
	out.data = out.data[:start+ascii]
	if op == OpLower {
		for i, c := range value[:ascii] {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			out.data[start+i] = c
		}
	} else {
		for i, c := range value[:ascii] {
			if c >= 'a' && c <= 'z' {
				c -= 'a' - 'A'
			}
			out.data[start+i] = c
		}
	}
	for tail := value[ascii:]; len(tail) > 0; {
		r, width := utf8.DecodeRune(tail)
		out.data = utf8.AppendRune(out.data, mapping(r))
		tail = tail[width:]
	}
	return nil
}
