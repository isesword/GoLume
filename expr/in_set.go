package expr

import (
	"bytes"
	"math"
	"slices"
	"sort"

	"gocolumn/memory"
)

// Small lists retain ordered guarded evaluation. Larger literal-only lists use
// one immutable sorted set: no candidate vectors or per-candidate workspaces.
const literalInThreshold = 16

// literalInSet normalizes literal leaves in the needle's comparison domain.
// Integer keys never round
// through float64. Floating keys canonicalize signed zero and omit NaNs, which
// cannot compare equal. Strings compare bytes, including binary-column needles.
// Its arrays and packed string payload become immutable instruction storage,
// safe to share across executions. Each execution admits the full payload
// conservatively, despite that sharing.
type literalInSet struct {
	keys    []uint64
	strings []string
	typ     memory.DataType
	hasNull bool
}

func compileLiteralIn(bound *BoundExpr) *literalInSet {
	children := bound.Children[1:]
	if len(children) < literalInThreshold {
		return nil
	}
	// Only total literal leaves qualify. Never fold casts or other expressions:
	// even constant expressions can fail only after earlier candidates miss.
	for _, child := range children {
		switch child.Expr.(type) {
		case Int64Literal, Float64Literal, StringLiteral, BoolLiteral, NullLiteral,
			Date32Literal, TimestampLiteral, DurationLiteral, TimestampUnitLiteral:
		default:
			return nil
		}
	}
	set := &literalInSet{typ: bound.Children[0].Field.Type}
	stringDomain := familyOf(set.typ) == familyString
	if stringDomain {
		set.strings = make([]string, 0, len(children))
	} else {
		set.keys = make([]uint64, 0, len(children))
	}
	for _, child := range children {
		var integer int64
		switch n := child.Expr.(type) {
		case NullLiteral:
			set.hasNull = true
			continue
		case StringLiteral:
			set.strings = append(set.strings, n.Value)
			continue
		case Float64Literal:
			if key, ok := set.floatKey(n.Value); ok {
				set.keys = append(set.keys, key)
			}
			continue
		case Int64Literal:
			integer = n.Value
		case BoolLiteral:
			if n.Value {
				integer = 1
			}
		case Date32Literal:
			integer = int64(n.Days)
		case TimestampLiteral:
			integer = n.Value
		case DurationLiteral:
			integer = int64(n.Value)
		case TimestampUnitLiteral:
			integer, _ = n.Nanoseconds() // Bind already validated the unit/range.
		}
		if key, ok := set.integerKey(integer); ok {
			set.keys = append(set.keys, key)
		}
	}
	slices.Sort(set.keys)
	set.keys = slices.Compact(set.keys)
	slices.Sort(set.strings)
	set.strings = slices.Compact(set.strings)
	return set
}

func (s *literalInSet) integerKey(value int64) (uint64, bool) {
	switch familyOf(s.typ) {
	case familyF64:
		floating := float64(value)
		if compareIntFloat(value, floating) != 0 {
			return 0, false
		}
		return floatInKey(floating), true
	case familyU32, familyU64:
		return uint64(value), value >= 0
	default:
		return uint64(value), true
	}
}

func (s *literalInSet) floatKey(value float64) (uint64, bool) {
	if math.IsNaN(value) {
		return 0, false
	}
	switch familyOf(s.typ) {
	case familyF64:
		return floatInKey(value), true
	case familyU32, familyU64:
		if value < 0 || value >= 0x1p64 || math.Trunc(value) != value {
			return 0, false
		}
		return uint64(value), true
	default:
		if value < -0x1p63 || value >= 0x1p63 || math.Trunc(value) != value {
			return 0, false
		}
		return uint64(int64(value)), true
	}
}

func floatInKey(value float64) uint64 {
	if value == 0 {
		return 0
	}
	return math.Float64bits(value)
}

// instruction keeps set payloads in the existing opcode-specific slots. This
// replaces the unused instruction.Children slice without changing instruction
// or register layout; dictionary caches embed a complete instruction.
func (s *literalInSet) instruction(root int) instruction {
	in := instruction{Code: scalarInSet, Type: memory.TypeBool, SourceType: s.typ, A: root, B: -1, C: -1, InSet: s.keys}
	if s.hasNull {
		in.I64 = 1
	}
	if familyOf(s.typ) == familyString {
		in.InSet = make([]uint64, len(s.strings)+1)
		var size int
		for _, value := range s.strings {
			size += len(value)
		}
		data := make([]byte, size)
		end := 0
		for i, value := range s.strings {
			end += copy(data[end:], value)
			in.InSet[i+1] = uint64(end)
		}
		in.String = string(data)
	}
	return in
}

func literalInBytes(in instruction) int64 {
	return int64(cap(in.InSet))*8 + int64(len(in.String))
}

func evaluateLiteralIn(out, needle *scalarRegister, in instruction, rows int) error {
	clear(out.Truth)
	out.validity = out.ownedValidity
	clear(out.validity)
	for row := 0; row < rows; row++ {
		if row&1023 == 0 {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(needle, row) {
			continue
		}
		var found bool
		if familyOf(in.SourceType) == familyString {
			value, err := registerString(needle, row)
			if err != nil {
				return err
			}
			count := len(in.InSet) - 1
			i := sort.Search(count, func(i int) bool {
				return bytes.Compare([]byte(in.String[in.InSet[i]:in.InSet[i+1]]), value) >= 0
			})
			found = i < count && bytes.Equal([]byte(in.String[in.InSet[i]:in.InSet[i+1]]), value)
		} else {
			var key uint64
			var err error
			switch familyOf(in.SourceType) {
			case familyF64:
				var value float64
				value, err = registerFloat64(needle, row)
				if math.IsNaN(value) {
					if in.I64 == 0 {
						memory.Set(out.validity, row)
					}
					continue
				}
				key = floatInKey(value)
			case familyU32, familyU64:
				key, err = registerUint64(needle, row)
			default:
				var value int64
				value, err = registerInt64(needle, row)
				key = uint64(value)
			}
			if err != nil {
				return err
			}
			_, found = slices.BinarySearch(in.InSet, key)
		}
		if found {
			memory.Set(out.Truth, row)
		}
		if found || in.I64 == 0 {
			memory.Set(out.validity, row)
		}
	}
	return nil
}
