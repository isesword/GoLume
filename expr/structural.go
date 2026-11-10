package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"gocolumn/memory"
)

// structuralInterner names exact node descriptors, not hashes of subtrees.
// Child IDs make each descriptor proportional to the node's own payload and
// arity. Go's map still checks complete key equality when hashes collide.
// The interner is compile-local and may span guarded regions: only structural
// identity is shared; each region's register memo and evaluation domain is local.
type structuralInterner struct {
	keys  map[string]int
	bound map[*BoundExpr]int
}

func newStructuralInterner() *structuralInterner {
	return &structuralInterner{keys: make(map[string]int), bound: make(map[*BoundExpr]int)}
}

func (s *structuralInterner) intern(parts ...string) int {
	key := scalarKey(parts...)
	if id, ok := s.keys[key]; ok {
		return id
	}
	id := len(s.keys) + 1
	s.keys[key] = id
	return id
}

// scalarKey length-frames every component, including arbitrary names and text.
func scalarKey(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

func (s *structuralInterner) field(field memory.Field) int {
	parts := []string{"field", field.Name, strconv.Itoa(int(field.Type)), strconv.FormatBool(field.Nullable), strconv.Itoa(int(field.Encoding)), field.DictionaryDomain, field.ArrowType, strconv.Itoa(int(field.DecimalPrecision)), strconv.Itoa(int(field.DecimalScale))}
	for _, child := range field.Children {
		parts = append(parts, strconv.Itoa(s.field(child)))
	}
	return s.intern(parts...)
}

func (s *structuralInterner) expression(bound *BoundExpr) int {
	if id, ok := s.bound[bound]; ok {
		return id
	}
	var parts []string
	switch n := bound.Expr.(type) {
	case BoolLiteral:
		parts = []string{"bool", strconv.FormatBool(n.Value)}
	case NullLiteral:
		parts = []string{"null", strconv.Itoa(int(n.Type))}
		if n.Type == memory.TypeDecimal {
			parts = append(parts, strconv.Itoa(s.field(bound.Field)))
		}
	case IfExpr:
		parts = []string{"if"}
	case CaseExpr:
		parts = []string{"case"}
	case CoalesceExpr:
		parts = []string{"coalesce"}
	case InExpr:
		parts = []string{"in"}
	case Column:
		parts = []string{"column", n.Name}
	case Int64Literal:
		parts = []string{"int64", strconv.FormatInt(n.Value, 10)}
	case Decimal128Literal:
		parts = []string{"decimal128", strconv.FormatInt(n.Value.HighBits(), 10), strconv.FormatUint(n.Value.LowBits(), 10), strconv.Itoa(int(n.Precision)), strconv.Itoa(int(n.Scale))}
	case Decimal128NullLiteral:
		parts = []string{"decimal128-null", strconv.Itoa(int(n.Precision)), strconv.Itoa(int(n.Scale))}
	case Float64Literal:
		// Signed zero and NaN payload bits are observable literal output values.
		parts = []string{"float64", strconv.FormatUint(math.Float64bits(n.Value), 16)}
	case StringLiteral:
		parts = []string{"string", n.Value}
	case Date32Literal:
		parts = []string{"date32", strconv.FormatInt(int64(n.Days), 10)}
	case TimestampLiteral:
		parts = []string{"timestamp", strconv.FormatInt(n.Value, 10)}
	case DurationLiteral:
		parts = []string{"duration", strconv.FormatInt(int64(n.Value), 10)}
	case TimestampUnitLiteral:
		parts = []string{"timestamp-unit", strconv.FormatUint(uint64(n.Unit), 10), strconv.FormatInt(n.Value, 10)}
	case Cast:
		parts = []string{"cast", strconv.Itoa(int(n.Target))}
	case Decimal128RescaleExpr:
		parts = []string{"decimal128-rescale", strconv.Itoa(int(n.Precision)), strconv.Itoa(int(n.Scale))}
	case TryCastExpr:
		parts = []string{"try-cast", strconv.Itoa(int(n.Target))}
	case Binary:
		parts = []string{"binary", strconv.Itoa(int(n.Op))}
	case Unary:
		parts = []string{"unary", strconv.Itoa(int(n.Op))}
	case Ternary:
		parts = []string{"ternary", strconv.Itoa(int(n.Op))}
	case JSONDecodeExpr:
		parts = []string{"json"}
		for _, field := range n.Schema.Fields {
			parts = append(parts, strconv.Itoa(s.field(field)))
		}
	default:
		// Bind rejects unsupported nodes before interning. Never silently assign an
		// identity using a display string if a new bound node misses this switch.
		panic(fmt.Sprintf("unsupported bound expression %T", bound.Expr))
	}
	// Frame metadata separately from ordered child IDs, so a variable-size JSON
	// schema cannot be confused with the expression's operands.
	metadata := scalarKey(parts...)
	parts = []string{"expr", metadata}
	for _, child := range bound.Children {
		parts = append(parts, strconv.Itoa(s.expression(child)))
	}
	id := s.intern(parts...)
	s.bound[bound] = id
	return id
}
