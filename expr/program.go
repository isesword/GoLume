package expr

import (
	"bytes"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"regexp"
	"unicode/utf8"
)

// Opcode identifies a physical predicate-program instruction.
type Opcode uint8

const (
	codeConstTrue Opcode = iota + 1
	codeCmpScalar
	codeAnd
	codeOr
	codeNot
	codeIsNull
	codeIsNotNull
	codeStringMatch
	codeCmpExtract
)

// Instruction is one compiled predicate-program operation.
type Instruction struct {
	Code          Opcode
	Op            Op
	Column        string
	Type          memory.DataType
	LiteralI64    int64
	LiteralU64    uint64
	LiteralF64    float64
	LiteralString string
	Regex         *regexp.Regexp
	Extract       Op
	A, B          int
}

// Program is a compact physical predicate program.
type Program struct {
	Instructions []Instruction
	Root         int
	Complexity   int
}

// SimpleComparison describes a specialized numeric column/literal fast path.
type SimpleComparison struct {
	Column     string
	Type       memory.DataType
	Op         Op
	LiteralI64 int64
	LiteralU64 uint64
	LiteralF64 float64
}

// BoolVector stores SQL three-valued boolean truth and validity bitmaps.
type BoolVector struct{ Truth, Validity memory.Bitmap }

type dictionaryPredicateCache struct {
	valid       bool
	domain      string
	instruction Instruction
	matches     []bool
}

// EvalWorkspace retains predicate registers and dictionary predicate results
// across executions.
type EvalWorkspace struct {
	regs       []BoolVector
	dictionary []dictionaryPredicateCache
}

// Ensure sizes retained predicate registers for rows and regs.
func (w *EvalWorkspace) Ensure(rows, regs int) {
	if cap(w.regs) < regs {
		w.regs = make([]BoolVector, regs)
	} else {
		w.regs = w.regs[:regs]
	}
	for i := range w.regs {
		w.regs[i].Truth = memory.EnsureBitmap(w.regs[i].Truth, rows)
		w.regs[i].Validity = memory.EnsureBitmap(w.regs[i].Validity, rows)
	}
	if cap(w.dictionary) < regs {
		previous := w.dictionary
		w.dictionary = make([]dictionaryPredicateCache, regs)
		copy(w.dictionary, previous)
	} else {
		w.dictionary = w.dictionary[:regs]
	}
}

// Register returns one compiled predicate register.
func (w *EvalWorkspace) Register(i int) BoolVector { return w.regs[i] }

// CompilePredicate lowers a logical predicate into a physical bitmap program.
// Phase 2.6 supports typed column/literal comparisons for numeric, string,
// date32, and timestamp values in addition to SQL three-valued boolean logic.
func CompilePredicate(schema memory.Schema, e Expr) (Program, *SimpleComparison, error) {
	if e == nil {
		return Program{Instructions: []Instruction{{Code: codeConstTrue}}, Root: 0, Complexity: 1}, nil, nil
	}
	var p Program
	var compile func(Expr) (int, error)
	compile = func(x Expr) (int, error) {
		switch n := x.(type) {
		case Binary:
			switch n.Op {
			case OpGT, OpGE, OpLT, OpLE, OpEQ:
				in, err := compileComparison(schema, n)
				if err != nil {
					return 0, err
				}
				p.Instructions = append(p.Instructions, in)
				return len(p.Instructions) - 1, nil
			case OpContains, OpStartsWith, OpEndsWith, OpRegexMatch:
				in, err := compileStringMatch(schema, n)
				if err != nil {
					return 0, err
				}
				p.Instructions = append(p.Instructions, in)
				return len(p.Instructions) - 1, nil
			case OpAnd, OpOr:
				a, err := compile(n.Left)
				if err != nil {
					return 0, err
				}
				b, err := compile(n.Right)
				if err != nil {
					return 0, err
				}
				code := codeAnd
				if n.Op == OpOr {
					code = codeOr
				}
				p.Instructions = append(p.Instructions, Instruction{Code: code, A: a, B: b})
				return len(p.Instructions) - 1, nil
			default:
				return 0, fmt.Errorf("unsupported binary predicate op %s", n.Op)
			}
		case Unary:
			switch n.Op {
			case OpNot:
				a, err := compile(n.Value)
				if err != nil {
					return 0, err
				}
				p.Instructions = append(p.Instructions, Instruction{Code: codeNot, A: a})
				return len(p.Instructions) - 1, nil
			case OpIsNull, OpIsNotNull:
				c, ok := n.Value.(Column)
				if !ok {
					return 0, fmt.Errorf("%s currently requires a column", n.Op)
				}
				f, ok := schema.Field(c.Name)
				if !ok {
					return 0, fmt.Errorf("column %q not found", c.Name)
				}
				code := codeIsNull
				if n.Op == OpIsNotNull {
					code = codeIsNotNull
				}
				p.Instructions = append(p.Instructions, Instruction{Code: code, Column: c.Name, Type: f.Type})
				return len(p.Instructions) - 1, nil
			default:
				return 0, fmt.Errorf("unsupported unary predicate op %s", n.Op)
			}
		default:
			return 0, fmt.Errorf("unsupported predicate node %T", x)
		}
	}
	root, err := compile(e)
	if err != nil {
		return Program{}, nil, err
	}
	p.Root = root
	p.Complexity = len(p.Instructions)
	var simple *SimpleComparison
	if len(p.Instructions) == 1 {
		in := p.Instructions[0]
		if in.Code == codeCmpScalar && familyOf(in.Type) != familyString {
			simple = &SimpleComparison{Column: in.Column, Type: in.Type, Op: in.Op, LiteralI64: in.LiteralI64, LiteralU64: in.LiteralU64, LiteralF64: in.LiteralF64}
		}
	}
	return p, simple, nil
}

func compileComparison(schema memory.Schema, n Binary) (Instruction, error) {
	if found, ok := n.Left.(Binary); ok && found.Op == OpFind {
		return compileFindComparison(schema, n, found)
	}
	if extracted, ok := n.Left.(Unary); ok {
		return compileExtractComparison(schema, n, extracted)
	}
	c, ok := n.Left.(Column)
	if !ok {
		return Instruction{}, fmt.Errorf("comparison left operand must be column or supported extraction")
	}
	f, ok := schema.Field(c.Name)
	if !ok {
		return Instruction{}, fmt.Errorf("column %q not found", c.Name)
	}
	in := Instruction{Code: codeCmpScalar, Op: n.Op, Column: c.Name, Type: f.Type}
	switch familyOf(f.Type) {
	case familyI32, familyI64:
		switch lit := n.Right.(type) {
		case Int64Literal:
			in.LiteralI64 = lit.Value
		case Date32Literal:
			if f.Type != memory.TypeDate32 {
				return Instruction{}, fmt.Errorf("date32 literal requires date32 column, got %s", f.Type)
			}
			in.LiteralI64 = int64(lit.Days)
		case TimestampLiteral:
			if f.Type != memory.TypeTimestamp {
				return Instruction{}, fmt.Errorf("timestamp literal requires timestamp column, got %s", f.Type)
			}
			in.LiteralI64 = lit.Value
		case TimestampUnitLiteral:
			if f.Type != memory.TypeTimestamp {
				return Instruction{}, fmt.Errorf("timestamp literal requires timestamp column, got %s", f.Type)
			}
			nanos, err := lit.Nanoseconds()
			if err != nil {
				return Instruction{}, err
			}
			in.LiteralI64 = nanos
		case DurationLiteral:
			if f.Type != memory.TypeDuration {
				return Instruction{}, fmt.Errorf("duration literal requires duration column, got %s", f.Type)
			}
			in.LiteralI64 = int64(lit.Value)
		default:
			return Instruction{}, fmt.Errorf("comparison right operand for %s must be a compatible integer/date/timestamp/duration literal", f.Type)
		}
	case familyU32, familyU64:
		lit, ok := n.Right.(Int64Literal)
		if !ok || lit.Value < 0 {
			return Instruction{}, fmt.Errorf("comparison right operand for %s must be a non-negative int64 literal", f.Type)
		}
		in.LiteralU64 = uint64(lit.Value)
	case familyF64:
		switch lit := n.Right.(type) {
		case Float64Literal:
			in.LiteralF64 = lit.Value
		case Int64Literal:
			in.LiteralF64 = float64(lit.Value)
		default:
			return Instruction{}, fmt.Errorf("comparison right operand for %s must be numeric literal", f.Type)
		}
	case familyString:
		lit, ok := n.Right.(StringLiteral)
		if !ok {
			return Instruction{}, fmt.Errorf("comparison right operand for string must be string literal")
		}
		in.LiteralString = lit.Value
	default:
		return Instruction{}, fmt.Errorf("comparison column %q type %s is not supported", c.Name, f.Type)
	}
	return in, nil
}

func compileStringMatch(schema memory.Schema, n Binary) (Instruction, error) {
	c, ok := n.Left.(Column)
	if !ok {
		return Instruction{}, fmt.Errorf("%s left operand must be a string column", n.Op)
	}
	f, ok := schema.Field(c.Name)
	if !ok {
		return Instruction{}, fmt.Errorf("column %q not found", c.Name)
	}
	if f.Type != memory.TypeString {
		return Instruction{}, fmt.Errorf("%s requires string column, got %s", n.Op, f.Type)
	}
	lit, ok := n.Right.(StringLiteral)
	if !ok {
		return Instruction{}, fmt.Errorf("%s right operand must be string literal", n.Op)
	}
	in := Instruction{Code: codeStringMatch, Op: n.Op, Column: c.Name, Type: f.Type, LiteralString: lit.Value}
	if n.Op == OpRegexMatch {
		re, err := compileRegex(lit.Value)
		if err != nil {
			return Instruction{}, err
		}
		in.Regex = re
	}
	return in, nil
}

func compileExtractComparison(schema memory.Schema, n Binary, extract Unary) (Instruction, error) {
	c, ok := extract.Value.(Column)
	if !ok {
		return Instruction{}, fmt.Errorf("%s extraction input must be column", extract.Op)
	}
	f, ok := schema.Field(c.Name)
	if !ok {
		return Instruction{}, fmt.Errorf("column %q not found", c.Name)
	}
	if _, err := InferScalarType(schema, extract); err != nil {
		return Instruction{}, err
	}
	lit, ok := n.Right.(Int64Literal)
	if !ok {
		return Instruction{}, fmt.Errorf("comparison right operand for %s extraction must be int64 literal", extract.Op)
	}
	return Instruction{Code: codeCmpExtract, Op: n.Op, Extract: extract.Op, Column: c.Name, Type: f.Type, LiteralI64: lit.Value}, nil
}

func compileFindComparison(schema memory.Schema, n Binary, found Binary) (Instruction, error) {
	c, ok := found.Left.(Column)
	if !ok {
		return Instruction{}, fmt.Errorf("FIND input must be a string column")
	}
	f, ok := schema.Field(c.Name)
	if !ok {
		return Instruction{}, fmt.Errorf("column %q not found", c.Name)
	}
	if f.Type != memory.TypeString {
		return Instruction{}, fmt.Errorf("FIND requires string column, got %s", f.Type)
	}
	needle, ok := found.Right.(StringLiteral)
	if !ok {
		return Instruction{}, fmt.Errorf("FIND predicate needle must be a string literal")
	}
	if _, err := InferScalarType(schema, found); err != nil {
		return Instruction{}, err
	}
	lit, ok := n.Right.(Int64Literal)
	if !ok {
		return Instruction{}, fmt.Errorf("comparison right operand for FIND must be int64 literal")
	}
	return Instruction{Code: codeCmpExtract, Op: n.Op, Extract: OpFind, Column: c.Name, Type: f.Type, LiteralI64: lit.Value, LiteralString: needle.Value}, nil
}

// Evaluate executes the predicate program over batch using SQL three-valued logic.
func (p Program) Evaluate(batch *memory.Batch, w *EvalWorkspace, kernels kernel.Set) (BoolVector, error) {
	if p.Root < 0 || p.Root >= len(p.Instructions) {
		return BoolVector{}, fmt.Errorf("invalid predicate program root")
	}
	w.Ensure(batch.Rows, len(p.Instructions))
	for i, in := range p.Instructions {
		out := &w.regs[i]
		switch in.Code {
		case codeConstTrue:
			fillAll(out.Truth, batch.Rows)
			fillAll(out.Validity, batch.Rows)
		case codeCmpScalar:
			validity, err := columnValidity(batch, in.Column, in.Type)
			if err != nil {
				return BoolVector{}, err
			}
			fillValidity(out.Validity, validity, batch.Rows)
			if err := evalKernelComparison(batch, in, out.Truth, kernels, &w.dictionary[i]); err != nil {
				return BoolVector{}, err
			}
		case codeStringMatch:
			validity, err := columnValidity(batch, in.Column, in.Type)
			if err != nil {
				return BoolVector{}, err
			}
			fillValidity(out.Validity, validity, batch.Rows)
			if err := evalStringMatch(batch, in, out.Truth, &w.dictionary[i]); err != nil {
				return BoolVector{}, err
			}
		case codeCmpExtract:
			validity, err := columnValidity(batch, in.Column, in.Type)
			if err != nil {
				return BoolVector{}, err
			}
			fillValidity(out.Validity, validity, batch.Rows)
			if err := evalExtractComparison(batch, in, out.Truth, kernels, &w.dictionary[i]); err != nil {
				return BoolVector{}, err
			}
		case codeIsNull, codeIsNotNull:
			validity, err := columnValidity(batch, in.Column, in.Type)
			if err != nil {
				return BoolVector{}, err
			}
			fillAll(out.Validity, batch.Rows)
			if in.Code == codeIsNull {
				for wi := range out.Truth {
					var valid uint64
					if validity == nil {
						valid = ^uint64(0)
					} else {
						valid = validity[wi]
					}
					out.Truth[wi] = ^valid
				}
				maskTail(out.Truth, batch.Rows)
			} else {
				fillValidity(out.Truth, validity, batch.Rows)
			}
		case codeAnd:
			a, b := w.regs[in.A], w.regs[in.B]
			kernels.BoolAnd(a.Truth, a.Validity, b.Truth, b.Validity, out.Truth, out.Validity, batch.Rows)
		case codeOr:
			a, b := w.regs[in.A], w.regs[in.B]
			kernels.BoolOr(a.Truth, a.Validity, b.Truth, b.Validity, out.Truth, out.Validity, batch.Rows)
		case codeNot:
			a := w.regs[in.A]
			kernels.BoolNot(a.Truth, a.Validity, out.Truth, out.Validity, batch.Rows)
		default:
			return BoolVector{}, fmt.Errorf("unknown predicate opcode %d", in.Code)
		}
	}
	return w.regs[p.Root], nil
}

func evalKernelComparison(batch *memory.Batch, in Instruction, out memory.Bitmap, kernels kernel.Set, cache *dictionaryPredicateCache) error {
	op, err := physicalCompareOp(in.Op)
	if err != nil {
		return err
	}
	switch familyOf(in.Type) {
	case familyI32:
		v, err := batch.Int32(in.Column)
		if err != nil {
			return err
		}
		kernels.CompareInt32(v.Values, v.Validity, out, int32(in.LiteralI64), op)
	case familyI64:
		v, err := batch.Int64(in.Column)
		if err != nil {
			return err
		}
		kernels.CompareInt64(v.Values, v.Validity, out, in.LiteralI64, op)
	case familyU32:
		v, err := batch.Uint32(in.Column)
		if err != nil {
			return err
		}
		kernels.CompareUint32(v.Values, v.Validity, out, uint32(in.LiteralU64), op)
	case familyU64:
		v, err := batch.Uint64(in.Column)
		if err != nil {
			return err
		}
		kernels.CompareUint64(v.Values, v.Validity, out, in.LiteralU64, op)
	case familyF64:
		v, err := batch.Float64(in.Column)
		if err != nil {
			return err
		}
		kernels.CompareFloat64(v.Values, v.Validity, out, in.LiteralF64, op)
	case familyString:
		return evalTypedComparison(batch, in, out, cache)
	default:
		return fmt.Errorf("comparison type %s unsupported", in.Type)
	}
	return nil
}

func physicalCompareOp(op Op) (kernel.CompareOp, error) {
	switch op {
	case OpGT:
		return kernel.CompareGT, nil
	case OpGE:
		return kernel.CompareGE, nil
	case OpLT:
		return kernel.CompareLT, nil
	case OpLE:
		return kernel.CompareLE, nil
	case OpEQ:
		return kernel.CompareEQ, nil
	default:
		return 0, fmt.Errorf("unsupported physical comparison op %s", op)
	}
}

func evalTypedComparison(batch *memory.Batch, in Instruction, out memory.Bitmap, cache *dictionaryPredicateCache) error {
	clear(out)
	switch familyOf(in.Type) {
	case familyI32:
		v, err := batch.Int32(in.Column)
		if err != nil {
			return err
		}
		for row, x := range v.Values {
			if v.IsValid(row) && compareI64(int64(x), in.LiteralI64, in.Op) {
				memory.Set(out, row)
			}
		}
	case familyI64:
		v, err := batch.Int64(in.Column)
		if err != nil {
			return err
		}
		for row, x := range v.Values {
			if v.IsValid(row) && compareI64(x, in.LiteralI64, in.Op) {
				memory.Set(out, row)
			}
		}
	case familyU32:
		v, err := batch.Uint32(in.Column)
		if err != nil {
			return err
		}
		for row, x := range v.Values {
			if v.IsValid(row) && compareU64(uint64(x), in.LiteralU64, in.Op) {
				memory.Set(out, row)
			}
		}
	case familyU64:
		v, err := batch.Uint64(in.Column)
		if err != nil {
			return err
		}
		for row, x := range v.Values {
			if v.IsValid(row) && compareU64(x, in.LiteralU64, in.Op) {
				memory.Set(out, row)
			}
		}
	case familyF64:
		v, err := batch.Float64(in.Column)
		if err != nil {
			return err
		}
		for row, x := range v.Values {
			if v.IsValid(row) && compareF64(x, in.LiteralF64, in.Op) {
				memory.Set(out, row)
			}
		}
	case familyString:
		lit := []byte(in.LiteralString)
		if v, ok := batch.Strings[in.Column]; ok {
			for row := 0; row < v.Len(); row++ {
				if v.IsValid(row) && compareBytes(v.ValueBytes(row), lit, in.Op) {
					memory.Set(out, row)
				}
			}
			return nil
		}
		if v, ok := batch.DictStrings[in.Column]; ok {
			selectDictionary(v, out, in, cache, func(value []byte) bool { return compareBytes(value, lit, in.Op) })
			return nil
		}
		return fmt.Errorf("string column %q not present", in.Column)
	default:
		return fmt.Errorf("comparison type %s unsupported", in.Type)
	}
	return nil
}

func evalStringMatch(batch *memory.Batch, in Instruction, out memory.Bitmap, cache *dictionaryPredicateCache) error {
	clear(out)
	needle := []byte(in.LiteralString)
	match := func(value []byte) bool {
		switch in.Op {
		case OpContains:
			return bytes.Contains(value, needle)
		case OpStartsWith:
			return bytes.HasPrefix(value, needle)
		case OpEndsWith:
			return bytes.HasSuffix(value, needle)
		case OpRegexMatch:
			return in.Regex != nil && in.Regex.Match(value)
		default:
			return false
		}
	}
	if v, ok := batch.Strings[in.Column]; ok {
		for row := 0; row < v.Len(); row++ {
			if v.IsValid(row) && match(v.ValueBytes(row)) {
				memory.Set(out, row)
			}
		}
		return nil
	}
	if v, ok := batch.DictStrings[in.Column]; ok {
		selectDictionary(v, out, in, cache, match)
		return nil
	}
	return fmt.Errorf("string column %q not present", in.Column)
}

func evalExtractComparison(batch *memory.Batch, in Instruction, out memory.Bitmap, kernels kernel.Set, cache *dictionaryPredicateCache) error {
	clear(out)
	if in.Extract == OpLength || in.Extract == OpByteLength {
		return evalLengthComparison(batch, in, out, kernels, cache)
	}
	if in.Extract == OpFind {
		return evalFindComparison(batch, in, out, cache)
	}
	if familyOf(in.Type) == familyI32 {
		v, err := batch.Int32(in.Column)
		if err != nil {
			return err
		}
		for row, raw := range v.Values {
			if !v.IsValid(row) {
				continue
			}
			part, err := calendarPart(in.Type, int64(raw), in.Extract)
			if err != nil {
				return err
			}
			if compareI64(part, in.LiteralI64, in.Op) {
				memory.Set(out, row)
			}
		}
		return nil
	}
	if familyOf(in.Type) == familyI64 {
		v, err := batch.Int64(in.Column)
		if err != nil {
			return err
		}
		for row, raw := range v.Values {
			if !v.IsValid(row) {
				continue
			}
			part, err := calendarPart(in.Type, raw, in.Extract)
			if err != nil {
				return err
			}
			if compareI64(part, in.LiteralI64, in.Op) {
				memory.Set(out, row)
			}
		}
		return nil
	}
	return fmt.Errorf("extraction type %s unsupported", in.Type)
}

func evalLengthComparison(batch *memory.Batch, in Instruction, out memory.Bitmap, kernels kernel.Set, cache *dictionaryPredicateCache) error {
	compare := func(value []byte) bool {
		length := int64(utf8.RuneCount(value))
		if in.Extract == OpByteLength {
			length = int64(len(value))
		}
		return compareI64(length, in.LiteralI64, in.Op)
	}
	if v, ok := batch.Strings[in.Column]; ok {
		op, err := physicalCompareOp(in.Op)
		if err != nil {
			return err
		}
		kernels.CompareStringLength(v.Offsets, v.Data, v.Validity, out, in.LiteralI64, op, in.Extract == OpLength)
		return nil
	}
	if v, ok := batch.DictStrings[in.Column]; ok {
		selectDictionary(v, out, in, cache, compare)
		return nil
	}
	return fmt.Errorf("string column %q not present", in.Column)
}

func evalFindComparison(batch *memory.Batch, in Instruction, out memory.Bitmap, cache *dictionaryPredicateCache) error {
	compare := func(value []byte) bool {
		byteIndex := bytes.Index(value, []byte(in.LiteralString))
		index := int64(-1)
		if byteIndex >= 0 {
			index = int64(utf8.RuneCount(value[:byteIndex]))
		}
		return compareI64(index, in.LiteralI64, in.Op)
	}
	if v, ok := batch.Strings[in.Column]; ok {
		for row := 0; row < v.Len(); row++ {
			if v.IsValid(row) && compare(v.ValueBytes(row)) {
				memory.Set(out, row)
			}
		}
		return nil
	}
	if v, ok := batch.DictStrings[in.Column]; ok {
		selectDictionary(v, out, in, cache, compare)
		return nil
	}
	return fmt.Errorf("string column %q not present", in.Column)
}

// selectDictionary evaluates the predicate once per dictionary value and then
// maps the cached result through row codes. This avoids repeating UTF-8 scans
// and substring searches for low-cardinality dictionary columns.
func selectDictionary(v *memory.DictionaryStringVector, out memory.Bitmap, in Instruction, cache *dictionaryPredicateCache, predicate func([]byte) bool) {
	dictionaryLen := v.DictionaryLen()
	if !cache.valid || cache.domain != v.DictionaryDomain || cache.instruction != in || len(cache.matches) != dictionaryLen {
		if cap(cache.matches) < dictionaryLen {
			cache.matches = make([]bool, dictionaryLen)
		} else {
			cache.matches = cache.matches[:dictionaryLen]
		}
		for code := range cache.matches {
			cache.matches[code] = predicate(v.DictionaryValue(uint32(code)))
		}
		cache.valid = true
		cache.domain = v.DictionaryDomain
		cache.instruction = in
	}
	for row, code := range v.Indices {
		if v.IsValid(row) && int(code) < len(cache.matches) && cache.matches[code] {
			memory.Set(out, row)
		}
	}
}

func compareI64(a, b int64, op Op) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	default:
		return false
	}
}

func compareU64(a, b uint64, op Op) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	default:
		return false
	}
}

func compareF64(a, b float64, op Op) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	default:
		return false
	}
}

func compareBytes(a, b []byte, op Op) bool {
	cmp := bytes.Compare(a, b)
	switch op {
	case OpGT:
		return cmp > 0
	case OpGE:
		return cmp >= 0
	case OpLT:
		return cmp < 0
	case OpLE:
		return cmp <= 0
	case OpEQ:
		return cmp == 0
	default:
		return false
	}
}

func fillValidity(dst, src memory.Bitmap, rows int) {
	if src == nil {
		fillAll(dst, rows)
		return
	}
	copy(dst, src)
	for i := len(src); i < len(dst); i++ {
		dst[i] = 0
	}
	maskTail(dst, rows)
}
func fillAll(dst memory.Bitmap, rows int) {
	for i := range dst {
		dst[i] = ^uint64(0)
	}
	maskTail(dst, rows)
}
func maskTail(b memory.Bitmap, rows int) {
	if len(b) > 0 {
		b[len(b)-1] &= memory.TailMask(rows)
	}
}

func columnValidity(batch *memory.Batch, name string, typ memory.DataType) (memory.Bitmap, error) {
	switch familyOf(typ) {
	case familyI32:
		v, e := batch.Int32(name)
		if e != nil {
			return nil, e
		}
		return v.Validity, nil
	case familyI64:
		v, e := batch.Int64(name)
		if e != nil {
			return nil, e
		}
		return v.Validity, nil
	case familyU32:
		v, e := batch.Uint32(name)
		if e != nil {
			return nil, e
		}
		return v.Validity, nil
	case familyU64:
		v, e := batch.Uint64(name)
		if e != nil {
			return nil, e
		}
		return v.Validity, nil
	case familyF64:
		v, e := batch.Float64(name)
		if e != nil {
			return nil, e
		}
		return v.Validity, nil
	case familyString:
		if v, ok := batch.Strings[name]; ok {
			return v.Validity, nil
		}
		if v, ok := batch.DictStrings[name]; ok {
			return v.Validity, nil
		}
		return nil, fmt.Errorf("string column %q not present", name)
	default:
		if v, ok := batch.Opaque[name]; ok {
			return v.Validity, nil
		}
		return nil, fmt.Errorf("validity for %s column %q unsupported", typ, name)
	}
}
