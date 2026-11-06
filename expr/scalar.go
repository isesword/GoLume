package expr

import (
	"bytes"
	"context"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ScalarOpcode identifies one physical scalar-expression instruction.
type ScalarOpcode uint8

const (
	scalarColumn ScalarOpcode = iota + 1
	scalarLiteralI64
	scalarLiteralF64
	scalarLiteralString
	scalarBinary
	scalarCast
	scalarUnary
	scalarTernary
	scalarJSONDecode
)

// ScalarInstruction is one operation in a compiled scalar-expression program.
// Type is the logical result type; SourceType is populated for explicit casts.
type ScalarInstruction struct {
	Code         ScalarOpcode
	Op           Op
	Type         memory.DataType
	SourceType   memory.DataType
	Column       string
	I64          int64
	F64          float64
	String       string
	Regex        *regexp.Regexp
	RegexGroup   int
	StructFields []memory.Field
	A, B, C      int
}

// ScalarProgram is a compact reusable typed scalar-expression program. It is
// kept as the single-expression API while ProjectionProgram shares one fused
// instruction DAG across multiple projection outputs.
type ScalarProgram struct {
	Instructions []ScalarInstruction
	Root         int
	Type         memory.DataType
	Nullable     bool
}

// ProjectionOutput maps one compiled root register to a named output column.
type ProjectionOutput struct {
	Name     string
	Root     int
	Type     memory.DataType
	Nullable bool
}

// ProjectionProgram is a fused scalar program for multiple named projection
// expressions. Common subexpressions compile to one shared register.
type ProjectionProgram struct {
	Instructions []ScalarInstruction
	Outputs      []ProjectionOutput
}

type scalarRegister struct {
	typ      memory.DataType
	validity memory.Bitmap

	i8  []int8
	i16 []int16
	i32 []int32
	i64 []int64
	u8  []uint8
	u16 []uint16
	u32 []uint32
	u64 []uint64
	f32 []float32
	f64 []float64

	strView *memory.StringViewVector
	strDict *memory.DictionaryStringVector
	strList *memory.StringListVector
	strct   *memory.StructVector
	offsets []uint32
	data    []byte

	constant bool
	i64lit   int64
	f64lit   float64
	strlit   string
	charged  int64
	reserve  func(int64) error
}

// ScalarWorkspace retains typed scalar registers across source reads. Column
// registers borrow input slices; computed registers retain their own buffers.
type ScalarWorkspace struct {
	regs         []scalarRegister
	ctx          context.Context
	manager      memory.Manager
	reservations []memory.Reservation
}

// SetMemoryManager configures memory accounting for subsequently grown
// computed registers. Existing reservations remain charged until Close.
func (w *ScalarWorkspace) SetMemoryManager(ctx context.Context, manager memory.Manager) {
	w.ctx = ctx
	w.manager = manager
}

// Close drops all retained and borrowed buffers and releases their reservations.
// The configured memory manager is kept so the workspace can be reused safely.
func (w *ScalarWorkspace) Close() error {
	reservations := w.reservations
	w.regs = nil
	w.reservations = nil
	for i := len(reservations) - 1; i >= 0; i-- {
		reservations[i].Release()
	}
	return nil
}

func (w *ScalarWorkspace) reserve(bytes int64) error {
	if bytes <= 0 || w.manager == nil {
		return nil
	}
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	r, err := w.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	w.reservations = append(w.reservations, r)
	return nil
}

// Ensure sizes scalar registers for one execution while retaining computed
// backing allocations from prior batches.
func (w *ScalarWorkspace) Ensure(_ int, regs int) {
	if cap(w.regs) < regs {
		w.regs = make([]scalarRegister, regs)
	} else {
		w.regs = w.regs[:regs]
	}
	for i := range w.regs {
		r := &w.regs[i]
		r.typ = memory.TypeInvalid
		r.strView = nil
		r.strDict = nil
		r.constant = false
		r.strlit = ""
		r.reserve = w.reserve
	}
}

type scalarCompiler struct {
	schema       memory.Schema
	instructions []ScalarInstruction
	memo         map[string]int
}

// CompileScalar validates and lowers one typed scalar expression.
func CompileScalar(schema memory.Schema, e Expr) (ScalarProgram, error) {
	c := newScalarCompiler(schema)
	root, info, err := c.compile(e)
	if err != nil {
		return ScalarProgram{}, err
	}
	return ScalarProgram{Instructions: c.instructions, Root: root, Type: info.Type, Nullable: info.Nullable}, nil
}

// CompileProjection compiles several named scalar expressions into one DAG and
// performs compile-time common-subexpression elimination across output columns.
func CompileProjection(schema memory.Schema, named []NamedExpr) (ProjectionProgram, error) {
	if len(named) == 0 {
		return ProjectionProgram{}, fmt.Errorf("projection requires expressions")
	}
	c := newScalarCompiler(schema)
	out := ProjectionProgram{}
	seen := make(map[string]struct{}, len(named))
	for _, n := range named {
		name, err := n.Name()
		if err != nil {
			return ProjectionProgram{}, err
		}
		if _, dup := seen[name]; dup {
			return ProjectionProgram{}, fmt.Errorf("projection contains duplicate output %q", name)
		}
		seen[name] = struct{}{}
		root, info, err := c.compile(n.Expr)
		if err != nil {
			return ProjectionProgram{}, fmt.Errorf("projection %q: %w", name, err)
		}
		out.Outputs = append(out.Outputs, ProjectionOutput{Name: name, Root: root, Type: info.Type, Nullable: info.Nullable})
	}
	out.Instructions = c.instructions
	return out, nil
}

func newScalarCompiler(schema memory.Schema) *scalarCompiler {
	return &scalarCompiler{schema: schema, memo: make(map[string]int)}
}

func (c *scalarCompiler) compile(e Expr) (int, TypeInfo, error) {
	if e == nil {
		return 0, TypeInfo{}, fmt.Errorf("scalar expression is nil")
	}
	info, err := InferScalarType(c.schema, e)
	if err != nil {
		return 0, TypeInfo{}, err
	}
	key := scalarExprKey(e)
	if reg, ok := c.memo[key]; ok {
		return reg, info, nil
	}
	var in ScalarInstruction
	switch n := e.(type) {
	case Column:
		in = ScalarInstruction{Code: scalarColumn, Type: info.Type, Column: n.Name}
	case Int64Literal:
		in = ScalarInstruction{Code: scalarLiteralI64, Type: memory.TypeInt64, I64: n.Value}
	case Float64Literal:
		in = ScalarInstruction{Code: scalarLiteralF64, Type: memory.TypeFloat64, F64: n.Value}
	case StringLiteral:
		in = ScalarInstruction{Code: scalarLiteralString, Type: memory.TypeString, String: n.Value}
	case Date32Literal:
		in = ScalarInstruction{Code: scalarLiteralI64, Type: memory.TypeDate32, I64: int64(n.Days)}
	case TimestampLiteral:
		in = ScalarInstruction{Code: scalarLiteralI64, Type: memory.TypeTimestamp, I64: n.Value}
	case DurationLiteral:
		in = ScalarInstruction{Code: scalarLiteralI64, Type: memory.TypeDuration, I64: int64(n.Value)}
	case TimestampUnitLiteral:
		nanos, err := n.Nanoseconds()
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarLiteralI64, Type: memory.TypeTimestamp, I64: nanos}
	case Cast:
		a, ai, err := c.compile(n.Value)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarCast, Type: n.Target, SourceType: ai.Type, A: a}
	case Unary:
		a, ai, err := c.compile(n.Value)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarUnary, Op: n.Op, Type: info.Type, SourceType: ai.Type, A: a}
	case Ternary:
		a, _, err := c.compile(n.Value)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		if n.Op == OpRegexExtract || n.Op == OpRegexExtractAll {
			pattern := n.First.(StringLiteral)
			group := n.Second.(Int64Literal)
			in = ScalarInstruction{Code: scalarTernary, Op: n.Op, Type: info.Type, A: a, B: -1, C: -1}
			in.Regex, in.RegexGroup, err = compileRegexExtract(pattern.Value, group.Value)
			if err != nil {
				return 0, TypeInfo{}, err
			}
			break
		}
		b, _, err := c.compile(n.First)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		cReg, _, err := c.compile(n.Second)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarTernary, Op: n.Op, Type: info.Type, A: a, B: b, C: cReg}
	case Binary:
		a, _, err := c.compile(n.Left)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		b, _, err := c.compile(n.Right)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarBinary, Op: n.Op, Type: info.Type, A: a, B: b}
	case JSONDecodeExpr:
		a, _, err := c.compile(n.Value)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarJSONDecode, Type: memory.TypeStruct, A: a, StructFields: append([]memory.Field(nil), n.Schema.Fields...)}
	default:
		return 0, TypeInfo{}, fmt.Errorf("unsupported scalar expression node %T", e)
	}
	c.instructions = append(c.instructions, in)
	reg := len(c.instructions) - 1
	c.memo[key] = reg
	return reg, info, nil
}

// scalarKey frames every component so arbitrary names and nested expressions
// cannot be mistaken for delimiters or neighboring nodes.
func scalarKey(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

func scalarFieldKey(field memory.Field) string {
	parts := []string{
		field.Name, strconv.Itoa(int(field.Type)), strconv.FormatBool(field.Nullable),
		strconv.Itoa(int(field.Encoding)), field.DictionaryDomain, field.ArrowType,
	}
	for _, child := range field.Children {
		parts = append(parts, scalarFieldKey(child))
	}
	return scalarKey(parts...)
}

func scalarExprKey(e Expr) string {
	switch n := e.(type) {
	case Column:
		return scalarKey("column", n.Name)
	case Int64Literal:
		return scalarKey("int64", strconv.FormatInt(n.Value, 10))
	case Float64Literal:
		// Preserve signed zero and distinct NaN payloads in literal outputs.
		return scalarKey("float64", strconv.FormatUint(math.Float64bits(n.Value), 16))
	case StringLiteral:
		return scalarKey("string", n.Value)
	case Date32Literal:
		return scalarKey("date32", strconv.FormatInt(int64(n.Days), 10))
	case TimestampLiteral:
		return scalarKey("timestamp", strconv.FormatInt(n.Value, 10))
	case DurationLiteral:
		return scalarKey("duration", strconv.FormatInt(int64(n.Value), 10))
	case TimestampUnitLiteral:
		return scalarKey("timestamp-unit", strconv.FormatUint(uint64(n.Unit), 10), strconv.FormatInt(n.Value, 10))
	case Cast:
		return scalarKey("cast", strconv.Itoa(int(n.Target)), scalarExprKey(n.Value))
	case Binary:
		return scalarKey("binary", strconv.Itoa(int(n.Op)), scalarExprKey(n.Left), scalarExprKey(n.Right))
	case Unary:
		return scalarKey("unary", strconv.Itoa(int(n.Op)), scalarExprKey(n.Value))
	case Ternary:
		return scalarKey("ternary", strconv.Itoa(int(n.Op)), scalarExprKey(n.Value), scalarExprKey(n.First), scalarExprKey(n.Second))
	case JSONDecodeExpr:
		parts := []string{"json", scalarExprKey(n.Value)}
		for _, field := range n.Schema.Fields {
			parts = append(parts, scalarFieldKey(field))
		}
		return scalarKey(parts...)
	default:
		return scalarKey(fmt.Sprintf("%T", e), e.String())
	}
}

// Evaluate executes one scalar program over batch and returns a typed register
// owned by workspace. The register remains valid until the next evaluation with
// the same workspace.
func (p ScalarProgram) Evaluate(batch *memory.Batch, w *ScalarWorkspace, kernels kernel.Set) (ScalarValue, error) {
	if p.Root < 0 || p.Root >= len(p.Instructions) {
		return ScalarValue{}, fmt.Errorf("invalid scalar program root")
	}
	if err := evaluateInstructions(p.Instructions, batch, w, kernels); err != nil {
		return ScalarValue{}, err
	}
	return scalarValue(&w.regs[p.Root], batch.Rows), nil
}

// EvaluateInt64 executes p and returns an int64 physical vector. Semantic
// date/timestamp results must use Evaluate so their logical type is preserved.
func (p ScalarProgram) EvaluateInt64(batch *memory.Batch, w *ScalarWorkspace, kernels kernel.Set) (*memory.Int64Vector, error) {
	v, err := p.Evaluate(batch, w, kernels)
	if err != nil {
		return nil, err
	}
	if v.Type != memory.TypeInt64 {
		return nil, fmt.Errorf("scalar program result type %s is not int64", v.Type)
	}
	return v.Int64, nil
}

// Evaluate executes a fused projection program and returns values in output
// order. Returned vector views alias workspace or input memory.
func (p ProjectionProgram) Evaluate(batch *memory.Batch, w *ScalarWorkspace, kernels kernel.Set) ([]ScalarValue, error) {
	if err := evaluateInstructions(p.Instructions, batch, w, kernels); err != nil {
		return nil, err
	}
	out := make([]ScalarValue, len(p.Outputs))
	for i, o := range p.Outputs {
		if o.Root < 0 || o.Root >= len(w.regs) {
			return nil, fmt.Errorf("projection output %q has invalid root", o.Name)
		}
		out[i] = scalarValue(&w.regs[o.Root], batch.Rows)
	}
	return out, nil
}

// ScalarValue is one typed vector view produced by a scalar program. Exactly
// one specialized vector field is non-nil according to Type.
type ScalarValue struct {
	Type             memory.DataType
	Int8             *memory.Int8Vector
	Int16            *memory.Int16Vector
	Int32            *memory.Int32Vector
	Int64            *memory.Int64Vector
	Uint8            *memory.Uint8Vector
	Uint16           *memory.Uint16Vector
	Uint32           *memory.Uint32Vector
	Uint64           *memory.Uint64Vector
	Float32          *memory.Float32Vector
	Float64          *memory.Float64Vector
	String           *memory.StringViewVector
	DictionaryString *memory.DictionaryStringVector
	StringList       *memory.StringListVector
	Struct           *memory.StructVector
}

func scalarValue(r *scalarRegister, rows int) ScalarValue {
	v := ScalarValue{Type: r.typ}
	switch r.typ {
	case memory.TypeInt8:
		v.Int8 = &memory.Int8Vector{Values: r.i8[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
		return v
	case memory.TypeInt16:
		v.Int16 = &memory.Int16Vector{Values: r.i16[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
		return v
	case memory.TypeUint8:
		v.Uint8 = &memory.Uint8Vector{Values: r.u8[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
		return v
	case memory.TypeUint16:
		v.Uint16 = &memory.Uint16Vector{Values: r.u16[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
		return v
	case memory.TypeFloat32:
		v.Float32 = &memory.Float32Vector{Values: r.f32[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
		return v
	}
	switch familyOf(r.typ) {
	case familyI32:
		v.Int32 = &memory.Int32Vector{Values: r.i32[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
	case familyI64:
		v.Int64 = &memory.Int64Vector{Values: r.i64[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
	case familyU32:
		v.Uint32 = &memory.Uint32Vector{Values: r.u32[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
	case familyU64:
		v.Uint64 = &memory.Uint64Vector{Values: r.u64[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
	case familyF64:
		v.Float64 = &memory.Float64Vector{Values: r.f64[:rows], Validity: r.validity, Encoding: memory.EncodingPlain}
	case familyString:
		if r.strDict != nil {
			v.DictionaryString = r.strDict
		} else if r.strView != nil {
			v.String = r.strView
		} else {
			v.String = &memory.StringViewVector{Offsets: r.offsets[:rows+1], Data: r.data, Validity: r.validity, Encoding: memory.EncodingView}
		}
	case familyListString:
		v.StringList = r.strList
	case familyInvalid:
		if r.typ == memory.TypeStruct {
			v.Struct = r.strct
		}
	}
	return v
}

type physicalFamily uint8

const (
	familyInvalid physicalFamily = iota
	familyI32
	familyI64
	familyU32
	familyU64
	familyF64
	familyString
	familyListString
)

func familyOf(t memory.DataType) physicalFamily {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return familyI32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return familyI64
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeBool:
		return familyU32
	case memory.TypeUint64:
		return familyU64
	case memory.TypeFloat32, memory.TypeFloat64:
		return familyF64
	case memory.TypeString, memory.TypeBinary:
		return familyString
	case memory.TypeList:
		return familyListString
	default:
		return familyInvalid
	}
}

func evaluateInstructions(instructions []ScalarInstruction, batch *memory.Batch, w *ScalarWorkspace, kernels kernel.Set) error {
	w.Ensure(batch.Rows, len(instructions))
	for i, in := range instructions {
		r := &w.regs[i]
		r.typ = in.Type
		switch in.Code {
		case scalarColumn:
			if err := bindScalarColumn(r, batch, in.Column, in.Type); err != nil {
				return err
			}
		case scalarLiteralI64:
			r.validity = nil
			switch familyOf(in.Type) {
			case familyI32:
				values := ensureI32(r, batch.Rows)
				for row := range values {
					values[row] = int32(in.I64)
				}
			case familyI64:
				values := ensureI64(r, batch.Rows)
				for row := range values {
					values[row] = in.I64
				}
			default:
				return fmt.Errorf("literal type %s unsupported", in.Type)
			}
		case scalarLiteralF64:
			r.validity = nil
			values := ensureF64(r, batch.Rows)
			for row := range values {
				values[row] = in.F64
			}
		case scalarLiteralString:
			r.validity = nil
			offsets := ensureOffsets(r, batch.Rows)
			offsets[0] = 0
			r.data = r.data[:0]
			for row := 0; row < batch.Rows; row++ {
				if uint64(len(r.data))+uint64(len(in.String)) > math.MaxUint32 {
					return fmt.Errorf("string literal result exceeds 4 GiB at row %d", row)
				}
				r.data = append(r.data, in.String...)
				offsets[row+1] = uint32(len(r.data))
			}
		case scalarCast:
			if err := evalCast(r, &w.regs[in.A], in.SourceType, in.Type, batch.Rows); err != nil {
				return err
			}
		case scalarBinary:
			if err := evalBinary(r, &w.regs[in.A], &w.regs[in.B], in.Op, in.Type, batch.Rows, kernels); err != nil {
				return err
			}
		case scalarUnary:
			if err := evalUnary(r, &w.regs[in.A], in.Op, in.SourceType, batch.Rows); err != nil {
				return err
			}
		case scalarTernary:
			var first, second *scalarRegister
			if in.B >= 0 && in.C >= 0 {
				first, second = &w.regs[in.B], &w.regs[in.C]
			}
			if err := evalTernary(r, &w.regs[in.A], first, second, in, batch.Rows); err != nil {
				return err
			}
		case scalarJSONDecode:
			if err := evalJSONDecode(r, &w.regs[in.A], in.StructFields, batch.Rows); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported scalar opcode %d", in.Code)
		}
	}
	return nil
}

func bindScalarColumn(r *scalarRegister, batch *memory.Batch, name string, typ memory.DataType) error {
	r.constant = false
	switch typ {
	case memory.TypeInt8:
		v, err := batch.Int8(name)
		if err != nil {
			return err
		}
		r.i8, r.validity = v.Values, v.Validity
		return nil
	case memory.TypeInt16:
		v, err := batch.Int16(name)
		if err != nil {
			return err
		}
		r.i16, r.validity = v.Values, v.Validity
		return nil
	case memory.TypeUint8:
		v, err := batch.Uint8(name)
		if err != nil {
			return err
		}
		r.u8, r.validity = v.Values, v.Validity
		return nil
	case memory.TypeUint16:
		v, err := batch.Uint16(name)
		if err != nil {
			return err
		}
		r.u16, r.validity = v.Values, v.Validity
		return nil
	case memory.TypeFloat32:
		v, err := batch.Float32(name)
		if err != nil {
			return err
		}
		r.f32, r.validity = v.Values, v.Validity
		return nil
	}
	switch familyOf(typ) {
	case familyI32:
		v, err := batch.Int32(name)
		if err != nil {
			return err
		}
		r.i32, r.validity = v.Values, v.Validity
	case familyI64:
		v, err := batch.Int64(name)
		if err != nil {
			return err
		}
		r.i64, r.validity = v.Values, v.Validity
	case familyU32:
		v, err := batch.Uint32(name)
		if err != nil {
			return err
		}
		r.u32, r.validity = v.Values, v.Validity
	case familyU64:
		v, err := batch.Uint64(name)
		if err != nil {
			return err
		}
		r.u64, r.validity = v.Values, v.Validity
	case familyF64:
		v, err := batch.Float64(name)
		if err != nil {
			return err
		}
		r.f64, r.validity = v.Values, v.Validity
	case familyString:
		if v, ok := batch.Strings[name]; ok {
			r.strView, r.strDict, r.validity = v, nil, v.Validity
			return nil
		}
		if v, ok := batch.DictStrings[name]; ok {
			r.strView, r.strDict, r.validity = nil, v, v.Validity
			return nil
		}
		return fmt.Errorf("string column %q not present in batch", name)
	case familyListString:
		v, err := batch.StringList(name)
		if err != nil {
			return err
		}
		r.strList = v
		r.validity = v.Validity
	default:
		if typ == memory.TypeStruct {
			v, err := batch.Struct(name)
			if err != nil {
				return err
			}
			r.strct, r.validity = v, v.Validity
			return nil
		}
		return fmt.Errorf("scalar column %q type %s has no specialized vector", name, typ)
	}
	return nil
}

func ensureValidity(r *scalarRegister, rows int) memory.Bitmap {
	r.validity = memory.EnsureBitmap(r.validity, rows)
	return r.validity
}

func combineValidity(r, a, b *scalarRegister, rows int) {
	if a.validity == nil && b.validity == nil {
		r.validity = nil
		return
	}
	v := ensureValidity(r, rows)
	for wi := range v {
		av := ^uint64(0)
		bv := ^uint64(0)
		if a.validity != nil {
			av = a.validity[wi]
		}
		if b.validity != nil {
			bv = b.validity[wi]
		}
		v[wi] = av & bv
	}
	if len(v) > 0 {
		v[len(v)-1] &= memory.TailMask(rows)
	}
}

func combineTernaryValidity(r, a, b, c *scalarRegister, rows int) {
	if a.validity == nil && b.validity == nil && c.validity == nil {
		r.validity = nil
		return
	}
	v := ensureValidity(r, rows)
	for wi := range v {
		av, bv, cv := ^uint64(0), ^uint64(0), ^uint64(0)
		if a.validity != nil {
			av = a.validity[wi]
		}
		if b.validity != nil {
			bv = b.validity[wi]
		}
		if c.validity != nil {
			cv = c.validity[wi]
		}
		v[wi] = av & bv & cv
	}
	if len(v) > 0 {
		v[len(v)-1] &= memory.TailMask(rows)
	}
}

func copyRegisterValidity(r, a *scalarRegister, rows int) {
	if a.validity == nil {
		r.validity = nil
		return
	}
	v := ensureValidity(r, rows)
	copy(v, a.validity)
	if len(v) > 0 {
		v[len(v)-1] &= memory.TailMask(rows)
	}
}

func isValidRegister(r *scalarRegister, row int) bool {
	return r.validity == nil || memory.IsSet(r.validity, row)
}

func ensureI32(r *scalarRegister, rows int) []int32 {
	if cap(r.i32) < rows {
		r.i32 = make([]int32, rows)
	} else {
		r.i32 = r.i32[:rows]
	}
	return r.i32
}
func ensureI8(r *scalarRegister, rows int) []int8 {
	if cap(r.i8) < rows {
		r.i8 = make([]int8, rows)
	} else {
		r.i8 = r.i8[:rows]
	}
	return r.i8
}
func ensureI16(r *scalarRegister, rows int) []int16 {
	if cap(r.i16) < rows {
		r.i16 = make([]int16, rows)
	} else {
		r.i16 = r.i16[:rows]
	}
	return r.i16
}
func ensureI64(r *scalarRegister, rows int) []int64 {
	if cap(r.i64) < rows {
		r.i64 = make([]int64, rows)
	} else {
		r.i64 = r.i64[:rows]
	}
	return r.i64
}
func ensureU32(r *scalarRegister, rows int) []uint32 {
	if cap(r.u32) < rows {
		r.u32 = make([]uint32, rows)
	} else {
		r.u32 = r.u32[:rows]
	}
	return r.u32
}
func ensureU8(r *scalarRegister, rows int) []uint8 {
	if cap(r.u8) < rows {
		r.u8 = make([]uint8, rows)
	} else {
		r.u8 = r.u8[:rows]
	}
	return r.u8
}
func ensureU16(r *scalarRegister, rows int) []uint16 {
	if cap(r.u16) < rows {
		r.u16 = make([]uint16, rows)
	} else {
		r.u16 = r.u16[:rows]
	}
	return r.u16
}
func ensureU64(r *scalarRegister, rows int) []uint64 {
	if cap(r.u64) < rows {
		r.u64 = make([]uint64, rows)
	} else {
		r.u64 = r.u64[:rows]
	}
	return r.u64
}
func ensureF64(r *scalarRegister, rows int) []float64 {
	if cap(r.f64) < rows {
		r.f64 = make([]float64, rows)
	} else {
		r.f64 = r.f64[:rows]
	}
	return r.f64
}
func ensureF32(r *scalarRegister, rows int) []float32 {
	if cap(r.f32) < rows {
		r.f32 = make([]float32, rows)
	} else {
		r.f32 = r.f32[:rows]
	}
	return r.f32
}
func ensureOffsets(r *scalarRegister, rows int) []uint32 {
	if cap(r.offsets) < rows+1 {
		r.offsets = make([]uint32, rows+1)
	} else {
		r.offsets = r.offsets[:rows+1]
	}
	return r.offsets
}

func evalBinary(out, a, b *scalarRegister, op Op, result memory.DataType, rows int, kernels kernel.Set) error {
	combineValidity(out, a, b, rows)
	if op == OpSplit {
		return evalStringSplit(out, a, b, rows)
	}
	if op == OpFind {
		return evalStringFind(out, a, b, rows)
	}
	if isTemporalType(result) {
		return evalTemporalBinary(out, a, b, op, result, rows, kernels)
	}
	switch familyOf(result) {
	case familyI64:
		values := ensureI64(out, rows)
		if familyOf(a.typ) == familyI64 && familyOf(b.typ) == familyI64 {
			arithmetic, ok := physicalCheckedArithmeticOp(op)
			if !ok {
				return fmt.Errorf("operator %s does not produce int64", op)
			}
			if row := kernels.Int64Arithmetic(a.i64[:rows], b.i64[:rows], values, out.validity, nil, arithmetic); row >= 0 {
				return arithmeticOverflow(op, result, row)
			}
			return nil
		}
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			av, err := registerInt64(a, row)
			if err != nil {
				return err
			}
			bv, err := registerInt64(b, row)
			if err != nil {
				return err
			}
			switch op {
			case OpAdd:
				value, ok := checkedAddInt64(av, bv)
				if !ok {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = value
			case OpSub:
				value, ok := checkedSubInt64(av, bv)
				if !ok {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = value
			case OpMul:
				value, ok := checkedMulInt64(av, bv)
				if !ok {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = value
			default:
				return fmt.Errorf("operator %s does not produce int64", op)
			}
		}
	case familyU64:
		values := ensureU64(out, rows)
		if familyOf(a.typ) == familyU64 && familyOf(b.typ) == familyU64 {
			arithmetic, ok := physicalCheckedArithmeticOp(op)
			if !ok {
				return fmt.Errorf("operator %s does not produce uint64", op)
			}
			if row := kernels.Uint64Arithmetic(a.u64[:rows], b.u64[:rows], values, out.validity, nil, arithmetic); row >= 0 {
				return arithmeticOverflow(op, result, row)
			}
			return nil
		}
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			av, err := registerUint64(a, row)
			if err != nil {
				return err
			}
			bv, err := registerUint64(b, row)
			if err != nil {
				return err
			}
			switch op {
			case OpAdd:
				value, carry := bits.Add64(av, bv, 0)
				if carry != 0 {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = value
			case OpSub:
				value, borrow := bits.Sub64(av, bv, 0)
				if borrow != 0 {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = value
			case OpMul:
				hi, lo := bits.Mul64(av, bv)
				if hi != 0 {
					return arithmeticOverflow(op, result, row)
				}
				values[row] = lo
			default:
				return fmt.Errorf("operator %s does not produce uint64", op)
			}
		}
	case familyF64:
		values := ensureF64(out, rows)
		if a.typ == memory.TypeFloat64 && b.typ == memory.TypeFloat64 {
			arithmetic, ok := physicalArithmeticOp(op)
			if !ok {
				return fmt.Errorf("operator %s does not produce float64", op)
			}
			kernels.Float64Arithmetic(a.f64[:rows], b.f64[:rows], values, out.validity, arithmetic)
			return nil
		}
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			av, err := registerFloat64(a, row)
			if err != nil {
				return err
			}
			bv, err := registerFloat64(b, row)
			if err != nil {
				return err
			}
			switch op {
			case OpAdd:
				values[row] = av + bv
			case OpSub:
				values[row] = av - bv
			case OpMul:
				values[row] = av * bv
			case OpDiv:
				values[row] = av / bv
			default:
				return fmt.Errorf("operator %s does not produce float64", op)
			}
		}
	case familyString:
		if op != OpConcat {
			return fmt.Errorf("operator %s does not produce string", op)
		}
		offsets := ensureOffsets(out, rows)
		offsets[0] = 0
		out.data = out.data[:0]
		for row := 0; row < rows; row++ {
			if isValidRegister(out, row) {
				av, err := registerString(a, row)
				if err != nil {
					return err
				}
				bv, err := registerString(b, row)
				if err != nil {
					return err
				}
				if err := appendStringResult(&out.data, av, op, row); err != nil {
					return err
				}
				if err := appendStringResult(&out.data, bv, op, row); err != nil {
					return err
				}
			}
			offsets[row+1] = uint32(len(out.data))
		}
	default:
		return fmt.Errorf("scalar result type %s unsupported", result)
	}
	return nil
}

func physicalArithmeticOp(op Op) (kernel.ArithmeticOp, bool) {
	switch op {
	case OpAdd:
		return kernel.ArithmeticAdd, true
	case OpSub:
		return kernel.ArithmeticSub, true
	case OpMul:
		return kernel.ArithmeticMul, true
	case OpDiv:
		return kernel.ArithmeticDiv, true
	default:
		return 0, false
	}
}

func physicalCheckedArithmeticOp(op Op) (kernel.ArithmeticOp, bool) {
	switch op {
	case OpAdd:
		return kernel.ArithmeticAdd, true
	case OpSub:
		return kernel.ArithmeticSub, true
	case OpMul:
		return kernel.ArithmeticMul, true
	default:
		return 0, false
	}
}

func evalTemporalBinary(out, a, b *scalarRegister, op Op, result memory.DataType, rows int, kernels kernel.Set) error {
	if op != OpAdd && op != OpSub {
		return fmt.Errorf("operator %s does not produce %s", op, result)
	}
	if result == memory.TypeDate32 {
		values := ensureI32(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			value, err := temporalArithmeticValue(a, b, op, result, row)
			if err != nil {
				return err
			}
			if value < math.MinInt32 || value > math.MaxInt32 {
				return arithmeticOverflow(op, result, row)
			}
			values[row] = int32(value)
		}
		return nil
	}
	values := ensureI64(out, rows)
	if familyOf(a.typ) == familyI64 && familyOf(b.typ) == familyI64 {
		arithmetic, _ := physicalCheckedArithmeticOp(op)
		if row := kernels.Int64Arithmetic(a.i64[:rows], b.i64[:rows], values, out.validity, nil, arithmetic); row >= 0 {
			return arithmeticOverflow(op, result, row)
		}
		return nil
	}
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			values[row] = 0
			continue
		}
		value, err := temporalArithmeticValue(a, b, op, result, row)
		if err != nil {
			return err
		}
		values[row] = value
	}
	return nil
}

func temporalArithmeticValue(a, b *scalarRegister, op Op, result memory.DataType, row int) (int64, error) {
	var temporal, delta *scalarRegister
	switch op {
	case OpAdd:
		if a.typ == result {
			temporal, delta = a, b
		} else if b.typ == result {
			temporal, delta = b, a
		} else {
			return 0, fmt.Errorf("operator + requires %s input", result)
		}
	case OpSub:
		if a.typ != result {
			return 0, fmt.Errorf("operator - requires %s left input", result)
		}
		temporal, delta = a, b
	}
	raw, err := registerInt64(temporal, row)
	if err != nil {
		return 0, err
	}
	d, err := registerInt64(delta, row)
	if err != nil {
		return 0, err
	}
	var value int64
	var ok bool
	if op == OpAdd {
		value, ok = checkedAddInt64(raw, d)
	} else {
		value, ok = checkedSubInt64(raw, d)
	}
	if !ok {
		return 0, arithmeticOverflow(op, result, row)
	}
	return value, nil
}

func arithmeticOverflow(op Op, typ memory.DataType, row int) error {
	return fmt.Errorf("checked %s arithmetic for %s overflows at row %d", op, typ, row)
}

func checkedAddInt64(a, b int64) (int64, bool) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, false
	}
	return a + b, true
}

func checkedSubInt64(a, b int64) (int64, bool) {
	if (b > 0 && a < math.MinInt64+b) || (b < 0 && a > math.MaxInt64+b) {
		return 0, false
	}
	return a - b, true
}

func checkedMulInt64(a, b int64) (int64, bool) {
	if (a == math.MinInt64 && b == -1) || (b == math.MinInt64 && a == -1) {
		return 0, false
	}
	value := a * b
	if a != 0 && value/a != b {
		return 0, false
	}
	return value, true
}

func timestampUnitScale(unit TimestampUnit) (int64, error) {
	switch unit {
	case TimestampUnitSecond:
		return int64(time.Second), nil
	case TimestampUnitMillisecond:
		return int64(time.Millisecond), nil
	case TimestampUnitMicrosecond:
		return int64(time.Microsecond), nil
	default:
		return 0, fmt.Errorf("unsupported timestamp unit %d", unit)
	}
}

func normalizeTimestampUnitLiteral(v TimestampUnitLiteral) (int64, error) {
	scale, err := timestampUnitScale(v.Unit)
	if err != nil {
		return 0, err
	}
	nanos, ok := checkedMulInt64(v.Value, scale)
	if !ok {
		return 0, fmt.Errorf("%s overflows UTC nanosecond normalization", v)
	}
	return nanos, nil
}

func evalUnary(out, in *scalarRegister, op Op, source memory.DataType, rows int) error {
	copyRegisterValidity(out, in, rows)
	if out.typ == memory.TypeString {
		return evalStringUnary(out, in, op, rows)
	}
	values := ensureI64(out, rows)
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			values[row] = 0
			continue
		}
		switch op {
		case OpLength:
			text, err := registerString(in, row)
			if err != nil {
				return err
			}
			values[row] = int64(utf8.RuneCount(text))
		case OpByteLength:
			text, err := registerString(in, row)
			if err != nil {
				return err
			}
			values[row] = int64(len(text))
		case OpYear, OpMonth, OpDay, OpHour, OpMinute, OpSecond:
			raw, err := registerInt64(in, row)
			if err != nil {
				return err
			}
			values[row], err = calendarPart(source, raw, op)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("operator %s does not produce int64", op)
		}
	}
	return nil
}

func evalStringUnary(out, in *scalarRegister, op Op, rows int) error {
	offsets := ensureOffsets(out, rows)
	offsets[0] = 0
	out.data = out.data[:0]
	for row := 0; row < rows; row++ {
		if isValidRegister(out, row) {
			text, err := registerString(in, row)
			if err != nil {
				return err
			}
			value, err := transformString(text, op)
			if err != nil {
				return fmt.Errorf("%s at row %d: %w", op, row, err)
			}
			if err := appendStringResult(&out.data, value, op, row); err != nil {
				return err
			}
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return nil
}

func transformString(value []byte, op Op) ([]byte, error) {
	switch op {
	case OpTrim:
		return []byte(strings.TrimSpace(string(value))), nil
	case OpTrimStart:
		return []byte(strings.TrimLeftFunc(string(value), unicodeWhitespace)), nil
	case OpTrimEnd:
		return []byte(strings.TrimRightFunc(string(value), unicodeWhitespace)), nil
	case OpLower:
		return []byte(strings.ToLower(string(value))), nil
	case OpUpper:
		return []byte(strings.ToUpper(string(value))), nil
	default:
		return nil, fmt.Errorf("operator does not produce string")
	}
}

func unicodeWhitespace(r rune) bool { return unicode.IsSpace(r) }

func evalStringFind(out, value, needle *scalarRegister, rows int) error {
	if out.typ != memory.TypeInt64 {
		return fmt.Errorf("FIND result type %s is not int64", out.typ)
	}
	values := ensureI64(out, rows)
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			values[row] = 0
			continue
		}
		text, err := registerString(value, row)
		if err != nil {
			return err
		}
		query, err := registerString(needle, row)
		if err != nil {
			return err
		}
		byteIndex := strings.Index(string(text), string(query))
		if byteIndex < 0 {
			values[row] = -1
			continue
		}
		values[row] = int64(utf8.RuneCount(text[:byteIndex]))
	}
	return nil
}

func evalTernary(out, value, first, second *scalarRegister, in ScalarInstruction, rows int) error {
	op := in.Op
	if op == OpRegexExtractAll {
		return evalRegexExtractAll(out, value, in, rows)
	}
	if out.typ != memory.TypeString {
		return fmt.Errorf("%s result type %s is not string", op, out.typ)
	}
	if op == OpRegexExtract {
		copyRegisterValidity(out, value, rows)
	} else {
		combineTernaryValidity(out, value, first, second, rows)
	}
	if op == OpRegexExtract && out.validity == nil {
		validity := ensureValidity(out, rows)
		for i := range validity {
			validity[i] = ^uint64(0)
		}
		if len(validity) > 0 {
			validity[len(validity)-1] &= memory.TailMask(rows)
		}
	}
	offsets := ensureOffsets(out, rows)
	offsets[0] = 0
	out.data = out.data[:0]
	for row := 0; row < rows; row++ {
		if isValidRegister(out, row) {
			input, err := registerString(value, row)
			if err != nil {
				return err
			}
			var result []byte
			switch op {
			case OpReplace, OpReplaceAll:
				old, err := registerString(first, row)
				if err != nil {
					return err
				}
				newValue, err := registerString(second, row)
				if err != nil {
					return err
				}
				count := 1
				if op == OpReplaceAll {
					count = -1
				}
				result = []byte(strings.Replace(string(input), string(old), string(newValue), count))
			case OpSlice:
				offset, err := registerInt64(first, row)
				if err != nil {
					return err
				}
				length, err := registerInt64(second, row)
				if err != nil {
					return err
				}
				result, err = sliceUTF8(input, offset, length)
				if err != nil {
					return fmt.Errorf("SLICE at row %d: %w", row, err)
				}
			case OpRegexExtract:
				if in.Regex == nil {
					return fmt.Errorf("REGEX_EXTRACT has no compiled pattern")
				}
				matches := in.Regex.FindSubmatchIndex(input)
				start := in.RegexGroup * 2
				if matches == nil || start+1 >= len(matches) || matches[start] < 0 {
					out.validity[row>>6] &^= uint64(1) << uint(row&63)
					result = nil
				} else {
					result = input[matches[start]:matches[start+1]]
				}
			default:
				return fmt.Errorf("operator %s does not produce string", op)
			}
			if err := appendStringResult(&out.data, result, op, row); err != nil {
				return err
			}
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return nil
}

func resetStringListResult(out *scalarRegister, rows, children, dataBytes int, childNullable bool) (*memory.StringListVector, error) {
	bytesNeeded := int64(rows+1)*4 + int64(children+1)*4 + int64(dataBytes)
	bytesNeeded += int64(memory.WordsFor(rows)) * 8
	if childNullable {
		bytesNeeded += int64(memory.WordsFor(children)) * 8
	}
	if bytesNeeded > out.charged && out.reserve != nil {
		if err := out.reserve(bytesNeeded - out.charged); err != nil {
			return nil, err
		}
		out.charged = bytesNeeded
	}
	list := out.strList
	if list == nil {
		list = &memory.StringListVector{}
		out.strList = list
	}
	if cap(list.Offsets) < rows+1 {
		list.Offsets = make([]uint32, rows+1)
	} else {
		list.Offsets = list.Offsets[:rows+1]
		clear(list.Offsets)
	}
	list.Validity = out.validity
	list.Encoding = memory.EncodingView
	if list.Values == nil {
		list.Values = &memory.StringViewVector{}
	}
	child := list.Values
	if cap(child.Offsets) == 0 {
		child.Offsets = make([]uint32, 1, children+1)
	} else {
		if cap(child.Offsets) < children+1 {
			child.Offsets = make([]uint32, 1, children+1)
		} else {
			child.Offsets = child.Offsets[:1]
		}
		child.Offsets[0] = 0
	}
	if cap(child.Data) < dataBytes {
		child.Data = make([]byte, 0, dataBytes)
	} else {
		child.Data = child.Data[:0]
	}
	if childNullable {
		child.Validity = memory.EnsureBitmap(child.Validity, children)
	} else {
		child.Validity = child.Validity[:0]
	}
	child.Encoding = memory.EncodingView
	return list, nil
}

func appendStringListElement(list *memory.StringListVector, value []byte, valid bool, op Op, row int) error {
	child := list.Values
	childRow := len(child.Offsets) - 1
	if childRow >= math.MaxUint32 {
		return fmt.Errorf("%s list result exceeds uint32 elements at row %d", op, row)
	}
	if !valid {
		if len(child.Validity) == 0 {
			child.Validity = memory.EnsureBitmap(child.Validity, childRow+1)
			for i := 0; i < childRow; i++ {
				memory.Set(child.Validity, i)
			}
		} else if memory.WordsFor(childRow+1) > len(child.Validity) {
			grown := make(memory.Bitmap, memory.WordsFor(childRow+1))
			copy(grown, child.Validity)
			child.Validity = grown
		}
	} else {
		if len(child.Validity) != 0 {
			if memory.WordsFor(childRow+1) > len(child.Validity) {
				grown := make(memory.Bitmap, memory.WordsFor(childRow+1))
				copy(grown, child.Validity)
				child.Validity = grown
			}
			memory.Set(child.Validity, childRow)
		}
		if err := appendStringResult(&child.Data, value, op, row); err != nil {
			return err
		}
	}
	child.Offsets = append(child.Offsets, uint32(len(child.Data)))
	return nil
}

func evalStringSplit(out, value, delimiter *scalarRegister, rows int) error {
	if out.typ != memory.TypeList {
		return fmt.Errorf("SPLIT result type %s is not list", out.typ)
	}
	children, dataBytes := 0, 0
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			continue
		}
		input, err := registerString(value, row)
		if err != nil {
			return err
		}
		sep, err := registerString(delimiter, row)
		if err != nil {
			return err
		}
		dataBytes += len(input)
		if dataBytes < 0 || uint64(dataBytes) > math.MaxUint32 {
			return fmt.Errorf("SPLIT string result exceeds 4 GiB at row %d", row)
		}
		if len(sep) == 0 {
			children += utf8.RuneCount(input)
		} else {
			children += bytes.Count(input, sep) + 1
			dataBytes -= bytes.Count(input, sep) * len(sep)
		}
		if uint64(children) > math.MaxUint32 {
			return fmt.Errorf("SPLIT list result exceeds uint32 elements at row %d", row)
		}
	}
	list, err := resetStringListResult(out, rows, children, dataBytes, false)
	if err != nil {
		return err
	}
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			list.Offsets[row+1] = uint32(list.Values.Len())
			continue
		}
		input, err := registerString(value, row)
		if err != nil {
			return err
		}
		sep, err := registerString(delimiter, row)
		if err != nil {
			return err
		}
		if len(sep) == 0 {
			for len(input) > 0 {
				_, width := utf8.DecodeRune(input)
				if err := appendStringListElement(list, input[:width], true, OpSplit, row); err != nil {
					return err
				}
				input = input[width:]
			}
		} else {
			for {
				at := indexSplitDelimiter(input, sep)
				if at < 0 {
					if err := appendStringListElement(list, input, true, OpSplit, row); err != nil {
						return err
					}
					break
				}
				if err := appendStringListElement(list, input[:at], true, OpSplit, row); err != nil {
					return err
				}
				input = input[at+len(sep):]
			}
		}
		list.Offsets[row+1] = uint32(list.Values.Len())
	}
	return nil
}

func indexSplitDelimiter(value, delimiter []byte) int {
	if len(delimiter) == 1 {
		return bytes.IndexByte(value, delimiter[0])
	}
	return bytes.Index(value, delimiter)
}

func evalRegexExtractAll(out, value *scalarRegister, in ScalarInstruction, rows int) error {
	if out.typ != memory.TypeList || in.Regex == nil {
		return fmt.Errorf("REGEX_EXTRACT_ALL has invalid compiled result")
	}
	copyRegisterValidity(out, value, rows)
	children, dataBytes := 0, 0
	childNullable := false
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			continue
		}
		input, err := registerString(value, row)
		if err != nil {
			return err
		}
		capture := in.RegexGroup * 2
		for _, match := range in.Regex.FindAllSubmatchIndex(input, -1) {
			children++
			if capture+1 >= len(match) || match[capture] < 0 {
				childNullable = true
				continue
			}
			dataBytes += match[capture+1] - match[capture]
		}
		if uint64(children) > math.MaxUint32 || uint64(dataBytes) > math.MaxUint32 {
			return fmt.Errorf("REGEX_EXTRACT_ALL result exceeds uint32 bounds at row %d", row)
		}
	}
	list, err := resetStringListResult(out, rows, children, dataBytes, childNullable)
	if err != nil {
		return err
	}
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			list.Offsets[row+1] = uint32(list.Values.Len())
			continue
		}
		input, err := registerString(value, row)
		if err != nil {
			return err
		}
		matches := in.Regex.FindAllSubmatchIndex(input, -1)
		capture := in.RegexGroup * 2
		for _, match := range matches {
			valid := capture+1 < len(match) && match[capture] >= 0
			var captured []byte
			if valid {
				captured = input[match[capture]:match[capture+1]]
			}
			if err := appendStringListElement(list, captured, valid, OpRegexExtractAll, row); err != nil {
				return err
			}
		}
		list.Offsets[row+1] = uint32(list.Values.Len())
	}
	return nil
}

func sliceUTF8(value []byte, offset, length int64) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("length must be non-negative")
	}
	runes := int64(utf8.RuneCount(value))
	start := offset
	if start < 0 {
		start = runes + start
		if start < 0 {
			start = 0
		}
	}
	if start >= runes || length == 0 {
		return []byte{}, nil
	}
	end := runes
	if length < runes-start {
		end = start + length
	}
	return value[runeByteOffset(value, start):runeByteOffset(value, end)], nil
}

func runeByteOffset(value []byte, count int64) int {
	if count <= 0 {
		return 0
	}
	pos := 0
	for seen := int64(0); pos < len(value) && seen < count; seen++ {
		_, width := utf8.DecodeRune(value[pos:])
		pos += width
	}
	return pos
}

func appendStringResult(dst *[]byte, value []byte, op Op, row int) error {
	if uint64(len(*dst))+uint64(len(value)) > math.MaxUint32 {
		return fmt.Errorf("%s string result exceeds 4 GiB at row %d", op, row)
	}
	*dst = append(*dst, value...)
	return nil
}

func calendarPart(source memory.DataType, value int64, op Op) (int64, error) {
	instant, err := utcCalendarTime(source, value)
	if err != nil {
		return 0, err
	}
	switch op {
	case OpYear:
		return int64(instant.Year()), nil
	case OpMonth:
		return int64(instant.Month()), nil
	case OpDay:
		return int64(instant.Day()), nil
	case OpHour:
		return int64(instant.Hour()), nil
	case OpMinute:
		return int64(instant.Minute()), nil
	case OpSecond:
		return int64(instant.Second()), nil
	default:
		return 0, fmt.Errorf("operator %s is not a calendar extractor", op)
	}
}

func utcCalendarTime(source memory.DataType, value int64) (time.Time, error) {
	switch source {
	case memory.TypeDate32:
		return time.Unix(value*24*60*60, 0).UTC(), nil
	case memory.TypeDate64:
		return time.Unix(value/1000, (value%1000)*int64(time.Millisecond)).UTC(), nil
	case memory.TypeTimestamp:
		return time.Unix(value/int64(time.Second), value%int64(time.Second)).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("type %s has no UTC calendar representation", source)
	}
}

func evalCast(out, in *scalarRegister, from, to memory.DataType, rows int) error {
	if !CanCast(from, to) {
		return fmt.Errorf("cannot cast %s to %s", from, to)
	}
	copyRegisterValidity(out, in, rows)
	if from == memory.TypeString && (to == memory.TypeTimestamp || to == memory.TypeDuration) {
		return evalStringTemporalCast(out, in, to, rows)
	}
	// Narrow numeric targets retain their native physical width. This matters
	// for direct projections and keeps explicit casts consistent with input
	// column storage instead of routing through the legacy widened families.
	switch to {
	case memory.TypeInt8, memory.TypeInt16:
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				continue
			}
			v, err := registerInt64(in, row)
			if err != nil {
				return err
			}
			if (to == memory.TypeInt8 && (v < math.MinInt8 || v > math.MaxInt8)) || (to == memory.TypeInt16 && (v < math.MinInt16 || v > math.MaxInt16)) {
				return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
			}
			if to == memory.TypeInt8 {
				ensureI8(out, rows)[row] = int8(v)
			} else {
				ensureI16(out, rows)[row] = int16(v)
			}
		}
		return nil
	case memory.TypeUint8, memory.TypeUint16:
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				continue
			}
			v, err := registerUint64(in, row)
			if err != nil {
				return err
			}
			max := uint64(math.MaxUint16)
			if to == memory.TypeUint8 {
				max = math.MaxUint8
			}
			if v > max {
				return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
			}
			if to == memory.TypeUint8 {
				ensureU8(out, rows)[row] = uint8(v)
			} else {
				ensureU16(out, rows)[row] = uint16(v)
			}
		}
		return nil
	case memory.TypeFloat32:
		values := ensureF32(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerFloat64(in, row)
			if err != nil {
				return err
			}
			if math.IsInf(v, 0) || math.Abs(v) > math.MaxFloat32 {
				return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
			}
			values[row] = float32(v)
		}
		return nil
	}
	switch familyOf(to) {
	case familyI32:
		values := ensureI32(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerInt64(in, row)
			if err != nil {
				return err
			}
			if v < math.MinInt32 || v > math.MaxInt32 {
				return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
			}
			values[row] = int32(v)
		}
	case familyI64:
		values := ensureI64(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerInt64(in, row)
			if err != nil {
				if familyOf(from) == familyF64 {
					fv, ferr := registerFloat64(in, row)
					if ferr != nil || fv < math.MinInt64 || fv > math.MaxInt64 {
						return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
					}
					values[row] = int64(fv)
					continue
				}
				return err
			}
			values[row] = v
		}
	case familyU32:
		values := ensureU32(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerUint64(in, row)
			if err != nil || v > math.MaxUint32 {
				return fmt.Errorf("cast %s to %s overflows at row %d", from, to, row)
			}
			values[row] = uint32(v)
		}
	case familyU64:
		values := ensureU64(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerUint64(in, row)
			if err != nil {
				return err
			}
			values[row] = v
		}
	case familyF64:
		values := ensureF64(out, rows)
		for row := 0; row < rows; row++ {
			if !isValidRegister(out, row) {
				values[row] = 0
				continue
			}
			v, err := registerFloat64(in, row)
			if err != nil {
				return err
			}
			values[row] = v
		}
	case familyString:
		offsets := ensureOffsets(out, rows)
		offsets[0] = 0
		out.data = out.data[:0]
		for row := 0; row < rows; row++ {
			if isValidRegister(out, row) {
				var err error
				out.data, err = appendScalarText(out.data, in, row)
				if err != nil {
					return err
				}
			}
			offsets[row+1] = uint32(len(out.data))
		}
	default:
		return fmt.Errorf("cast target %s has no physical vector", to)
	}
	return nil
}

func evalStringTemporalCast(out, in *scalarRegister, to memory.DataType, rows int) error {
	values := ensureI64(out, rows)
	for row := 0; row < rows; row++ {
		if !isValidRegister(out, row) {
			values[row] = 0
			continue
		}
		text, err := registerString(in, row)
		if err != nil {
			return err
		}
		switch to {
		case memory.TypeTimestamp:
			values[row], err = parseTimestampText(string(text))
		case memory.TypeDuration:
			var duration time.Duration
			duration, err = time.ParseDuration(string(text))
			values[row] = int64(duration)
		default:
			return fmt.Errorf("cannot parse string as %s", to)
		}
		if err != nil {
			return fmt.Errorf("cast string to %s at row %d: %w", to, row, err)
		}
	}
	return nil
}

func parseTimestampText(text string) (int64, error) {
	instant, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		// RFC3339 itself requires an offset. The engine deliberately permits the
		// otherwise unambiguous no-offset spelling and interprets it as UTC.
		instant, err = time.ParseInLocation("2006-01-02T15:04:05.999999999", text, time.UTC)
		if err != nil {
			return 0, fmt.Errorf("expected RFC3339 timestamp with Z/offset or UTC no-offset form: %w", err)
		}
	}
	min, max := time.Unix(0, math.MinInt64).UTC(), time.Unix(0, math.MaxInt64).UTC()
	if instant.Before(min) || instant.After(max) {
		return 0, fmt.Errorf("timestamp is outside the UTC nanosecond range")
	}
	return instant.UnixNano(), nil
}

func registerInt64(r *scalarRegister, row int) (int64, error) {
	if r.constant {
		switch familyOf(r.typ) {
		case familyI32, familyI64:
			return r.i64lit, nil
		case familyU32, familyU64:
			if r.i64lit < 0 {
				return 0, fmt.Errorf("negative unsigned literal")
			}
			return r.i64lit, nil
		case familyF64:
			return int64(r.f64lit), nil
		}
	}
	switch familyOf(r.typ) {
	case familyI32:
		switch r.typ {
		case memory.TypeInt8:
			return int64(r.i8[row]), nil
		case memory.TypeInt16:
			return int64(r.i16[row]), nil
		default:
			return int64(r.i32[row]), nil
		}
	case familyI64:
		return r.i64[row], nil
	case familyU32:
		switch r.typ {
		case memory.TypeUint8:
			return int64(r.u8[row]), nil
		case memory.TypeUint16:
			return int64(r.u16[row]), nil
		default:
			return int64(r.u32[row]), nil
		}
	case familyU64:
		if r.u64[row] > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value overflows int64 at row %d", row)
		}
		return int64(r.u64[row]), nil
	case familyF64:
		if r.f64[row] < math.MinInt64 || r.f64[row] > math.MaxInt64 {
			return 0, fmt.Errorf("float64 value overflows int64 at row %d", row)
		}
		return int64(r.f64[row]), nil
	default:
		return 0, fmt.Errorf("type %s cannot be converted to int64", r.typ)
	}
}

func registerUint64(r *scalarRegister, row int) (uint64, error) {
	if r.constant {
		if r.i64lit < 0 {
			return 0, fmt.Errorf("negative value cannot convert to uint64")
		}
		if familyOf(r.typ) == familyF64 {
			if r.f64lit < 0 || r.f64lit > math.MaxUint64 {
				return 0, fmt.Errorf("float64 value overflows uint64")
			}
			return uint64(r.f64lit), nil
		}
		return uint64(r.i64lit), nil
	}
	switch familyOf(r.typ) {
	case familyI32:
		v, _ := registerInt64(r, row)
		if v < 0 {
			return 0, fmt.Errorf("negative value cannot convert to uint64 at row %d", row)
		}
		return uint64(v), nil
	case familyI64:
		if r.i64[row] < 0 {
			return 0, fmt.Errorf("negative value cannot convert to uint64 at row %d", row)
		}
		return uint64(r.i64[row]), nil
	case familyU32:
		switch r.typ {
		case memory.TypeUint8:
			return uint64(r.u8[row]), nil
		case memory.TypeUint16:
			return uint64(r.u16[row]), nil
		default:
			return uint64(r.u32[row]), nil
		}
	case familyU64:
		return r.u64[row], nil
	case familyF64:
		if r.f64[row] < 0 || r.f64[row] > math.MaxUint64 {
			return 0, fmt.Errorf("float64 value overflows uint64 at row %d", row)
		}
		return uint64(r.f64[row]), nil
	default:
		return 0, fmt.Errorf("type %s cannot be converted to uint64", r.typ)
	}
}

func registerFloat64(r *scalarRegister, row int) (float64, error) {
	if r.constant {
		if familyOf(r.typ) == familyF64 {
			return r.f64lit, nil
		}
		return float64(r.i64lit), nil
	}
	switch familyOf(r.typ) {
	case familyI32:
		v, _ := registerInt64(r, row)
		return float64(v), nil
	case familyI64:
		return float64(r.i64[row]), nil
	case familyU32:
		switch r.typ {
		case memory.TypeUint8:
			return float64(r.u8[row]), nil
		case memory.TypeUint16:
			return float64(r.u16[row]), nil
		default:
			return float64(r.u32[row]), nil
		}
	case familyU64:
		return float64(r.u64[row]), nil
	case familyF64:
		if r.typ == memory.TypeFloat32 {
			return float64(r.f32[row]), nil
		}
		return r.f64[row], nil
	default:
		return 0, fmt.Errorf("type %s cannot be converted to float64", r.typ)
	}
}

func registerString(r *scalarRegister, row int) ([]byte, error) {
	if r.constant {
		return []byte(r.strlit), nil
	}
	if r.strView != nil {
		return r.strView.ValueBytes(row), nil
	}
	if r.strDict != nil {
		return r.strDict.DictionaryValue(r.strDict.Indices[row]), nil
	}
	if familyOf(r.typ) == familyString && len(r.offsets) > row+1 {
		lo, hi := r.offsets[row], r.offsets[row+1]
		if lo > hi || int(hi) > len(r.data) {
			return nil, fmt.Errorf("computed string offsets invalid at row %d", row)
		}
		return r.data[lo:hi], nil
	}
	return nil, fmt.Errorf("type %s is not a string register", r.typ)
}

func appendScalarText(dst []byte, r *scalarRegister, row int) ([]byte, error) {
	switch familyOf(r.typ) {
	case familyI32, familyI64:
		v, err := registerInt64(r, row)
		if err != nil {
			return dst, err
		}
		return strconv.AppendInt(dst, v, 10), nil
	case familyU32, familyU64:
		v, err := registerUint64(r, row)
		if err != nil {
			return dst, err
		}
		return strconv.AppendUint(dst, v, 10), nil
	case familyF64:
		v, err := registerFloat64(r, row)
		if err != nil {
			return dst, err
		}
		return strconv.AppendFloat(dst, v, 'g', -1, 64), nil
	case familyString:
		v, err := registerString(r, row)
		if err != nil {
			return dst, err
		}
		return append(dst, v...), nil
	default:
		return dst, fmt.Errorf("type %s cannot cast to string", r.typ)
	}
}
