package expr

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"strconv"
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
)

// ScalarInstruction is one operation in a compiled scalar-expression program.
// Type is the logical result type; SourceType is populated for explicit casts.
type ScalarInstruction struct {
	Code       ScalarOpcode
	Op         Op
	Type       memory.DataType
	SourceType memory.DataType
	Column     string
	I64        int64
	F64        float64
	String     string
	A, B       int
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

	i32 []int32
	i64 []int64
	u32 []uint32
	u64 []uint64
	f64 []float64

	strView *memory.StringViewVector
	strDict *memory.DictionaryStringVector
	offsets []uint32
	data    []byte

	constant bool
	i64lit   int64
	f64lit   float64
	strlit   string
}

// ScalarWorkspace retains typed scalar registers across source reads. Column
// registers borrow input slices; computed registers retain their own buffers.
type ScalarWorkspace struct {
	regs []scalarRegister
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
	case Cast:
		a, ai, err := c.compile(n.Value)
		if err != nil {
			return 0, TypeInfo{}, err
		}
		in = ScalarInstruction{Code: scalarCast, Type: n.Target, SourceType: ai.Type, A: a}
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
	default:
		return 0, TypeInfo{}, fmt.Errorf("unsupported scalar expression node %T", e)
	}
	c.instructions = append(c.instructions, in)
	reg := len(c.instructions) - 1
	c.memo[key] = reg
	return reg, info, nil
}

func scalarExprKey(e Expr) string {
	switch n := e.(type) {
	case Column:
		return "c:" + n.Name
	case Int64Literal:
		return "i:" + strconv.FormatInt(n.Value, 10)
	case Float64Literal:
		return "f:" + strconv.FormatFloat(n.Value, 'g', -1, 64)
	case StringLiteral:
		return "s:" + strconv.Quote(n.Value)
	case Date32Literal:
		return "d32:" + strconv.FormatInt(int64(n.Days), 10)
	case TimestampLiteral:
		return "ts:" + strconv.FormatInt(n.Value, 10)
	case Cast:
		return "cast:" + n.Target.String() + ":" + scalarExprKey(n.Value)
	case Binary:
		return "b:" + n.Op.String() + ":" + scalarExprKey(n.Left) + ":" + scalarExprKey(n.Right)
	default:
		return fmt.Sprintf("%T:%s", e, e.String())
	}
}

// Evaluate executes one scalar program over batch and returns a typed register
// owned by workspace. The register remains valid until the next evaluation with
// the same workspace.
func (p ScalarProgram) Evaluate(batch *memory.Batch, w *ScalarWorkspace) (ScalarValue, error) {
	if p.Root < 0 || p.Root >= len(p.Instructions) {
		return ScalarValue{}, fmt.Errorf("invalid scalar program root")
	}
	if err := evaluateInstructions(p.Instructions, batch, w); err != nil {
		return ScalarValue{}, err
	}
	return scalarValue(&w.regs[p.Root], batch.Rows), nil
}

// EvaluateInt64 executes p and returns an int64 physical vector. Semantic
// date/timestamp results must use Evaluate so their logical type is preserved.
func (p ScalarProgram) EvaluateInt64(batch *memory.Batch, w *ScalarWorkspace) (*memory.Int64Vector, error) {
	v, err := p.Evaluate(batch, w)
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
func (p ProjectionProgram) Evaluate(batch *memory.Batch, w *ScalarWorkspace) ([]ScalarValue, error) {
	if err := evaluateInstructions(p.Instructions, batch, w); err != nil {
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
	Int32            *memory.Int32Vector
	Int64            *memory.Int64Vector
	Uint32           *memory.Uint32Vector
	Uint64           *memory.Uint64Vector
	Float64          *memory.Float64Vector
	String           *memory.StringViewVector
	DictionaryString *memory.DictionaryStringVector
}

func scalarValue(r *scalarRegister, rows int) ScalarValue {
	v := ScalarValue{Type: r.typ}
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
)

func familyOf(t memory.DataType) physicalFamily {
	switch t {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		return familyI32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return familyI64
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		return familyU32
	case memory.TypeUint64:
		return familyU64
	case memory.TypeFloat32, memory.TypeFloat64:
		return familyF64
	case memory.TypeString, memory.TypeBinary:
		return familyString
	default:
		return familyInvalid
	}
}

func evaluateInstructions(instructions []ScalarInstruction, batch *memory.Batch, w *ScalarWorkspace) error {
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
				r.data = append(r.data, in.String...)
				offsets[row+1] = uint32(len(r.data))
			}
		case scalarCast:
			if err := evalCast(r, &w.regs[in.A], in.SourceType, in.Type, batch.Rows); err != nil {
				return err
			}
		case scalarBinary:
			if err := evalBinary(r, &w.regs[in.A], &w.regs[in.B], in.Op, in.Type, batch.Rows); err != nil {
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
	default:
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
func ensureOffsets(r *scalarRegister, rows int) []uint32 {
	if cap(r.offsets) < rows+1 {
		r.offsets = make([]uint32, rows+1)
	} else {
		r.offsets = r.offsets[:rows+1]
	}
	return r.offsets
}

func evalBinary(out, a, b *scalarRegister, op Op, result memory.DataType, rows int) error {
	combineValidity(out, a, b, rows)
	switch familyOf(result) {
	case familyI64:
		values := ensureI64(out, rows)
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
				values[row] = av + bv
			case OpSub:
				values[row] = av - bv
			case OpMul:
				values[row] = av * bv
			default:
				return fmt.Errorf("operator %s does not produce int64", op)
			}
		}
	case familyU64:
		values := ensureU64(out, rows)
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
				values[row] = av + bv
			case OpSub:
				values[row] = av - bv
			case OpMul:
				values[row] = av * bv
			default:
				return fmt.Errorf("operator %s does not produce uint64", op)
			}
		}
	case familyF64:
		values := ensureF64(out, rows)
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
				out.data = append(out.data, av...)
				out.data = append(out.data, bv...)
			}
			offsets[row+1] = uint32(len(out.data))
		}
	default:
		return fmt.Errorf("scalar result type %s unsupported", result)
	}
	return nil
}

func evalCast(out, in *scalarRegister, from, to memory.DataType, rows int) error {
	if !CanCast(from, to) {
		return fmt.Errorf("cannot cast %s to %s", from, to)
	}
	copyRegisterValidity(out, in, rows)
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
		return int64(r.i32[row]), nil
	case familyI64:
		return r.i64[row], nil
	case familyU32:
		return int64(r.u32[row]), nil
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
		if r.i32[row] < 0 {
			return 0, fmt.Errorf("negative value cannot convert to uint64 at row %d", row)
		}
		return uint64(r.i32[row]), nil
	case familyI64:
		if r.i64[row] < 0 {
			return 0, fmt.Errorf("negative value cannot convert to uint64 at row %d", row)
		}
		return uint64(r.i64[row]), nil
	case familyU32:
		return uint64(r.u32[row]), nil
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
		return float64(r.i32[row]), nil
	case familyI64:
		return float64(r.i64[row]), nil
	case familyU32:
		return float64(r.u32[row]), nil
	case familyU64:
		return float64(r.u64[row]), nil
	case familyF64:
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
