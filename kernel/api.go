package kernel

import (
	"fmt"
	"gocolumn/memory"
)

// Mode controls how a kernel set selects scalar or SIMD implementations.
type Mode uint8

const (
	// ModeAuto enables only operation-level backends approved by performance gates.
	ModeAuto Mode = iota
	// ModeScalar forces every operation to use its scalar backend.
	ModeScalar
	// ModeSIMD forces available SIMD candidates for correctness and diagnostics.
	ModeSIMD
)

// String returns the stable mode name used by diagnostics.
func (m Mode) String() string {
	switch m {
	case ModeAuto:
		return "auto"
	case ModeScalar:
		return "scalar"
	case ModeSIMD:
		return "forced-simd"
	default:
		return fmt.Sprintf("mode(%d)", m)
	}
}

// CompareOp is a physical numeric comparison operation.
type CompareOp uint8

const (
	// CompareGT evaluates greater-than.
	CompareGT CompareOp = iota + 1
	// CompareGE evaluates greater-than-or-equal.
	CompareGE
	// CompareLT evaluates less-than.
	CompareLT
	// CompareLE evaluates less-than-or-equal.
	CompareLE
	// CompareEQ evaluates equality.
	CompareEQ
)

// String returns the stable comparison operation name.
func (op CompareOp) String() string {
	switch op {
	case CompareGT:
		return "gt"
	case CompareGE:
		return "ge"
	case CompareLT:
		return "lt"
	case CompareLE:
		return "le"
	case CompareEQ:
		return "eq"
	default:
		return fmt.Sprintf("compare(%d)", op)
	}
}

// ArithmeticOp is a physical arithmetic operation.
type ArithmeticOp uint8

const (
	// ArithmeticAdd performs addition.
	ArithmeticAdd ArithmeticOp = iota + 1
	// ArithmeticSub performs subtraction.
	ArithmeticSub
	// ArithmeticMul performs multiplication.
	ArithmeticMul
	// ArithmeticDiv performs floating-point division.
	ArithmeticDiv
)

// String returns the stable arithmetic operation name.
func (op ArithmeticOp) String() string {
	switch op {
	case ArithmeticAdd:
		return "add"
	case ArithmeticSub:
		return "sub"
	case ArithmeticMul:
		return "mul"
	case ArithmeticDiv:
		return "div"
	default:
		return fmt.Sprintf("arithmetic(%d)", op)
	}
}

// KernelKind identifies a broad dispatch family.
type KernelKind uint8

const (
	// KernelComparison identifies typed comparison operations.
	KernelComparison KernelKind = iota + 1
	// KernelArithmetic identifies typed arithmetic operations.
	KernelArithmetic
	// KernelBoolean identifies SQL three-valued boolean operations.
	KernelBoolean
	// KernelHash identifies batch hashing operations.
	KernelHash
	// KernelScale identifies checked unit-scaling operations.
	KernelScale
	// KernelStringLength identifies string length operations.
	KernelStringLength
	// KernelCompact identifies fixed-width compaction operations.
	KernelCompact
)

// String returns the stable kernel-family name.
func (k KernelKind) String() string {
	switch k {
	case KernelComparison:
		return "comparison"
	case KernelArithmetic:
		return "arithmetic"
	case KernelBoolean:
		return "boolean"
	case KernelHash:
		return "hash"
	case KernelScale:
		return "scale"
	case KernelStringLength:
		return "string-length"
	case KernelCompact:
		return "compact"
	default:
		return fmt.Sprintf("kernel-kind(%d)", k)
	}
}

// KernelID is an immutable comparable identifier for one independently
// selected operation. Private fields prevent invalid external combinations.
type KernelID struct {
	kind KernelKind
	typ  memory.DataType
	op   uint8
}

// Kind returns the broad family containing id.
func (id KernelID) Kind() KernelKind { return id.kind }

// String returns the stable operation-level identifier.
func (id KernelID) String() string {
	switch id.kind {
	case KernelComparison:
		return fmt.Sprintf("compare-%s-%s", id.typ, CompareOp(id.op))
	case KernelArithmetic:
		return fmt.Sprintf("arithmetic-%s-%s", id.typ, ArithmeticOp(id.op))
	case KernelBoolean:
		names := [...]string{"", "boolean-and", "boolean-or", "boolean-not"}
		if int(id.op) < len(names) {
			return names[id.op]
		}
	case KernelHash:
		names := [...]string{"", "hash64-batch", "hash128-batch"}
		if int(id.op) < len(names) {
			return names[id.op]
		}
	case KernelScale:
		return "int64-scale"
	case KernelStringLength:
		if id.op == 1 {
			return "string-byte-length"
		}
		if id.op == 2 {
			return "string-utf8-length"
		}
	case KernelCompact:
		return fmt.Sprintf("compact-%s", id.typ)
	}
	return "invalid-kernel"
}

// ComparisonID returns the exact comparison kernel identifier for typ and op.
func ComparisonID(typ memory.DataType, op CompareOp) (KernelID, bool) {
	if op < CompareGT || op > CompareEQ {
		return KernelID{}, false
	}
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		typ = memory.TypeInt32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		typ = memory.TypeInt64
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		typ = memory.TypeUint32
	case memory.TypeUint64:
	case memory.TypeFloat32, memory.TypeFloat64:
		typ = memory.TypeFloat64
	default:
		return KernelID{}, false
	}
	return KernelID{kind: KernelComparison, typ: typ, op: uint8(op)}, true
}

// ArithmeticID returns the exact arithmetic kernel identifier for typ and op.
func ArithmeticID(typ memory.DataType, op ArithmeticOp) (KernelID, bool) {
	if op < ArithmeticAdd || op > ArithmeticDiv {
		return KernelID{}, false
	}
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		typ = memory.TypeInt32
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		typ = memory.TypeInt64
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32:
		typ = memory.TypeUint32
	case memory.TypeUint64:
	case memory.TypeFloat32, memory.TypeFloat64:
		typ = memory.TypeFloat64
	default:
		return KernelID{}, false
	}
	if typ != memory.TypeFloat64 && op == ArithmeticDiv {
		return KernelID{}, false
	}
	return KernelID{kind: KernelArithmetic, typ: typ, op: uint8(op)}, true
}

var (
	// KernelBoolAnd identifies SQL three-valued AND.
	KernelBoolAnd = KernelID{kind: KernelBoolean, op: 1}
	// KernelBoolOr identifies SQL three-valued OR.
	KernelBoolOr = KernelID{kind: KernelBoolean, op: 2}
	// KernelBoolNot identifies SQL three-valued NOT.
	KernelBoolNot = KernelID{kind: KernelBoolean, op: 3}
	// KernelHash64Batch identifies canonical batch hashing of uint64 keys.
	KernelHash64Batch = KernelID{kind: KernelHash, op: 1}
	// KernelHash128Batch identifies canonical nullable pair batch hashing.
	KernelHash128Batch = KernelID{kind: KernelHash, op: 2}
	// KernelInt64Scale identifies checked in-place int64 scaling.
	KernelInt64Scale = KernelID{kind: KernelScale, typ: memory.TypeInt64, op: 1}
	// KernelStringByteLength identifies byte-length comparison.
	KernelStringByteLength = KernelID{kind: KernelStringLength, typ: memory.TypeString, op: 1}
	// KernelStringUTF8Length identifies UTF-8 rune-length comparison.
	KernelStringUTF8Length = KernelID{kind: KernelStringLength, typ: memory.TypeString, op: 2}
	// KernelCompactInt32 identifies int32 compaction.
	KernelCompactInt32 = KernelID{kind: KernelCompact, typ: memory.TypeInt32, op: 1}
	// KernelCompactInt64 identifies int64 compaction.
	KernelCompactInt64 = KernelID{kind: KernelCompact, typ: memory.TypeInt64, op: 1}
	// KernelCompactUint32 identifies uint32 compaction.
	KernelCompactUint32 = KernelID{kind: KernelCompact, typ: memory.TypeUint32, op: 1}
	// KernelCompactUint64 identifies uint64 compaction.
	KernelCompactUint64 = KernelID{kind: KernelCompact, typ: memory.TypeUint64, op: 1}
	// KernelCompactFloat64 identifies float64 compaction.
	KernelCompactFloat64 = KernelID{kind: KernelCompact, typ: memory.TypeFloat64, op: 1}
)

var allKernelIDs = buildKernelIDs()

func buildKernelIDs() []KernelID {
	ids := make([]KernelID, 0, 64)
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeInt64, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat64} {
		for op := CompareGT; op <= CompareEQ; op++ {
			id, _ := ComparisonID(typ, op)
			ids = append(ids, id)
		}
		for op := ArithmeticAdd; op <= ArithmeticDiv; op++ {
			if typ != memory.TypeFloat64 && op == ArithmeticDiv {
				continue
			}
			id, _ := ArithmeticID(typ, op)
			ids = append(ids, id)
		}
	}
	return append(ids,
		KernelBoolAnd, KernelBoolOr, KernelBoolNot,
		KernelHash64Batch, KernelHash128Batch, KernelInt64Scale,
		KernelStringByteLength, KernelStringUTF8Length,
		KernelCompactInt32, KernelCompactInt64, KernelCompactUint32, KernelCompactUint64, KernelCompactFloat64,
	)
}

// KernelIDs returns every supported operation in stable report order.
func KernelIDs() []KernelID { return append([]KernelID(nil), allKernelIDs...) }

// KernelCapability describes the selected backend for one exact operation.
type KernelCapability struct {
	HardwareSIMD bool
	Emulated     bool
	VectorBits   int
	Backend      string
}

type capabilityState struct {
	mode Mode
	arch string
	byID map[KernelID]KernelCapability
}

// Capabilities is an immutable view of one Set's selected backends.
type Capabilities struct{ state *capabilityState }

// Valid reports whether the capability view belongs to a constructed Set.
func (c Capabilities) Valid() bool { return c.state != nil }

// Mode returns the selection mode used to construct the owning Set.
func (c Capabilities) Mode() Mode { return c.state.mode }

// Arch returns the runtime architecture used for dispatch selection.
func (c Capabilities) Arch() string { return c.state.arch }

// For reports an exact backend. Unknown IDs are never silently scalar.
func (c Capabilities) For(id KernelID) (KernelCapability, bool) {
	if c.state == nil {
		return KernelCapability{}, false
	}
	capability, ok := c.state.byID[id]
	return capability, ok
}

type int32CompareFunc func([]int32, memory.Bitmap, memory.Bitmap, int32) int
type int64CompareFunc func([]int64, memory.Bitmap, memory.Bitmap, int64) int
type uint32CompareFunc func([]uint32, memory.Bitmap, memory.Bitmap, uint32) int
type uint64CompareFunc func([]uint64, memory.Bitmap, memory.Bitmap, uint64) int
type float64CompareFunc func([]float64, memory.Bitmap, memory.Bitmap, float64) int
type float64ArithmeticFunc func(left, right, out []float64, validity memory.Bitmap)
type int32ArithmeticFunc func(left, right, out []int32, validity, overflow memory.Bitmap) int
type int64ArithmeticFunc func(left, right, out []int64, validity, overflow memory.Bitmap) int
type uint32ArithmeticFunc func(left, right, out []uint32, validity, overflow memory.Bitmap) int
type uint64ArithmeticFunc func(left, right, out []uint64, validity, overflow memory.Bitmap) int
type boolBinaryFunc func(aTruth, aValidity, bTruth, bValidity, outTruth, outValidity memory.Bitmap, rows int)
type boolNotFunc func(truth, validity, outTruth, outValidity memory.Bitmap, rows int)
type hash64BatchFunc func(keys, out []uint64)
type hash128BatchFunc func(a, b []uint64, validMasks []uint8, out []uint64)
type int64ScaleFunc func(values []int64, validity memory.Bitmap, factor int64) int
type stringLengthCompareFunc func(offsets []uint32, data []byte, validity, out memory.Bitmap, literal int64) int
type int32CompactFunc func(values []int32, selection memory.Bitmap, out []int32) int
type int64CompactFunc func(values []int64, selection memory.Bitmap, out []int64) int
type uint32CompactFunc func(values []uint32, selection memory.Bitmap, out []uint32) int
type uint64CompactFunc func(values []uint64, selection memory.Bitmap, out []uint64) int
type float64CompactFunc func(values []float64, selection memory.Bitmap, out []float64) int

type dispatch struct {
	compareInt32      [CompareEQ + 1]int32CompareFunc
	compareInt64      [CompareEQ + 1]int64CompareFunc
	compareUint32     [CompareEQ + 1]uint32CompareFunc
	compareUint64     [CompareEQ + 1]uint64CompareFunc
	compareFloat64    [CompareEQ + 1]float64CompareFunc
	arithmeticInt32   [ArithmeticDiv + 1]int32ArithmeticFunc
	arithmeticInt64   [ArithmeticDiv + 1]int64ArithmeticFunc
	arithmeticUint32  [ArithmeticDiv + 1]uint32ArithmeticFunc
	arithmeticUint64  [ArithmeticDiv + 1]uint64ArithmeticFunc
	arithmeticFloat64 [ArithmeticDiv + 1]float64ArithmeticFunc
	boolAnd           boolBinaryFunc
	boolOr            boolBinaryFunc
	boolNot           boolNotFunc
	hash64            hash64BatchFunc
	hash128           hash128BatchFunc
	scaleInt64        int64ScaleFunc
	stringByteLength  [CompareEQ + 1]stringLengthCompareFunc
	stringUTF8Length  [CompareEQ + 1]stringLengthCompareFunc
	compactInt32      int32CompactFunc
	compactInt64      int64CompactFunc
	compactUint32     uint32CompactFunc
	compactUint64     uint64CompactFunc
	compactFloat64    float64CompactFunc
	caps              *capabilityState
}

// Set is an immutable, comparable handle to one complete dispatch table.
type Set struct{ d *dispatch }

// New constructs a complete kernel set for mode and the current architecture.
func New(mode Mode) Set {
	s := newScalarSet(mode)
	if mode != ModeScalar {
		installSIMD(&s, mode)
	}
	return s
}

// Valid reports whether s was constructed by New.
func (s Set) Valid() bool { return s.d != nil }

// Mode returns the dispatch selection mode.
func (s Set) Mode() Mode { return s.d.caps.mode }

// Arch returns the runtime architecture used to construct s.
func (s Set) Arch() string { return s.d.caps.arch }

// Capabilities returns an immutable operation-level capability view.
func (s Set) Capabilities() Capabilities { return Capabilities{state: s.d.caps} }

// Capability reports the selected backend for one exact operation.
func (s Set) Capability(id KernelID) (KernelCapability, bool) { return s.Capabilities().For(id) }

// CompareInt32 compares nullable int32 values and writes a packed selection bitmap.
func (s Set) CompareInt32(v []int32, valid, out memory.Bitmap, lit int32, op CompareOp) int {
	return s.d.compareInt32[op](v, valid, out, lit)
}

// CompareInt64 compares nullable int64 values and writes a packed selection bitmap.
func (s Set) CompareInt64(v []int64, valid, out memory.Bitmap, lit int64, op CompareOp) int {
	return s.d.compareInt64[op](v, valid, out, lit)
}

// CompareUint32 compares nullable uint32 values and writes a packed selection bitmap.
func (s Set) CompareUint32(v []uint32, valid, out memory.Bitmap, lit uint32, op CompareOp) int {
	return s.d.compareUint32[op](v, valid, out, lit)
}

// CompareUint64 compares nullable uint64 values and writes a packed selection bitmap.
func (s Set) CompareUint64(v []uint64, valid, out memory.Bitmap, lit uint64, op CompareOp) int {
	return s.d.compareUint64[op](v, valid, out, lit)
}

// CompareFloat64 compares nullable float64 values with IEEE comparison semantics.
func (s Set) CompareFloat64(v []float64, valid, out memory.Bitmap, lit float64, op CompareOp) int {
	return s.d.compareFloat64[op](v, valid, out, lit)
}

// Int32Arithmetic performs checked nullable int32 arithmetic and returns the first overflow row.
func (s Set) Int32Arithmetic(a, b, out []int32, valid, overflow memory.Bitmap, op ArithmeticOp) int {
	return s.d.arithmeticInt32[op](a, b, out, valid, overflow)
}

// Int64Arithmetic performs checked nullable int64 arithmetic and returns the first overflow row.
func (s Set) Int64Arithmetic(a, b, out []int64, valid, overflow memory.Bitmap, op ArithmeticOp) int {
	return s.d.arithmeticInt64[op](a, b, out, valid, overflow)
}

// Uint32Arithmetic performs checked nullable uint32 arithmetic and returns the first overflow row.
func (s Set) Uint32Arithmetic(a, b, out []uint32, valid, overflow memory.Bitmap, op ArithmeticOp) int {
	return s.d.arithmeticUint32[op](a, b, out, valid, overflow)
}

// Uint64Arithmetic performs checked nullable uint64 arithmetic and returns the first overflow row.
func (s Set) Uint64Arithmetic(a, b, out []uint64, valid, overflow memory.Bitmap, op ArithmeticOp) int {
	return s.d.arithmeticUint64[op](a, b, out, valid, overflow)
}

// Float64Arithmetic performs nullable float64 arithmetic.
func (s Set) Float64Arithmetic(a, b, out []float64, valid memory.Bitmap, op ArithmeticOp) {
	s.d.arithmeticFloat64[op](a, b, out, valid)
}

// BoolAnd applies packed SQL three-valued AND.
func (s Set) BoolAnd(at, av, bt, bv, out, outv memory.Bitmap, rows int) {
	s.d.boolAnd(at, av, bt, bv, out, outv, rows)
}

// BoolOr applies packed SQL three-valued OR.
func (s Set) BoolOr(at, av, bt, bv, out, outv memory.Bitmap, rows int) {
	s.d.boolOr(at, av, bt, bv, out, outv, rows)
}

// BoolNot applies packed SQL three-valued NOT.
func (s Set) BoolNot(t, v, out, outv memory.Bitmap, rows int) { s.d.boolNot(t, v, out, outv, rows) }

// Hash64Batch computes canonical hashes for a batch of uint64 keys.
func (s Set) Hash64Batch(keys, out []uint64) { s.d.hash64(keys, out) }

// Hash128Batch computes canonical hashes for nullable two-part keys.
func (s Set) Hash128Batch(a, b []uint64, valid []uint8, out []uint64) {
	s.d.hash128(a, b, valid, out)
}

// ScaleInt64 multiplies valid values in place and returns the first overflow row.
func (s Set) ScaleInt64(values []int64, valid memory.Bitmap, factor int64) int {
	return s.d.scaleInt64(values, valid, factor)
}

// CompareStringLength compares byte or UTF-8 rune lengths and writes a packed bitmap.
func (s Set) CompareStringLength(offsets []uint32, data []byte, valid, out memory.Bitmap, lit int64, op CompareOp, utf8Length bool) int {
	if utf8Length {
		return s.d.stringUTF8Length[op](offsets, data, valid, out, lit)
	}
	return s.d.stringByteLength[op](offsets, data, valid, out, lit)
}

// CompactInt32 compacts selected int32 values into out and returns the output length.
func (s Set) CompactInt32(v []int32, selection memory.Bitmap, out []int32) int {
	return s.d.compactInt32(v, selection, out)
}

// CompactInt64 compacts selected int64 values into out and returns the output length.
func (s Set) CompactInt64(v []int64, selection memory.Bitmap, out []int64) int {
	return s.d.compactInt64(v, selection, out)
}

// CompactUint32 compacts selected uint32 values into out and returns the output length.
func (s Set) CompactUint32(v []uint32, selection memory.Bitmap, out []uint32) int {
	return s.d.compactUint32(v, selection, out)
}

// CompactUint64 compacts selected uint64 values into out and returns the output length.
func (s Set) CompactUint64(v []uint64, selection memory.Bitmap, out []uint64) int {
	return s.d.compactUint64(v, selection, out)
}

// CompactFloat64 compacts selected float64 values into out and returns the output length.
func (s Set) CompactFloat64(v []float64, selection memory.Bitmap, out []float64) int {
	return s.d.compactFloat64(v, selection, out)
}

func (s Set) setCapability(id KernelID, capability KernelCapability) { s.d.caps.byID[id] = capability }

func newScalarSet(mode Mode) Set {
	d := &dispatch{caps: &capabilityState{mode: mode, arch: runtimeArch(), byID: make(map[KernelID]KernelCapability, len(allKernelIDs))}}
	installGeneratedScalar(d)
	d.boolAnd, d.boolOr, d.boolNot = scalarBoolAnd, scalarBoolOr, scalarBoolNot
	d.hash64, d.hash128, d.scaleInt64 = scalarHash64Batch, scalarHash128Batch, scalarInt64Scale
	d.compactInt32, d.compactInt64 = scalarInt32Compact, scalarInt64Compact
	d.compactUint32, d.compactUint64, d.compactFloat64 = scalarUint32Compact, scalarUint64Compact, scalarFloat64Compact
	for _, id := range allKernelIDs {
		d.caps.byID[id] = KernelCapability{Backend: "scalar"}
	}
	return Set{d: d}
}
