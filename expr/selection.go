package expr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"slices"
	"unsafe"
)

// SupportsSelection reports whether this program can execute on a dense,
// selected fixed-width input domain. Unsupported programs use ordinary batch
// evaluation. The whitelist is deliberately closed to future opcodes.
func (p Program) SupportsSelection() bool {
	return p.selection != nil
}

// selectionPlan is immutable compile-time metadata. Runtime initialization
// reserves space before copying these column descriptors into mutable state;
// neither capability checks nor execution allocate a DAG traversal map.
type selectionPlan struct {
	columns []selectedColumn
}

func (p *Program) planSelection() {
	p.selection = nil
	if len(p.instructions) == 0 {
		return
	}
	columns := make(map[string]memory.DataType)
	supported := p.visitInstructions(func(in instruction) bool {
		if selectionWidth(instructionResultType(in)) == 0 {
			return false
		}
		switch in.Code {
		case scalarColumn, scalarLiteralI64, scalarLiteralF64, scalarLiteralBool,
			scalarLiteralNull, scalarBinary, scalarCast, scalarTryCast, scalarUnary,
			scalarCompare, scalarBoolean, scalarConditional, codeConstTrue,
			codeCmpScalar, codeAnd, codeOr, codeNot, codeIsNull, codeIsNotNull, codeCmpExtract:
		default:
			return false
		}
		if in.Column != "" {
			if selectionWidth(in.Type) == 0 {
				return false
			}
			columns[in.Column] = in.Type
		}
		if in.RightColumn != "" {
			columns[in.RightColumn] = in.Type
		}
		return true
	})
	if !supported {
		return
	}
	plan := &selectionPlan{columns: make([]selectedColumn, 0, len(columns))}
	for name, typ := range columns {
		plan.columns = append(plan.columns, selectedColumn{name: name, typ: typ})
	}
	slices.SortFunc(plan.columns, func(a, b selectedColumn) int { return cmp.Compare(a.name, b.name) })
	p.selection = plan
}

func selectionWidth(t memory.DataType) int64 {
	switch t {
	case memory.TypeInt8, memory.TypeUint8:
		return 1
	case memory.TypeInt16, memory.TypeUint16:
		return 2
	case memory.TypeBool, memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32,
		memory.TypeDate32, memory.TypeTime32:
		return 4
	case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64,
		memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return 8
	}
	return 0
}

type selectedColumn struct {
	name     string
	typ      memory.DataType
	validity memory.Bitmap
}
type selectedWorkspace struct {
	initialized   bool
	metadataLease memory.Reservation
	metadataBytes int64
	columns       []selectedColumn
	batch         *memory.Batch
	capacity      int
	lease         memory.Reservation
	bytes         int64
}

func (s *selectedWorkspace) close() {
	if s.lease != nil {
		s.lease.Release()
	}
	if s.metadataLease != nil {
		s.metadataLease.Release()
	}
	*s = selectedWorkspace{}
}
func (s *selectedWorkspace) init(ctx context.Context, p Program, manager memory.Manager) error {
	if s.initialized {
		return nil
	}
	if !p.SupportsSelection() {
		return fmt.Errorf("expression program does not support selected evaluation")
	}
	count := len(p.selection.columns)
	n, err := scalarBytes(count, unsafe.Sizeof(selectedColumn{}))
	if err != nil {
		return err
	}
	lease, err := manager.Reserve(ctx, n)
	if err != nil {
		return fmt.Errorf("reserve selected expression metadata: %w", err)
	}
	s.columns = make([]selectedColumn, count)
	copy(s.columns, p.selection.columns)
	s.metadataLease, s.metadataBytes, s.initialized = lease, n, true
	return nil
}

// EvaluateSelected gathers only the fixed-width columns referenced by the
// program, then runs the ordinary executor on the dense selected domain. Each
// referenced input is gathered once even when several outputs use it. Results
// borrow workspace storage until the next evaluation or Close. Nil rows means
// all input rows; a nonnil empty selection means zero rows. Row indices must be
// strictly increasing and in range. No expression executes on rejected rows.
func (w *Workspace) EvaluateSelected(ctx context.Context, batch *memory.Batch, rows []int, kernels kernel.Set) (values []ScalarValue, err error) {
	if w == nil || w.closed {
		return nil, fmt.Errorf("expression workspace is closed")
	}
	if ctx == nil {
		return nil, fmt.Errorf("expression evaluation context is nil")
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil && q != w.query {
		return nil, fmt.Errorf("expression workspace cannot change query ownership")
	}
	if rows == nil {
		return w.Evaluate(ctx, batch, kernels)
	}
	if !w.program.SupportsSelection() {
		return nil, fmt.Errorf("expression program does not support selected evaluation")
	}
	defer func() {
		if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
			_ = w.Close()
		}
	}()
	if batch == nil {
		return nil, fmt.Errorf("expression evaluation requires batch")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = validWorkspaceRows(len(rows)); err != nil {
		return nil, err
	}
	previous := -1
	for i, row := range rows {
		if i&4095 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
		}
		if row <= previous || row >= batch.Rows {
			return nil, fmt.Errorf("invalid selected expression row %d", row)
		}
		previous = row
	}
	// Results expire at the next evaluation. Drop logical views recursively
	// before replacing gathered buffers and releasing their reservation. A guarded
	// region skipped by the next evaluation would otherwise retain the old input.
	w.storage.clearSelectedViews()
	if err = w.selected.init(ctx, w.program, w.manager); err != nil {
		return nil, err
	}
	if err = w.selected.gather(ctx, batch, rows, w.manager); err != nil {
		return nil, err
	}
	return w.Evaluate(ctx, w.selected.batch, kernels)
}

func (s *selectedWorkspace) gather(ctx context.Context, input *memory.Batch, rows []int, manager memory.Manager) error {
	// Validate before allocating or reading vectors. Only referenced columns are
	// relevant to this evaluation; unrelated input payloads remain untouched.
	for _, c := range s.columns {
		if err := validateBoundColumn(input, c.name, c.typ); err != nil {
			return err
		}
	}
	if s.batch == nil || len(rows) > s.capacity {
		n := int64(0)
		for _, c := range s.columns {
			width := selectionWidth(c.typ)
			validity := int64(memory.WordsFor(len(rows))) * 8
			if int64(len(rows)) > (math.MaxInt64-validity-n)/width {
				return fmt.Errorf("selected expression size overflow")
			}
			n += int64(len(rows))*width + validity
		}
		lease, err := manager.Reserve(ctx, n)
		if err != nil {
			return fmt.Errorf("reserve selected expression input: %w", err)
		}
		next := memory.NewBatch()
		for i := range s.columns {
			c := &s.columns[i]
			c.validity = make(memory.Bitmap, memory.WordsFor(len(rows)))
			allocateSelectedColumn(next, *c, len(rows))
		}
		old := s.lease
		s.batch, s.lease, s.capacity, s.bytes = next, lease, len(rows), n
		if old != nil {
			old.Release()
		}
	}
	for _, c := range s.columns {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := gatherSelectedColumn(ctx, s.batch, input, c, rows); err != nil {
			return err
		}
	}
	s.batch.Rows = len(rows)
	return nil
}
func gatherSelectedValues[T any](ctx context.Context, dst []T, validity memory.Bitmap, src []T, srcValidity memory.Bitmap, rows []int) error {
	clear(validity)
	for i, row := range rows {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		dst[i] = src[row]
		if srcValidity != nil && memory.IsSet(srcValidity, row) {
			memory.Set(validity, i)
		}
	}
	return nil
}
func allocateSelectedColumn(b *memory.Batch, c selectedColumn, rows int) {
	switch c.typ {
	case memory.TypeInt8:
		b.Int8s[c.name] = &memory.Int8Vector{Values: make([]int8, rows), Validity: c.validity}
	case memory.TypeInt16:
		b.Int16s[c.name] = &memory.Int16Vector{Values: make([]int16, rows), Validity: c.validity}
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		b.Int32s[c.name] = &memory.Int32Vector{Values: make([]int32, rows), Validity: c.validity}
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		b.Int64s[c.name] = &memory.Int64Vector{Values: make([]int64, rows), Validity: c.validity}
	case memory.TypeUint8:
		b.Uint8s[c.name] = &memory.Uint8Vector{Values: make([]uint8, rows), Validity: c.validity}
	case memory.TypeUint16:
		b.Uint16s[c.name] = &memory.Uint16Vector{Values: make([]uint16, rows), Validity: c.validity}
	case memory.TypeUint32, memory.TypeBool:
		b.Uint32s[c.name] = &memory.Uint32Vector{Values: make([]uint32, rows), Validity: c.validity}
	case memory.TypeUint64:
		b.Uint64s[c.name] = &memory.Uint64Vector{Values: make([]uint64, rows), Validity: c.validity}
	case memory.TypeFloat32:
		b.Float32s[c.name] = &memory.Float32Vector{Values: make([]float32, rows), Validity: c.validity}
	case memory.TypeFloat64:
		b.Float64s[c.name] = &memory.Float64Vector{Values: make([]float64, rows), Validity: c.validity}
	}
}
func gatherSelectedColumn(ctx context.Context, dst, src *memory.Batch, c selectedColumn, rows []int) error {
	switch c.typ {
	case memory.TypeInt8:
		d, v := dst.Int8s[c.name], src.Int8s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeInt16:
		d, v := dst.Int16s[c.name], src.Int16s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		d, v := dst.Int32s[c.name], src.Int32s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		d, v := dst.Int64s[c.name], src.Int64s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeUint8:
		d, v := dst.Uint8s[c.name], src.Uint8s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeUint16:
		d, v := dst.Uint16s[c.name], src.Uint16s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeUint32, memory.TypeBool:
		d, v := dst.Uint32s[c.name], src.Uint32s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeUint64:
		d, v := dst.Uint64s[c.name], src.Uint64s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeFloat32:
		d, v := dst.Float32s[c.name], src.Float32s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	case memory.TypeFloat64:
		d, v := dst.Float64s[c.name], src.Float64s[c.name]
		d.Values, d.Validity = d.Values[:len(rows)], c.validity[:memory.WordsFor(len(rows))]
		d.Encoding = v.Encoding
		if v.Validity == nil {
			d.Validity = nil
		}
		return gatherSelectedValues(ctx, d.Values, d.Validity, v.Values, v.Validity, rows)
	}
	return fmt.Errorf("unsupported selected column type %s", c.typ)
}

// clearSelectedViews invalidates logical values without discarding reusable
// physical owners. Selected programs are fixed-width: scalarColumn binds into
// regs, never owned; all non-column physical buffers remain workspace-owned.
func (w *evaluationState) clearSelectedViews() {
	if w == nil {
		return
	}
	clear(w.regs[:cap(w.regs)])
	clear(w.outputs[:cap(w.outputs)])
	w.activeRows = nil
	for i := range w.owned[:cap(w.owned)] {
		for _, region := range w.owned[:cap(w.owned)][i].regions {
			region.clearSelectedViews()
		}
	}
}
