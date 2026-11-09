package physical

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

// ProjectSource computes typed projection expressions over an immutable child
// source. All outputs share one expression program, including Boolean values,
// so common subexpressions reuse the same evaluation domain.
type ProjectSource struct {
	input            source.Source
	schema           memory.Schema
	named            []expr.NamedExpr
	program          expr.Program
	includeInput     bool
	manager          memory.Manager
	selectionEnabled bool
}

// DropColumnsSource removes internal columns without evaluating or copying the
// remaining vectors. It is used by physical plans that materialize temporary
// keys (for example expression ordering) but must preserve the logical input
// schema, including opaque Arrow columns.
type DropColumnsSource struct {
	input  source.Source
	schema memory.Schema
	drop   []string
}

// NewDropColumnsSource returns a source that hides columns from input. Every
// requested column must be present; an empty drop list returns input unchanged.
func NewDropColumnsSource(input source.Source, columns []string) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("drop-columns input is nil")
	}
	if len(columns) == 0 {
		return input, nil
	}
	dropSet := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		if _, exists := input.Schema().Field(name); !exists {
			return nil, fmt.Errorf("drop-columns column %q not found", name)
		}
		dropSet[name] = struct{}{}
	}
	fields := make([]memory.Field, 0, len(input.Schema().Fields)-len(dropSet))
	for _, field := range input.Schema().Fields {
		if _, drop := dropSet[field.Name]; !drop {
			fields = append(fields, field)
		}
	}
	drop := make([]string, 0, len(dropSet))
	for _, field := range input.Schema().Fields {
		if _, exists := dropSet[field.Name]; exists {
			drop = append(drop, field.Name)
		}
	}
	return &DropColumnsSource{input: input, schema: memory.NewSchema(fields...), drop: drop}, nil
}

// Schema returns the input schema without dropped columns.
func (s *DropColumnsSource) Schema() memory.Schema { return s.schema }

// Replayability forwards the child snapshot contract.
func (s *DropColumnsSource) Replayability() source.Replayability {
	return source.ReplayabilityOf(s.input)
}

// Ordering preserves child keys that remain visible after dropping internal
// projection columns. The guaranteed prefix ends at the first removed key.
func (s *DropColumnsSource) Ordering() []source.OrderingColumn {
	provider, ok := s.input.(source.OrderingProvider)
	if !ok {
		return nil
	}
	out := provider.Ordering()
	for index, key := range out {
		if _, visible := s.schema.Field(key.Name); !visible {
			return append([]source.OrderingColumn(nil), out[:index]...)
		}
	}
	return append([]source.OrderingColumn(nil), out...)
}

// StreamPlan reports a zero-copy batch-local projection node.
func (s *DropColumnsSource) StreamPlan() source.StreamPlanNode {
	child := source.StreamPlanOf(s.input)
	return source.StreamPlanNode{
		Operator:          "DropColumns",
		Class:             source.StreamPipeline,
		PartitionParallel: child.PartitionParallel,
		Children:          []source.StreamPlanNode{child},
	}
}

// Stats preserves input row and partition statistics.
func (s *DropColumnsSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

// Partitions preserves input partition boundaries.
func (s *DropColumnsSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewBatchStream creates a zero-copy native stream over the child.
func (s *DropColumnsSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, nil, false)
}

// NewBatchStreamWithPartitions propagates prepared partition metadata.
func (s *DropColumnsSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, parts, true)
}

func (s *DropColumnsSource) newBatchStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		return nil, err
	}
	return &dropColumnsBatchStream{state: unaryStreamState{child: child}, drop: append([]string(nil), s.drop...)}, nil
}

type dropColumnsBatchStream struct {
	state unaryStreamState
	drop  []string
}

// Next returns the next child Batch after removing hidden bindings.
func (s *dropColumnsBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if stop, err := s.state.before(ctx, dst); stop {
		return false, err
	}
	ok, err := s.state.child.Next(ctx, dst)
	if err != nil {
		dst.ClearColumns()
		return false, s.state.fail(err)
	}
	if !ok {
		dst.ClearColumns()
		return false, s.state.finish()
	}
	for _, name := range s.drop {
		removeColumn(dst, name)
	}
	return true, nil
}

// Close releases the child stream.
func (s *dropColumnsBatchStream) Close() error { return s.state.Close() }

// Dictionary forwards dictionary metadata for columns that remain visible.
func (s *DropColumnsSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	if _, exists := s.schema.Field(column); !exists {
		return source.Dictionary{}, fmt.Errorf("dropped column %q has no dictionary", column)
	}
	dp, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("drop-columns input has no dictionary provider")
	}
	return dp.Dictionary(ctx, column)
}

// NewProjectSource compiles named typed expressions against input. When
// includeInput is true, input fields are retained and aliases replace matching
// columns; otherwise only the named expressions are exposed.
func NewProjectSource(input source.Source, named []expr.NamedExpr, includeInput bool, managers ...memory.Manager) (*ProjectSource, error) {
	if input == nil {
		return nil, fmt.Errorf("physical projection input is nil")
	}
	if len(named) == 0 {
		return nil, fmt.Errorf("physical projection requires expressions")
	}
	in := input.Schema()
	fields := make([]memory.Field, 0, len(in.Fields)+len(named))
	positions := make(map[string]int)
	if includeInput {
		fields = append(fields, in.Fields...)
		for i, f := range fields {
			positions[f.Name] = i
		}
	}
	produced := make(map[string]struct{}, len(named))
	for _, n := range named {
		name, err := n.Name()
		if err != nil {
			return nil, err
		}
		if _, dup := produced[name]; dup {
			return nil, fmt.Errorf("physical projection contains duplicate output %q", name)
		}
		produced[name] = struct{}{}
		bound, err := expr.Bind(in, n.Expr)
		if err != nil {
			return nil, fmt.Errorf("projection %q: %w", name, err)
		}
		field := bound.OutputField(name)
		if pos, exists := positions[name]; exists {
			fields[pos] = field
		} else {
			positions[name] = len(fields)
			fields = append(fields, field)
		}
	}
	program, err := expr.CompileExpressions(in, named)
	if err != nil {
		return nil, err
	}
	var manager memory.Manager
	if len(managers) != 0 {
		manager = managers[0]
	}
	selectionEnabled := supportsSelectedProject(input, named, includeInput, program)
	return &ProjectSource{
		selectionEnabled: selectionEnabled,
		input:            input,
		schema:           memory.NewSchema(fields...),
		named:            append([]expr.NamedExpr(nil), named...),
		program:          program,
		includeInput:     includeInput,
		manager:          manager,
	}, nil
}

// Schema returns the projected output schema.
func (s *ProjectSource) Schema() memory.Schema { return s.schema }

// Replayability forwards the child snapshot contract.
func (s *ProjectSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// StreamPlan reports a batch-local expression projection node.
func (s *ProjectSource) StreamPlan() source.StreamPlanNode {
	detail := fmt.Sprintf("outputs=%d", len(s.named))
	if s.includeInput {
		detail += " include_input=true"
	}
	child := source.StreamPlanOf(s.input)
	return source.StreamPlanNode{
		Operator:          "Project",
		Class:             source.StreamPipeline,
		Detail:            detail,
		PartitionParallel: child.PartitionParallel,
		Children:          []source.StreamPlanNode{child},
	}
}

// Stats preserves child row and partition statistics.
func (s *ProjectSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Partitions preserves child partition boundaries.
func (s *ProjectSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// NewBatchStream creates a native fused projection stream.
func (s *ProjectSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, nil, false)
}

// NewBatchStreamWithPartitions propagates prepared partition metadata.
func (s *ProjectSource) NewBatchStreamWithPartitions(ctx context.Context, opts source.ReadOptions, parts []source.Partition) (source.BatchStream, error) {
	return s.newBatchStream(ctx, opts, parts, true)
}

func (s *ProjectSource) newBatchStream(ctx context.Context, opts source.ReadOptions, parts []source.Partition, prepared bool) (source.BatchStream, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil {
		opts.MemoryManager = query.Operator("SourceScan", memory.MemoryWorkspace)
	}
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		return nil, err
	}
	localOwner := *s
	var selection *filterBatchStream
	if s.selectionEnabled {
		// The optimization is negotiated only between these concrete, internal
		// operators. Public streams continue to produce ordinary dense batches.
		selection, _ = child.(*filterBatchStream)
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil {
		localOwner.manager = query.Operator("Project", memory.MemoryWorkspace)
	} else if localOwner.manager == nil {
		localOwner.manager = memory.ClassifiedManager(opts.MemoryManager, "Project", memory.MemoryWorkspace)
	}
	workspace, err := expr.NewWorkspace(ctx, localOwner.program, localOwner.manager, "Project")
	if err != nil {
		return nil, errors.Join(err, child.Close())
	}
	stream := &projectBatchStream{owner: &localOwner, input: memory.NewBatch(), kernels: opts.Kernels, opts: opts.Normalize(), workspace: workspace, query: memory.QueryBudgetFromContext(ctx)}
	stream.selection = selection
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.clearBorrowed()
		stream.selected = nil
		stream.selection = nil
		stream.selectedCount = 0
		stream.releaseInputView()
		stream.input.ReleaseOwned()
		return stream.workspace.Close()
	}
	return stream, nil
}

func applyProjectBatch(ctx context.Context, owner *ProjectSource, input, dst *memory.Batch, workspace *expr.Workspace, kernels kernel.Set) error {
	return applySelectedProjectBatch(ctx, owner, input, dst, workspace, kernels, nil)
}

func applySelectedProjectBatch(ctx context.Context, owner *ProjectSource, input, dst *memory.Batch, workspace *expr.Workspace, kernels kernel.Set, rows []int) error {
	values, err := workspace.EvaluateSelected(ctx, input, rows, kernels)
	if err != nil {
		return err
	}
	if len(values) != owner.program.OutputCount() {
		return fmt.Errorf("projection output count mismatch")
	}
	dst.ClearColumns()
	if owner.includeInput {
		bindAll(dst, input)
	}
	for i := 0; i < owner.program.OutputCount(); i++ {
		name := owner.program.OutputName(i)
		removeColumn(dst, name)
		if err := bindScalarValue(dst, name, values[i]); err != nil {
			return err
		}
	}
	// Passthrough values may be backed by owned Arrow range slices in input.
	// The stream retains their owner; output clearing must never release it.
	borrowFilterOwners(dst)
	n := input.Rows
	if rows != nil {
		n = len(rows)
	}
	dst.Reset(n)
	return nil
}

type projectBatchStream struct {
	selection     *filterBatchStream
	selected      []int
	selectedCount int
	borrowed      *memory.Batch
	opts          source.ReadOptions
	inputOffset   int
	nextRows      int
	inputView     *memory.Batch
	viewLease     memory.Reservation
	state         unaryStreamState
	owner         *ProjectSource
	input         *memory.Batch
	workspace     *expr.Workspace
	query         *memory.QueryBudget
	kernels       kernel.Set
}

// Next evaluates bounded input ranges. Pure expression candidates that exceed
// payload or allocation budgets are discarded before a smaller prefix is tried;
// successful outputs retain their input view and workspace until the next call.
func (s *projectBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	s.clearBorrowed()
	if stop, err := s.state.before(ctx, dst); stop {
		return false, err
	}
	dst.ClearColumns()
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != s.query {
		return false, s.state.fail(fmt.Errorf("project stream cannot change query ownership"))
	} else if query == nil && s.query != nil {
		ctx = memory.WithQueryBudget(ctx, s.query)
	}
	s.releaseInputView()
	for s.inputOffset >= s.inputRows() {
		var ok bool
		var err error
		if s.selection != nil {
			// Drop the prior borrow before the filter replaces/releases rows.
			s.selected = nil
			ok, err = s.selection.nextSelection(ctx, s.input)
			s.selectedCount = s.selection.selectedRows
			if ok && s.selectedCount != s.input.Rows {
				s.selected = s.selection.rows[:s.selectedCount]
			}
		} else {
			ok, err = s.state.child.Next(ctx, s.input)
		}
		if err != nil {
			return false, s.state.fail(err)
		}
		if !ok {
			return false, s.state.finish()
		}
		s.inputOffset = 0
		if s.input.Rows == 0 {
			continue
		}
	}
	rows := s.inputRows() - s.inputOffset
	if s.nextRows > 0 && rows > s.nextRows {
		rows = s.nextRows
	}
	initialRows := rows
	for {
		if err := ctx.Err(); err != nil {
			return false, s.state.fail(err)
		}
		var err error
		if s.selected != nil {
			err = applySelectedProjectBatch(ctx, s.owner, s.input, dst, s.workspace, s.kernels, s.selected[s.inputOffset:s.inputOffset+rows])
		} else {
			err = s.bindInputView(ctx, rows)
			if err == nil {
				err = applyProjectBatch(ctx, s.owner, s.inputView, dst, s.workspace, s.kernels)
			}
		}
		nextRows := rows / 2
		if err == nil {
			selected, selectErr := memory.SelectBatchRangeRows(dst, 0, dst.Rows, s.opts.BatchPayloadTargetBytes, s.opts.BatchPayloadBytes)
			if selectErr != nil {
				err = selectErr
			} else if selected == rows {
				s.inputOffset += rows
				if s.nextRows == 0 || rows < initialRows || rows > s.nextRows {
					s.nextRows = rows
				}
				s.borrowed = dst
				return true, nil
			} else {
				nextRows = selected
			}
		}
		// Never hide semantic errors. Only resource pressure or an oversized
		// candidate may shrink; a single row must fit all hard limits.
		retry := rows > 1 && (err == nil || errors.Is(err, memory.ErrBudgetExceeded))
		dst.ClearColumns()
		s.releaseInputView()
		_ = s.workspace.Close()
		if !retry {
			return false, s.state.fail(err)
		}
		s.workspace, err = expr.NewWorkspace(ctx, s.owner.program, s.owner.manager, "Project")
		if err != nil {
			return false, s.state.fail(err)
		}
		if nextRows < 1 {
			nextRows = 1
		}
		if nextRows >= rows {
			nextRows = rows - 1
		}
		rows = nextRows
	}
}

func (s *projectBatchStream) inputRows() int {
	if s.selection != nil {
		return s.selectedCount
	}
	return s.input.Rows
}
func (s *projectBatchStream) clearBorrowed() {
	if s.borrowed != nil {
		s.borrowed.ClearColumns()
		s.borrowed = nil
	}
}

func (s *projectBatchStream) bindInputView(ctx context.Context, rows int) error {
	bytes, err := memory.BorrowedRangeMetadataBytes(s.input, s.inputOffset, rows)
	if err != nil {
		return err
	}
	if bytes > 0 && s.owner.manager != nil {
		s.viewLease, err = s.owner.manager.Reserve(ctx, bytes)
		if err != nil {
			return err
		}
	}
	s.inputView, err = memory.BorrowBatchRange(s.input, s.inputOffset, rows)
	if err != nil {
		s.releaseInputView()
	}
	return err
}
func (s *projectBatchStream) releaseInputView() {
	if s.inputView != nil {
		s.inputView.ClearColumns()
	}
	s.inputView = nil
	if s.viewLease != nil {
		s.viewLease.Release()
		s.viewLease = nil
	}
}

// Close releases projection reservations and the child stream.
func (s *projectBatchStream) Close() error { return s.state.Close() }

func bindScalarValue(dst *memory.Batch, name string, v expr.ScalarValue) error {
	switch {
	case v.Decimal != nil:
		dst.Decimals[name] = v.Decimal
	case v.Int8 != nil:
		dst.Int8s[name] = v.Int8
	case v.Int16 != nil:
		dst.Int16s[name] = v.Int16
	case v.Int32 != nil:
		dst.Int32s[name] = v.Int32
	case v.Int64 != nil:
		dst.Int64s[name] = v.Int64
	case v.Uint32 != nil:
		dst.Uint32s[name] = v.Uint32
	case v.Uint8 != nil:
		dst.Uint8s[name] = v.Uint8
	case v.Uint16 != nil:
		dst.Uint16s[name] = v.Uint16
	case v.Uint64 != nil:
		dst.Uint64s[name] = v.Uint64
	case v.Float64 != nil:
		dst.Float64s[name] = v.Float64
	case v.Float32 != nil:
		dst.Float32s[name] = v.Float32
	case v.DictionaryString != nil:
		dst.DictStrings[name] = v.DictionaryString
	case v.String != nil:
		dst.Strings[name] = v.String
	case v.StringList != nil:
		dst.StringLists[name] = v.StringList
	case v.Struct != nil:
		dst.Structs[name] = v.Struct
	case v.Opaque != nil:
		dst.Opaque[name] = v.Opaque
	default:
		return fmt.Errorf("projection %q type %s has no physical vector", name, v.Type)
	}
	return nil
}

func bindAll(dst, src *memory.Batch) {
	for k, v := range src.Int8s {
		dst.Int8s[k] = v
	}
	for k, v := range src.Int16s {
		dst.Int16s[k] = v
	}
	for k, v := range src.Uint8s {
		dst.Uint8s[k] = v
	}
	for k, v := range src.Uint16s {
		dst.Uint16s[k] = v
	}
	for k, v := range src.Float32s {
		dst.Float32s[k] = v
	}

	for k, v := range src.Int64s {
		dst.Int64s[k] = v
	}
	for k, v := range src.Int32s {
		dst.Int32s[k] = v
	}
	for k, v := range src.Uint32s {
		dst.Uint32s[k] = v
	}
	for k, v := range src.Uint64s {
		dst.Uint64s[k] = v
	}
	for k, v := range src.Decimals {
		dst.Decimals[k] = v
	}
	for k, v := range src.Float64s {
		dst.Float64s[k] = v
	}
	for k, v := range src.Strings {
		dst.Strings[k] = v
	}
	for k, v := range src.DictStrings {
		dst.DictStrings[k] = v
	}
	for k, v := range src.StringLists {
		dst.StringLists[k] = v
	}
	for k, v := range src.Structs {
		dst.Structs[k] = v
	}
	for k, v := range src.Opaque {
		dst.Opaque[k] = v
	}
}

func bindNamed(dst *memory.Batch, out string, src *memory.Batch, in string) error {
	if v, ok := src.Int8s[in]; ok {
		dst.Int8s[out] = v
		return nil
	}
	if v, ok := src.Int16s[in]; ok {
		dst.Int16s[out] = v
		return nil
	}
	if v, ok := src.Uint8s[in]; ok {
		dst.Uint8s[out] = v
		return nil
	}
	if v, ok := src.Uint16s[in]; ok {
		dst.Uint16s[out] = v
		return nil
	}
	if v, ok := src.Float32s[in]; ok {
		dst.Float32s[out] = v
		return nil
	}

	if v, ok := src.Int64s[in]; ok {
		dst.Int64s[out] = v
		return nil
	}
	if v, ok := src.Int32s[in]; ok {
		dst.Int32s[out] = v
		return nil
	}
	if v, ok := src.Uint32s[in]; ok {
		dst.Uint32s[out] = v
		return nil
	}
	if v, ok := src.Uint64s[in]; ok {
		dst.Uint64s[out] = v
		return nil
	}
	if v, ok := src.Decimals[in]; ok {
		dst.Decimals[out] = v
		return nil
	}
	if v, ok := src.Float64s[in]; ok {
		dst.Float64s[out] = v
		return nil
	}
	if v, ok := src.Strings[in]; ok {
		dst.Strings[out] = v
		return nil
	}
	if v, ok := src.DictStrings[in]; ok {
		dst.DictStrings[out] = v
		return nil
	}
	if v, ok := src.StringLists[in]; ok {
		dst.StringLists[out] = v
		return nil
	}
	if v, ok := src.Structs[in]; ok {
		dst.Structs[out] = v
		return nil
	}
	if v, ok := src.Opaque[in]; ok {
		dst.Opaque[out] = v
		return nil
	}
	return fmt.Errorf("projection input column %q not present in batch", in)
}

var _ source.StreamPlanProvider = (*DropColumnsSource)(nil)
var _ source.StreamPlanProvider = (*ProjectSource)(nil)
var _ source.ReplayabilityProvider = (*DropColumnsSource)(nil)
var _ source.ReplayabilityProvider = (*ProjectSource)(nil)
var _ source.BatchStreamProvider = (*DropColumnsSource)(nil)
var _ source.PreparedBatchStreamProvider = (*DropColumnsSource)(nil)
var _ source.BatchStream = (*dropColumnsBatchStream)(nil)
var _ source.BatchStreamProvider = (*ProjectSource)(nil)
var _ source.PreparedBatchStreamProvider = (*ProjectSource)(nil)
var _ source.BatchStream = (*projectBatchStream)(nil)

func removeColumn(dst *memory.Batch, name string) {
	delete(dst.Int8s, name)
	delete(dst.Int16s, name)
	delete(dst.Uint8s, name)
	delete(dst.Uint16s, name)
	delete(dst.Float32s, name)

	delete(dst.Int64s, name)
	delete(dst.Int32s, name)
	delete(dst.Uint32s, name)
	delete(dst.Uint64s, name)
	delete(dst.Float64s, name)
	delete(dst.Decimals, name)
	delete(dst.Strings, name)
	delete(dst.DictStrings, name)
	delete(dst.StringLists, name)
	delete(dst.Structs, name)
	delete(dst.Opaque, name)
}

// Dictionary forwards stable dictionary metadata for passthrough dictionary
// columns, including aliases. Computed expressions never expose dictionary
// encoding unless they are a direct passthrough column.
func (s *ProjectSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	for _, n := range s.named {
		name, err := n.Name()
		if err != nil || name != column {
			continue
		}
		c, ok := n.Expr.(expr.Column)
		if !ok {
			return source.Dictionary{}, fmt.Errorf("projection column %q has no dictionary", column)
		}
		dp, ok := s.input.(source.DictionaryProvider)
		if !ok {
			return source.Dictionary{}, fmt.Errorf("projection input has no dictionary provider")
		}
		return dp.Dictionary(ctx, c.Name)
	}
	if s.includeInput {
		if _, ok := s.input.Schema().Field(column); ok {
			dp, ok := s.input.(source.DictionaryProvider)
			if !ok {
				return source.Dictionary{}, fmt.Errorf("projection input has no dictionary provider")
			}
			return dp.Dictionary(ctx, column)
		}
	}
	return source.Dictionary{}, fmt.Errorf("projection column %q has no dictionary", column)
}
