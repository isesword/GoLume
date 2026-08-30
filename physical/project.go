package physical

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/kernel"
	"gocolumn/memory"
	"gocolumn/source"
)

// ProjectSource computes typed scalar projection expressions over an immutable
// child source. Phase 2.6 compiles every computed output into one fused
// ProjectionProgram so common subexpressions are evaluated once per batch.
type ProjectSource struct {
	input        source.Source
	schema       memory.Schema
	named        []expr.NamedExpr
	program      expr.ProjectionProgram
	includeInput bool
	manager      memory.Manager
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

// NewReader creates a reader that removes only temporary vector bindings.
func (s *DropColumnsSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	reader, err := s.input.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &dropColumnsReader{child: reader, drop: append([]string(nil), s.drop...)}, nil
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

type dropColumnsReader struct {
	child source.Reader
	drop  []string
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

// Close releases the wrapped source reader.
func (r *dropColumnsReader) Close() error { return r.child.Close() }

// ReadPartition reads one partition and removes temporary bindings.
func (r *dropColumnsReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if err := r.child.ReadPartition(ctx, p, dst); err != nil {
		return err
	}
	for _, name := range r.drop {
		removeColumn(dst, name)
	}
	return nil
}

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

// NewProjectSource compiles named scalar expressions against input. When
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
		var field memory.Field
		if c, ok := n.Expr.(expr.Column); ok {
			f, exists := in.Field(c.Name)
			if !exists {
				return nil, fmt.Errorf("projection column %q not found", c.Name)
			}
			f.Name = name
			field = f
		} else {
			info, err := expr.InferScalarType(in, n.Expr)
			if err != nil {
				return nil, fmt.Errorf("projection %q: %w", name, err)
			}
			field = memory.Field{Name: name, Type: info.Type, Nullable: info.Nullable, Encoding: memory.EncodingPlain}
			if info.Type == memory.TypeString || info.Type == memory.TypeBinary {
				field.Encoding = memory.EncodingView
			} else if info.Type == memory.TypeList {
				elementNullable := false
				if ternary, ok := n.Expr.(expr.Ternary); ok && ternary.Op == expr.OpRegexExtractAll {
					elementNullable = true
				}
				field = memory.ListStringField(name, info.Nullable, elementNullable)
			} else if info.Type == memory.TypeStruct {
				decoded, ok := n.Expr.(expr.JSONDecodeExpr)
				if !ok {
					return nil, fmt.Errorf("projection %q struct expression is unsupported", name)
				}
				field = memory.StructField(name, true, decoded.Schema.Fields...)
			}
		}
		if pos, exists := positions[name]; exists {
			fields[pos] = field
		} else {
			positions[name] = len(fields)
			fields = append(fields, field)
		}
	}
	program, err := expr.CompileProjection(in, named)
	if err != nil {
		return nil, err
	}
	var manager memory.Manager
	if len(managers) != 0 {
		manager = managers[0]
	}
	return &ProjectSource{
		input:        input,
		schema:       memory.NewSchema(fields...),
		named:        append([]expr.NamedExpr(nil), named...),
		program:      program,
		includeInput: includeInput,
		manager:      manager,
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

// NewReader creates an independent projection reader.
func (s *ProjectSource) NewReader(ctx context.Context, opts source.ReadOptions) (source.Reader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	r, err := s.input.NewReader(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &projectReader{owner: s, child: r, input: memory.NewBatch(), kernels: opts.Kernels}, nil
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
	child, err := openUnaryInputStream(ctx, s.input, opts, parts, prepared)
	if err != nil {
		return nil, err
	}
	stream := &projectBatchStream{owner: s, input: memory.NewBatch(), kernels: opts.Kernels}
	stream.state.child = child
	stream.state.closeOwn = func() error {
		stream.input.ReleaseOwned()
		return stream.workspace.Close()
	}
	return stream, nil
}

type projectReader struct {
	owner     *ProjectSource
	child     source.Reader
	input     *memory.Batch
	workspace expr.ScalarWorkspace
	kernels   kernel.Set
}

// Close releases the child reader.
func (r *projectReader) Close() error {
	_ = r.workspace.Close()
	return r.child.Close()
}

// ReadPartition evaluates the fused projection program for one child partition.
func (r *projectReader) ReadPartition(ctx context.Context, p source.Partition, dst *memory.Batch) error {
	if err := r.child.ReadPartition(ctx, p, r.input); err != nil {
		return err
	}
	return applyProjectBatch(ctx, r.owner, r.input, dst, &r.workspace, r.kernels)
}

func applyProjectBatch(ctx context.Context, owner *ProjectSource, input, dst *memory.Batch, workspace *expr.ScalarWorkspace, kernels kernel.Set) error {
	workspace.SetMemoryManager(ctx, owner.manager)
	values, err := owner.program.Evaluate(input, workspace, kernels)
	if err != nil {
		return err
	}
	if len(values) != len(owner.program.Outputs) {
		return fmt.Errorf("projection output count mismatch")
	}
	dst.ClearColumns()
	if owner.includeInput {
		bindAll(dst, input)
	}
	for i, out := range owner.program.Outputs {
		removeColumn(dst, out.Name)
		if err := bindScalarValue(dst, out.Name, values[i]); err != nil {
			return err
		}
	}
	dst.Reset(input.Rows)
	return nil
}

type projectBatchStream struct {
	state     unaryStreamState
	owner     *ProjectSource
	input     *memory.Batch
	workspace expr.ScalarWorkspace
	kernels   kernel.Set
}

// Next evaluates the fused projection over the next child Batch.
func (s *projectBatchStream) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if stop, err := s.state.before(ctx, dst); stop {
		return false, err
	}
	ok, err := s.state.child.Next(ctx, s.input)
	if err != nil {
		dst.ClearColumns()
		return false, s.state.fail(err)
	}
	if !ok {
		dst.ClearColumns()
		return false, s.state.finish()
	}
	if err := applyProjectBatch(ctx, s.owner, s.input, dst, &s.workspace, s.kernels); err != nil {
		dst.ClearColumns()
		return false, s.state.fail(err)
	}
	return true, nil
}

// Close releases projection reservations and the child stream.
func (s *projectBatchStream) Close() error { return s.state.Close() }

func bindScalarValue(dst *memory.Batch, name string, v expr.ScalarValue) error {
	switch {
	case v.Int32 != nil:
		dst.Int32s[name] = v.Int32
	case v.Int64 != nil:
		dst.Int64s[name] = v.Int64
	case v.Uint32 != nil:
		dst.Uint32s[name] = v.Uint32
	case v.Uint64 != nil:
		dst.Uint64s[name] = v.Uint64
	case v.Float64 != nil:
		dst.Float64s[name] = v.Float64
	case v.DictionaryString != nil:
		dst.DictStrings[name] = v.DictionaryString
	case v.String != nil:
		dst.Strings[name] = v.String
	case v.StringList != nil:
		dst.StringLists[name] = v.StringList
	case v.Struct != nil:
		dst.Structs[name] = v.Struct
	default:
		return fmt.Errorf("projection %q type %s has no physical vector", name, v.Type)
	}
	return nil
}

func bindAll(dst, src *memory.Batch) {
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
	delete(dst.Int64s, name)
	delete(dst.Int32s, name)
	delete(dst.Uint32s, name)
	delete(dst.Uint64s, name)
	delete(dst.Float64s, name)
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
