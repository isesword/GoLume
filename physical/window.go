package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"math/big"
)

// WindowFunction identifies one physical analytic calculation. Lazy lowering
// converts logical window expressions into this execution-only representation.
type WindowFunction uint8

const (
	// WindowRowNumber numbers rows from one within each partition.
	WindowRowNumber WindowFunction = iota
	// WindowRank assigns peer-aware ranks with gaps.
	WindowRank
	// WindowDenseRank assigns peer-aware ranks without gaps.
	WindowDenseRank
	// WindowCountRows counts frame rows.
	WindowCountRows
	// WindowCountValues counts non-NULL frame values.
	WindowCountValues
	// WindowSum sums non-NULL frame values.
	WindowSum
	// WindowMin selects the minimum non-NULL frame value.
	WindowMin
	// WindowMax selects the maximum non-NULL frame value.
	WindowMax
	// WindowAvg averages non-NULL frame values.
	WindowAvg
)

// WindowBoundKind identifies one side of an inclusive ROWS frame.
type WindowBoundKind uint8

const (
	// WindowUnboundedPreceding starts at the first partition row.
	WindowUnboundedPreceding WindowBoundKind = iota
	// WindowPreceding selects a constant number of preceding rows.
	WindowPreceding
	// WindowCurrentRow selects the current row.
	WindowCurrentRow
	// WindowFollowing selects a constant number of following rows.
	WindowFollowing
	// WindowUnboundedFollowing ends at the final partition row.
	WindowUnboundedFollowing
)

// WindowBound is one constant ROWS frame boundary.
type WindowBound struct {
	Kind   WindowBoundKind
	Offset int64
}

// WindowFrame is an inclusive partition-local ROWS interval.
type WindowFrame struct {
	Start WindowBound
	End   WindowBound
}

// WindowExpression describes one physical window output.
type WindowExpression struct {
	Function WindowFunction
	Column   string
	Frame    WindowFrame
	Alias    string
}

// WindowSource materializes its child, orders row ordinals by partition and
// sort keys, computes all expressions in one partition scan, then writes each
// result back to its original input row. It intentionally exposes one global
// output partition while preserving input row order.
type WindowSource struct {
	input        source.Source
	partitionBy  []string
	orderBy      []SortKey
	expressions  []WindowExpression
	outputFields []memory.Field
	output       memory.Schema
	manager      memory.Manager
	mode         WindowExecutionMode
}

// WindowExecutionMode selects resident compatibility, ordered bounded state,
// or the external-sort pipeline used by strict bounded execution.
type WindowExecutionMode uint8

const (
	// WindowExecutionResident retains the compatibility full-input algorithm.
	WindowExecutionResident WindowExecutionMode = iota
	// WindowExecutionOrdered consumes already ordered input with bounded state.
	WindowExecutionOrdered
	// WindowExecutionExternalSort sorts, evaluates, and restores input order.
	WindowExecutionExternalSort
)

// WindowSourceOptions controls the physical window execution strategy.
type WindowSourceOptions struct {
	Mode WindowExecutionMode
}

// StreamPlan reports the selected capability tier.
func (s *WindowSource) StreamPlan() source.StreamPlanNode {
	class := source.StreamBlocking
	detail := fmt.Sprintf("strategy=resident outputs=%d", len(s.expressions))
	switch s.mode {
	case WindowExecutionOrdered:
		class = source.StreamBounded
		detail = fmt.Sprintf("strategy=ordered-state outputs=%d", len(s.expressions))
	case WindowExecutionExternalSort:
		class = source.StreamSpillable
		detail = fmt.Sprintf("strategy=external-sort-restore outputs=%d", len(s.expressions))
	}
	return source.StreamPlanNode{
		Operator: "Window",
		Class:    class,
		Detail:   detail,
		Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)},
	}
}

// NewWindowSource validates a full-materialization analytic window source.
// outputFields contain only named window results; input columns remain visible.
func NewWindowSource(input source.Source, partitionBy []string, orderBy []SortKey, expressions []WindowExpression, outputFields []memory.Field, manager memory.Manager) (source.Source, error) {
	return NewWindowSourceWithOptions(input, partitionBy, orderBy, expressions, outputFields, manager, WindowSourceOptions{})
}

// NewWindowSourceWithOptions validates an analytic window source and selects
// its resident, already-ordered, or external-sort implementation.
func NewWindowSourceWithOptions(input source.Source, partitionBy []string, orderBy []SortKey, expressions []WindowExpression, outputFields []memory.Field, manager memory.Manager, options WindowSourceOptions) (source.Source, error) {
	if input == nil {
		return nil, fmt.Errorf("window input is nil")
	}
	if options.Mode > WindowExecutionExternalSort {
		return nil, fmt.Errorf("invalid window execution mode %d", options.Mode)
	}
	if len(orderBy) == 0 {
		return nil, fmt.Errorf("window requires at least one ordering key")
	}
	if len(expressions) == 0 || len(expressions) != len(outputFields) {
		return nil, fmt.Errorf("window expressions and output fields must be non-empty and aligned")
	}
	inputSchema := input.Schema()
	for _, name := range partitionBy {
		if _, ok := inputSchema.Field(name); !ok {
			return nil, fmt.Errorf("window partition column %q not found", name)
		}
	}
	if err := validateSortKeys(inputSchema, orderBy); err != nil {
		return nil, fmt.Errorf("window ordering: %w", err)
	}
	output, err := windowOutputSchema(inputSchema, outputFields)
	if err != nil {
		return nil, err
	}
	if options.Mode != WindowExecutionResident {
		if err := validateBoundedWindowExpressions(expressions); err != nil {
			return nil, err
		}
	}
	return &WindowSource{
		input:        input,
		partitionBy:  append([]string(nil), partitionBy...),
		orderBy:      append([]SortKey(nil), orderBy...),
		expressions:  append([]WindowExpression(nil), expressions...),
		outputFields: append([]memory.Field(nil), outputFields...),
		output:       output,
		manager:      manager,
		mode:         options.Mode,
	}, nil
}

func windowOutputSchema(input memory.Schema, fields []memory.Field) (memory.Schema, error) {
	out := append([]memory.Field(nil), input.Fields...)
	positions := make(map[string]int, len(out)+len(fields))
	for i, field := range out {
		positions[field.Name] = i
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Name == "" {
			return memory.Schema{}, fmt.Errorf("window output field requires a name")
		}
		if _, duplicate := seen[field.Name]; duplicate {
			return memory.Schema{}, fmt.Errorf("window contains duplicate output %q", field.Name)
		}
		seen[field.Name] = struct{}{}
		if pos, replace := positions[field.Name]; replace {
			out[pos] = field
		} else {
			positions[field.Name] = len(out)
			out = append(out, field)
		}
	}
	return memory.NewSchema(out...), nil
}

// Schema returns the input columns plus named analytic results.
func (s *WindowSource) Schema() memory.Schema { return s.output }

// Replayability forwards the child snapshot contract.
func (s *WindowSource) Replayability() source.Replayability { return source.ReplayabilityOf(s.input) }

// Ordering is preserved because every strategy restores original input order.
func (s *WindowSource) Ordering() []source.OrderingColumn {
	provider, ok := s.input.(source.OrderingProvider)
	if !ok {
		return nil
	}
	return append([]source.OrderingColumn(nil), provider.Ordering()...)
}

// Stats retains source row statistics; window outputs add only a small fixed
// vector payload that is reserved at execution time.
func (s *WindowSource) Stats(ctx context.Context) (source.Stats, error) { return s.input.Stats(ctx) }

// Partitions exposes a single global partition because analytic ordering may
// cross every child input partition.
func (s *WindowSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.input.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}

// Dictionary forwards stable source dictionary metadata for retained inputs.
func (s *WindowSource) Dictionary(ctx context.Context, column string) (source.Dictionary, error) {
	provider, ok := s.input.(source.DictionaryProvider)
	if !ok {
		return source.Dictionary{}, fmt.Errorf("window input has no dictionary provider")
	}
	return provider.Dictionary(ctx, column)
}

type windowReader struct {
	owner       *WindowSource
	child       source.BatchStream
	scratch     *memory.Batch
	reservation memory.Reservation
	done        bool
}

// Close releases the input reader and memory reservation.
func (r *windowReader) Close() error {
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
	r.scratch.ReleaseOwned()
	return r.child.Close()
}

func (r *windowReader) reserve(ctx context.Context) error {
	if r.reservation != nil || r.owner.manager == nil {
		return nil
	}
	st, err := r.owner.input.Stats(ctx)
	if err != nil {
		return err
	}
	bytes := estimateOrderBytes(r.owner.input.Schema(), st.Rows)
	resultBytes := estimateOrderBytes(r.owner.output, st.Rows)
	if resultBytes > math.MaxInt64-bytes {
		bytes = math.MaxInt64
	} else {
		bytes += resultBytes
	}
	if st.Bytes > bytes {
		bytes = st.Bytes
	}
	reservation, err := r.owner.manager.Reserve(ctx, bytes)
	if err != nil {
		return err
	}
	r.reservation = reservation
	return nil
}

// Next evaluates every window expression exactly once for the global
// input. Results are gathered in original row order, not ordering-key order.
func (r *windowReader) Next(ctx context.Context, dst *memory.Batch) (bool, error) {
	if r.done {
		dst.ClearColumns()
		return false, nil
	}
	r.done = true
	if err := r.reserve(ctx); err != nil {
		return false, err
	}
	full, err := materializeStream(ctx, r.owner.input.Schema(), r.child, r.scratch)
	if err != nil {
		return false, err
	}
	defer full.ReleaseOwned()
	partitionKeys := make([]SortKey, len(r.owner.partitionBy))
	for i, name := range r.owner.partitionBy {
		partitionKeys[i] = SortKey{Column: name}
	}
	keys := make([]SortKey, 0, len(partitionKeys)+len(r.owner.orderBy))
	keys = append(keys, partitionKeys...)
	keys = append(keys, r.owner.orderBy...)
	rows, err := sortRows(ctx, full.Rows, batchComparator{batch: full, schema: r.owner.input.Schema(), keys: keys}, 1, nil)
	if err != nil {
		return false, err
	}
	identity := make([]int, full.Rows)
	for i := range identity {
		identity[i] = i
	}
	if err := takeRows(ctx, dst, full, identity); err != nil {
		return false, err
	}
	for _, window := range r.owner.expressions {
		clearWindowOutput(dst, window.Alias)
		if len(rows) == 0 {
			if err := evaluateWindowExpression(ctx, full, r.owner.input.Schema(), dst, nil, r.owner.orderBy, window); err != nil {
				return false, err
			}
			continue
		}
		for start := 0; start < len(rows); {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			end := start + 1
			for end < len(rows) && sameRowsByKeys(full, r.owner.input.Schema(), partitionKeys, rows[start], rows[end]) {
				end++
			}
			if err := evaluateWindowExpression(ctx, full, r.owner.input.Schema(), dst, rows[start:end], r.owner.orderBy, window); err != nil {
				return false, err
			}
			start = end
		}
	}
	// A global window emits a schema-bearing empty batch once, so Collect can
	// preserve typed columns for empty inputs without inventing a second path.
	return true, nil
}

func sameRowsByKeys(batch *memory.Batch, schema memory.Schema, keys []SortKey, left, right int) bool {
	for _, key := range keys {
		if compareBatchRows(batch, schema, key, left, right) != 0 {
			return false
		}
	}
	return true
}

func evaluateWindowExpression(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, order []SortKey, window WindowExpression) error {
	switch window.Function {
	case WindowRowNumber, WindowRank, WindowDenseRank:
		values := windowI64Values(output, window.Alias, input.Rows)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			switch window.Function {
			case WindowRowNumber:
				values[row] = int64(i + 1)
			case WindowRank:
				if i == 0 || !sameRowsByKeys(input, schema, order, rows[i-1], row) {
					values[row] = int64(i + 1)
				} else {
					values[row] = values[rows[i-1]]
				}
			case WindowDenseRank:
				if i == 0 {
					values[row] = 1
				} else if sameRowsByKeys(input, schema, order, rows[i-1], row) {
					values[row] = values[rows[i-1]]
				} else {
					values[row] = values[rows[i-1]] + 1
				}
			}
		}
		output.Int64s[window.Alias] = &memory.Int64Vector{Values: values}
		return nil
	case WindowCountRows:
		values := windowI64Values(output, window.Alias, input.Rows)
		for i, row := range rows {
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			if lo <= hi {
				values[row] = int64(hi - lo + 1)
			}
		}
		output.Int64s[window.Alias] = &memory.Int64Vector{Values: values}
		return nil
	case WindowCountValues:
		values := windowI64Values(output, window.Alias, input.Rows)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			for j := lo; j <= hi; j++ {
				valid, err := windowValueValid(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if valid {
					values[row]++
				}
			}
		}
		output.Int64s[window.Alias] = &memory.Int64Vector{Values: values}
		return nil
	case WindowSum:
		field, _ := schema.Field(window.Column)
		return evaluateWindowSum(ctx, input, schema, output, rows, window, field.Type)
	case WindowAvg:
		return evaluateWindowAverage(ctx, input, schema, output, rows, window)
	case WindowMin, WindowMax:
		field, _ := schema.Field(window.Column)
		return evaluateWindowMinMax(ctx, input, schema, output, rows, window, field.Type)
	default:
		return fmt.Errorf("unsupported window function %d", window.Function)
	}
}

func windowI64Values(batch *memory.Batch, name string, rows int) []int64 {
	if existing, ok := batch.Int64s[name]; ok && len(existing.Values) == rows {
		return existing.Values
	}
	return make([]int64, rows)
}

func windowNullableI64(batch *memory.Batch, name string, rows int) ([]int64, memory.Bitmap) {
	if existing, ok := batch.Int64s[name]; ok && len(existing.Values) == rows {
		return existing.Values, preserveWindowValidity(existing.Validity, rows)
	}
	return make([]int64, rows), memory.EnsureBitmap(nil, rows)
}

func windowNullableI32(batch *memory.Batch, name string, rows int) ([]int32, memory.Bitmap) {
	if existing, ok := batch.Int32s[name]; ok && len(existing.Values) == rows {
		return existing.Values, preserveWindowValidity(existing.Validity, rows)
	}
	return make([]int32, rows), memory.EnsureBitmap(nil, rows)
}

func windowNullableU64(batch *memory.Batch, name string, rows int) ([]uint64, memory.Bitmap) {
	if existing, ok := batch.Uint64s[name]; ok && len(existing.Values) == rows {
		return existing.Values, preserveWindowValidity(existing.Validity, rows)
	}
	return make([]uint64, rows), memory.EnsureBitmap(nil, rows)
}

func windowNullableU32(batch *memory.Batch, name string, rows int) ([]uint32, memory.Bitmap) {
	if existing, ok := batch.Uint32s[name]; ok && len(existing.Values) == rows {
		return existing.Values, preserveWindowValidity(existing.Validity, rows)
	}
	return make([]uint32, rows), memory.EnsureBitmap(nil, rows)
}

func windowNullableF64(batch *memory.Batch, name string, rows int) ([]float64, memory.Bitmap) {
	if existing, ok := batch.Float64s[name]; ok && len(existing.Values) == rows {
		return existing.Values, preserveWindowValidity(existing.Validity, rows)
	}
	return make([]float64, rows), memory.EnsureBitmap(nil, rows)
}

func preserveWindowValidity(validity memory.Bitmap, rows int) memory.Bitmap {
	words := memory.WordsFor(rows)
	if len(validity) >= words {
		return validity[:words]
	}
	out := make(memory.Bitmap, words)
	copy(out, validity)
	return out
}

func everyWindowRows(ctx context.Context, i int) error {
	if i&1023 == 0 {
		return ctx.Err()
	}
	return nil
}

func clearWindowOutput(batch *memory.Batch, name string) {
	delete(batch.Int64s, name)
	delete(batch.Int32s, name)
	delete(batch.Uint32s, name)
	delete(batch.Uint64s, name)
	delete(batch.Float64s, name)
	delete(batch.Strings, name)
	delete(batch.DictStrings, name)
	delete(batch.Opaque, name)
}

func windowFrameRange(position, size int, frame WindowFrame) (int, int) {
	return windowFrameStart(position, size, frame.Start), windowFrameEnd(position, size, frame.End)
}

func windowFrameStart(position, size int, bound WindowBound) int {
	switch bound.Kind {
	case WindowUnboundedPreceding:
		return 0
	case WindowPreceding:
		if bound.Offset >= int64(position) {
			return 0
		}
		return position - int(bound.Offset)
	case WindowCurrentRow:
		return position
	case WindowFollowing:
		if bound.Offset > int64(size-position) {
			return size
		}
		return position + int(bound.Offset)
	case WindowUnboundedFollowing:
		return size
	default:
		return size
	}
}

func windowFrameEnd(position, size int, bound WindowBound) int {
	switch bound.Kind {
	case WindowUnboundedPreceding:
		return -1
	case WindowPreceding:
		if bound.Offset > int64(position) {
			return -1
		}
		return position - int(bound.Offset)
	case WindowCurrentRow:
		return position
	case WindowFollowing:
		if bound.Offset >= int64(size-position) {
			return size - 1
		}
		return position + int(bound.Offset)
	case WindowUnboundedFollowing:
		return size - 1
	default:
		return -1
	}
}

func evaluateWindowSum(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, window WindowExpression, typ memory.DataType) error {
	switch {
	case windowSigned(typ):
		values, validity := windowNullableI64(output, window.Alias, input.Rows)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			total := new(big.Int)
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowInt64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok {
					total.Add(total, big.NewInt(value))
					found = true
				}
			}
			if !found {
				continue
			}
			if !total.IsInt64() {
				return fmt.Errorf("window sum %q overflows int64 at row %d", window.Alias, row)
			}
			values[row] = total.Int64()
			memory.Set(validity, row)
		}
		output.Int64s[window.Alias] = &memory.Int64Vector{Values: values, Validity: validity}
		return nil
	case windowUnsigned(typ):
		values, validity := windowNullableU64(output, window.Alias, input.Rows)
		limit := new(big.Int).Lsh(big.NewInt(1), 64)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			total := new(big.Int)
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowUint64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok {
					total.Add(total, new(big.Int).SetUint64(value))
					found = true
				}
			}
			if !found {
				continue
			}
			if total.Cmp(limit) >= 0 {
				return fmt.Errorf("window sum %q overflows uint64 at row %d", window.Alias, row)
			}
			values[row] = total.Uint64()
			memory.Set(validity, row)
		}
		output.Uint64s[window.Alias] = &memory.Uint64Vector{Values: values, Validity: validity}
		return nil
	case windowFloat(typ):
		values, validity := windowNullableF64(output, window.Alias, input.Rows)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			total := 0.0
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowFloat64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok {
					total += value
					found = true
				}
			}
			if found {
				values[row] = total
				memory.Set(validity, row)
			}
		}
		output.Float64s[window.Alias] = &memory.Float64Vector{Values: values, Validity: validity}
		return nil
	default:
		return fmt.Errorf("window sum column %q type %s is not numeric", window.Column, typ)
	}
}

func evaluateWindowAverage(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, window WindowExpression) error {
	values, validity := windowNullableF64(output, window.Alias, input.Rows)
	for i, row := range rows {
		if err := everyWindowRows(ctx, i); err != nil {
			return err
		}
		lo, hi := windowFrameRange(i, len(rows), window.Frame)
		total, count := 0.0, int64(0)
		for j := lo; j <= hi; j++ {
			value, ok, err := windowNumericFloat(input, schema, window.Column, rows[j])
			if err != nil {
				return err
			}
			if ok {
				total += value
				count++
			}
		}
		if count != 0 {
			values[row] = total / float64(count)
			memory.Set(validity, row)
		}
	}
	output.Float64s[window.Alias] = &memory.Float64Vector{Values: values, Validity: validity}
	return nil
}

func evaluateWindowMinMax(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, window WindowExpression, typ memory.DataType) error {
	switch {
	case windowSigned(typ):
		var values []int64
		var validity memory.Bitmap
		var narrow []int32
		if typ == memory.TypeInt64 {
			values, validity = windowNullableI64(output, window.Alias, input.Rows)
		} else {
			narrow, validity = windowNullableI32(output, window.Alias, input.Rows)
			values = make([]int64, input.Rows)
			for i := range narrow {
				values[i] = int64(narrow[i])
			}
		}
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			var best int64
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowInt64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok && (!found || windowPickInt(value, best, window.Function == WindowMin)) {
					best, found = value, true
				}
			}
			if found {
				values[row] = best
				memory.Set(validity, row)
			}
		}
		if typ == memory.TypeInt64 {
			output.Int64s[window.Alias] = &memory.Int64Vector{Values: values, Validity: validity}
			return nil
		}
		for i := range values {
			narrow[i] = int32(values[i])
		}
		output.Int32s[window.Alias] = &memory.Int32Vector{Values: narrow, Validity: validity}
		return nil
	case windowUnsigned(typ):
		var values []uint64
		var validity memory.Bitmap
		var narrow []uint32
		if typ == memory.TypeUint64 {
			values, validity = windowNullableU64(output, window.Alias, input.Rows)
		} else {
			narrow, validity = windowNullableU32(output, window.Alias, input.Rows)
			values = make([]uint64, input.Rows)
			for i := range narrow {
				values[i] = uint64(narrow[i])
			}
		}
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			var best uint64
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowUint64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok && (!found || windowPickUint(value, best, window.Function == WindowMin)) {
					best, found = value, true
				}
			}
			if found {
				values[row] = best
				memory.Set(validity, row)
			}
		}
		if typ == memory.TypeUint64 {
			output.Uint64s[window.Alias] = &memory.Uint64Vector{Values: values, Validity: validity}
			return nil
		}
		for i := range values {
			narrow[i] = uint32(values[i])
		}
		output.Uint32s[window.Alias] = &memory.Uint32Vector{Values: narrow, Validity: validity}
		return nil
	case windowFloat(typ):
		values, validity := windowNullableF64(output, window.Alias, input.Rows)
		for i, row := range rows {
			if err := everyWindowRows(ctx, i); err != nil {
				return err
			}
			lo, hi := windowFrameRange(i, len(rows), window.Frame)
			var best float64
			found := false
			for j := lo; j <= hi; j++ {
				value, ok, err := windowFloat64(input, schema, window.Column, rows[j])
				if err != nil {
					return err
				}
				if ok && (!found || windowPickFloat(value, best, window.Function == WindowMin)) {
					best, found = value, true
				}
			}
			if found {
				values[row] = best
				memory.Set(validity, row)
			}
		}
		output.Float64s[window.Alias] = &memory.Float64Vector{Values: values, Validity: validity}
		return nil
	default:
		return fmt.Errorf("window min/max column %q type %s is not numeric", window.Column, typ)
	}
}

func windowPickInt(value, best int64, min bool) bool {
	if min {
		return value < best
	}
	return value > best
}
func windowPickUint(value, best uint64, min bool) bool {
	if min {
		return value < best
	}
	return value > best
}
func windowPickFloat(value, best float64, min bool) bool {
	valueNaN, bestNaN := math.IsNaN(value), math.IsNaN(best)
	if valueNaN || bestNaN {
		if valueNaN && bestNaN {
			return false
		}
		if min {
			return !valueNaN
		}
		return valueNaN
	}
	if min {
		return value < best
	}
	return value > best
}

func windowSigned(typ memory.DataType) bool {
	switch typ {
	case memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64:
		return true
	}
	return false
}
func windowUnsigned(typ memory.DataType) bool {
	switch typ {
	case memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64:
		return true
	}
	return false
}
func windowFloat(typ memory.DataType) bool {
	return typ == memory.TypeFloat32 || typ == memory.TypeFloat64
}

func windowValueValid(batch *memory.Batch, schema memory.Schema, column string, row int) (bool, error) {
	field, ok := schema.Field(column)
	if !ok {
		return false, fmt.Errorf("window column %q not found", column)
	}
	switch field.Type {
	case memory.TypeInt8:
		v, err := batch.Int8(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeInt16:
		v, err := batch.Int16(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeInt32, memory.TypeDate32, memory.TypeTime32:
		v, err := batch.Int32(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeInt64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		v, err := batch.Int64(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeUint8:
		v, err := batch.Uint8(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeUint16:
		v, err := batch.Uint16(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeUint32:
		v, err := batch.Uint32(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeUint64:
		v, err := batch.Uint64(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeFloat32:
		v, err := batch.Float32(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeFloat64:
		v, err := batch.Float64(column)
		return err == nil && v.IsValid(row), err
	case memory.TypeString, memory.TypeBinary:
		if field.Encoding == memory.EncodingDictionary {
			v, err := batch.DictionaryString(column)
			return err == nil && v.IsValid(row), err
		}
		v, err := batch.String(column)
		return err == nil && v.IsValid(row), err
	default:
		v, err := batch.OpaqueColumn(column)
		if err != nil {
			return false, err
		}
		return v.Validity == nil || memory.IsSet(v.Validity, row), nil
	}
}

func windowInt64(batch *memory.Batch, schema memory.Schema, column string, row int) (int64, bool, error) {
	field, ok := schema.Field(column)
	if !ok {
		return 0, false, fmt.Errorf("window column %q not found", column)
	}
	if field.Type == memory.TypeInt64 {
		v, err := batch.Int64(column)
		if err != nil {
			return 0, false, err
		}
		return v.Values[row], v.IsValid(row), nil
	}
	if field.Type == memory.TypeInt8 {
		v, err := batch.Int8(column)
		if err != nil {
			return 0, false, err
		}
		return int64(v.Values[row]), v.IsValid(row), nil
	}
	if field.Type == memory.TypeInt16 {
		v, err := batch.Int16(column)
		if err != nil {
			return 0, false, err
		}
		return int64(v.Values[row]), v.IsValid(row), nil
	}
	v, err := batch.Int32(column)
	if err != nil {
		return 0, false, err
	}
	return int64(v.Values[row]), v.IsValid(row), nil
}
func windowUint64(batch *memory.Batch, schema memory.Schema, column string, row int) (uint64, bool, error) {
	field, ok := schema.Field(column)
	if !ok {
		return 0, false, fmt.Errorf("window column %q not found", column)
	}
	if field.Type == memory.TypeUint64 {
		v, err := batch.Uint64(column)
		if err != nil {
			return 0, false, err
		}
		return v.Values[row], v.IsValid(row), nil
	}
	if field.Type == memory.TypeUint8 {
		v, err := batch.Uint8(column)
		if err != nil {
			return 0, false, err
		}
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	if field.Type == memory.TypeUint16 {
		v, err := batch.Uint16(column)
		if err != nil {
			return 0, false, err
		}
		return uint64(v.Values[row]), v.IsValid(row), nil
	}
	v, err := batch.Uint32(column)
	if err != nil {
		return 0, false, err
	}
	return uint64(v.Values[row]), v.IsValid(row), nil
}
func windowFloat64(batch *memory.Batch, _ memory.Schema, column string, row int) (float64, bool, error) {
	if v, ok := batch.Float32s[column]; ok {
		return float64(v.Values[row]), v.IsValid(row), nil
	}
	v, err := batch.Float64(column)
	if err != nil {
		return 0, false, err
	}
	return v.Values[row], v.IsValid(row), nil
}
func windowNumericFloat(batch *memory.Batch, schema memory.Schema, column string, row int) (float64, bool, error) {
	field, ok := schema.Field(column)
	if !ok {
		return 0, false, fmt.Errorf("window column %q not found", column)
	}
	if windowSigned(field.Type) {
		v, ok, err := windowInt64(batch, schema, column, row)
		return float64(v), ok, err
	}
	if windowUnsigned(field.Type) {
		v, ok, err := windowUint64(batch, schema, column, row)
		return float64(v), ok, err
	}
	return windowFloat64(batch, schema, column, row)
}
