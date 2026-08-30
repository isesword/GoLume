package lazy

import (
	"fmt"
	"gocolumn/engine"
	"gocolumn/logical"
	"gocolumn/source"
)

// ExecutionPolicy is the public lazy spelling of engine.ExecutionPolicy.
type ExecutionPolicy = engine.ExecutionPolicy

const (
	// ExecutionAuto selects batch-local, resident, or spillable strategies per node.
	ExecutionAuto = engine.ExecutionAuto
	// ExecutionMemoryOnly disables spill for small workloads and diagnostics.
	ExecutionMemoryOnly = engine.ExecutionMemoryOnly
	// ExecutionBounded requires a finite budget and bounded or spillable state.
	ExecutionBounded = engine.ExecutionBounded
)

// ExecutionOptions overrides session execution behavior for one immutable
// LazyFrame and every logical node subsequently derived from it.
type ExecutionOptions struct {
	Policy ExecutionPolicy
}

// ErrBoundedMemoryLimitRequired reports a bounded query configured without a
// finite memory limit.
var ErrBoundedMemoryLimitRequired = engine.ErrBoundedMemoryLimitRequired

// ErrBoundedSpillRequired reports a bounded stateful plan without a finite
// spill quota.
var ErrBoundedSpillRequired = engine.ErrBoundedSpillRequired

// UnsupportedBoundedPlanError reports a physical node whose retained state is
// proportional to input and has no spill implementation.
type UnsupportedBoundedPlanError = engine.UnsupportedBoundedPlanError

// WithExecution returns a frame with a query-local execution policy override.
// The logical plan and Session remain shared and immutable.
func (f LazyFrame) WithExecution(opts ExecutionOptions) LazyFrame {
	if f.err != nil {
		return f
	}
	if err := opts.Policy.Validate(); err != nil {
		f.err = err
		return f
	}
	f.executionPolicy = opts.Policy
	f.hasExecutionPolicy = true
	return f
}

func (f LazyFrame) effectiveExecutionPolicy() (engine.ExecutionPolicy, error) {
	policy := engine.ExecutionAuto
	if f.hasExecutionPolicy {
		policy = f.executionPolicy
	} else if f.session != nil && f.session.engine != nil {
		policy = f.session.engine.ExecutionPolicy()
	}
	if err := policy.Validate(); err != nil {
		return engine.ExecutionAuto, err
	}
	return policy, nil
}

func (f LazyFrame) validateExecutionLogical(plan logical.Plan) error {
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return err
	}
	if policy != engine.ExecutionBounded {
		return nil
	}
	if f.session == nil || f.session.engine == nil || f.session.engine.Memory().Limit() <= 0 {
		return ErrBoundedMemoryLimitRequired
	}
	spillConfigured := f.session.engine.SpillConfigured()
	operator, path, ok := firstLogicalBlocking(plan, nil, spillConfigured)
	if !ok {
		return nil
	}
	detail := ""
	if (operator == "Aggregate" || operator == "Join" || operator == "Sort" || operator == "Window") && !spillConfigured {
		detail = ErrBoundedSpillRequired.Error() + "; configure engine.Options.Spill.MaxBytes"
	}
	var cause error
	if (operator == "Aggregate" || operator == "Join" || operator == "Sort" || operator == "Window") && !spillConfigured {
		cause = ErrBoundedSpillRequired
	}
	return &UnsupportedBoundedPlanError{Operator: operator, Path: path, Detail: detail, Cause: cause}
}

func firstLogicalBlocking(plan logical.Plan, path []string, aggregateSpillable bool) (string, []string, bool) {
	if plan == nil {
		return "", nil, false
	}
	name := "Scan"
	var children []logical.Plan
	blocking := false
	switch node := plan.(type) {
	case *logical.Scan:
		name = "Scan"
	case *logical.Filter:
		name, children = "Filter", []logical.Plan{node.Input}
	case *logical.Projection:
		name, children = "Projection", []logical.Plan{node.Input}
	case *logical.ExprProjection:
		name, children = "Projection", []logical.Plan{node.Input}
	case *logical.Limit:
		name, children = "Limit", []logical.Plan{node.Input}
	case *logical.TopK:
		name, children = "TopK", []logical.Plan{node.Input}
	case *logical.Sort:
		name, children, blocking = "Sort", []logical.Plan{node.Input}, !aggregateSpillable
	case *logical.Window:
		name, children, blocking = "Window", []logical.Plan{node.Input}, !logicalWindowBoundedCapable(node.Exprs)
	case *logical.Aggregate:
		// Aggregate capability depends on proven physical ordering or configured
		// spill and is decided by engine.Prepare after lowering. Still inspect its
		// child here so unsupported Sort/Join/Window nodes fail early.
		name, children = "Aggregate", []logical.Plan{node.Input}
	case *logical.Join:
		// Join strategy depends on ordering proof and configured spill after both
		// inputs are lowered. Without a spill quota strict Bounded execution
		// rejects it before any side is read; sorted merge remains an Auto/
		// MemoryOnly optimization because one duplicate group is unbounded.
		name, children, blocking = "Join", []logical.Plan{node.Left, node.Right}, !aggregateSpillable
	default:
		name, blocking = fmt.Sprintf("%T", plan), true
	}
	path = append(path, name)
	if blocking {
		return name, path, true
	}
	for _, child := range children {
		if operator, childPath, ok := firstLogicalBlocking(child, path, aggregateSpillable); ok {
			return operator, childPath, true
		}
	}
	return "", nil, false
}

func streamPlanForLogical(plan logical.Plan, aggregateClass source.StreamClass) source.StreamPlanNode {
	if plan == nil {
		return source.StreamPlanNode{Operator: "Invalid", Class: source.StreamBlocking}
	}
	node := source.StreamPlanNode{Class: source.StreamPipeline}
	var children []logical.Plan
	switch value := plan.(type) {
	case *logical.Scan:
		node.Operator = "Scan"
	case *logical.Filter:
		node.Operator, children = "Filter", []logical.Plan{value.Input}
	case *logical.Projection:
		node.Operator, children = "Project", []logical.Plan{value.Input}
	case *logical.ExprProjection:
		node.Operator, children = "Project", []logical.Plan{value.Input}
	case *logical.Limit:
		node.Operator, children = "Limit", []logical.Plan{value.Input}
	case *logical.TopK:
		node.Operator, node.Class, children = "TopK", source.StreamBounded, []logical.Plan{value.Input}
	case *logical.Sort:
		node.Operator, node.Class, children = "Sort", source.StreamBlocking, []logical.Plan{value.Input}
	case *logical.Window:
		node.Operator, children = "Window", []logical.Plan{value.Input}
		if logicalWindowBoundedCapable(value.Exprs) {
			node.Class = source.StreamSpillable
		} else {
			node.Class = source.StreamBlocking
		}
	case *logical.Aggregate:
		node.Operator, children = "Aggregate", []logical.Plan{value.Input}
		node.Class = aggregateClass
	case *logical.Join:
		node.Operator, node.Class, children = "Join", source.StreamBlocking, []logical.Plan{value.Left, value.Right}
	default:
		node.Operator, node.Class = fmt.Sprintf("%T", plan), source.StreamBlocking
	}
	for _, child := range children {
		node.Children = append(node.Children, streamPlanForLogical(child, aggregateClass))
	}
	return node
}

func (f LazyFrame) validateExecutionSource(src source.Source) error {
	policy, err := f.effectiveExecutionPolicy()
	if err != nil || policy != engine.ExecutionBounded {
		return err
	}
	node, path, ok := source.FirstBlocking(source.StreamPlanOf(src))
	if !ok {
		return nil
	}
	detail := node.Detail
	var cause error
	if node.Operator == "Window" {
		detail += "; unordered bounded windows require engine.Options.Spill.MaxBytes"
		cause = ErrBoundedSpillRequired
	}
	return &UnsupportedBoundedPlanError{Operator: node.Operator, Path: path, Detail: detail, Cause: cause}
}

func logicalWindowBoundedCapable(expressions []logical.WindowExpr) bool {
	for _, expression := range expressions {
		switch expression.Function {
		case logical.WindowRowNumber, logical.WindowRank, logical.WindowDenseRank:
			continue
		}
		if expression.Frame.End.Kind != logical.RowCurrent {
			return false
		}
		switch expression.Frame.Start.Kind {
		case logical.RowUnboundedPreceding, logical.RowPreceding, logical.RowCurrent:
		default:
			return false
		}
	}
	return true
}

func (f LazyFrame) executionSummary() (string, error) {
	policy, err := f.effectiveExecutionPolicy()
	if err != nil {
		return "", err
	}
	limit := int64(0)
	spillLimit := int64(0)
	workers := 1
	if f.session != nil && f.session.engine != nil {
		limit = f.session.engine.Memory().Limit()
		spillLimit = f.session.engine.SpillLimit()
		workers = f.session.engine.MaxWorkers()
	}
	memoryLimit := "unbounded"
	if limit > 0 {
		memoryLimit = fmt.Sprintf("%d", limit)
	}
	spill := "disabled"
	if spillLimit > 0 {
		spill = fmt.Sprintf("%d", spillLimit)
	}
	pipelineSummary := ""
	if f.session != nil && f.session.engine != nil {
		opts := f.session.engine.PipelineOptions()
		pipelineSummary = fmt.Sprintf(" pipeline_workers=%d target_morsel_bytes=%d max_in_flight_bytes=%d ordering=%s", opts.Workers, opts.TargetMorselBytes, opts.MaxInFlightBytes, opts.Ordering)
	}
	return fmt.Sprintf("EXECUTION policy=%s memory_limit=%s spill_limit=%s workers=%d%s", policy, memoryLimit, spill, workers, pipelineSummary), nil
}
