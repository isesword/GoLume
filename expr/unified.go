package expr

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// Compile binds and compiles one expression, regardless of whether its consumer
// projects the value or uses a BOOL value to select rows.
func Compile(schema memory.Schema, e Expr) (Program, error) {
	bound, err := Bind(schema, e)
	if err != nil {
		return Program{}, err
	}
	compiler := newScalarCompiler(schema)
	root, info, err := compiler.compileBound(bound)
	if err != nil {
		return Program{}, err
	}
	program := Program{instructions: compiler.instructions, root: root, typ: info.Type, nullable: info.Nullable, complexity: len(compiler.instructions), result: bound.OutputField("")}
	program.planStorage()
	program.planSelection()
	return program, nil
}

// Workspace is an execution-local, query-owned instance of one immutable
// Program. It shares the same typed executor used by all convenience APIs.
// Results are read-only borrowed views until the next evaluation or Close.
// Admission failure is retryable; cancellation and semantic errors close it.
type Workspace struct {
	program  Program
	storage  evaluationState
	query    *memory.QueryBudget
	manager  memory.Manager
	closed   bool
	selected selectedWorkspace
}

// NewWorkspace binds the program to one memory owner. A QueryBudget in ctx
// takes precedence over manager and remains bound when later contexts omit it.
func NewWorkspace(ctx context.Context, program Program, manager memory.Manager, operatorNames ...string) (*Workspace, error) {
	if ctx == nil {
		return nil, fmt.Errorf("expression workspace context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(program.outputs) == 0 {
		if program.root < 0 || program.root >= len(program.instructions) {
			return nil, fmt.Errorf("invalid expression root")
		}
	} else {
		for _, out := range program.outputs {
			if out.root < 0 || out.root >= len(program.instructions) {
				return nil, fmt.Errorf("invalid expression output root")
			}
		}
	}
	name := "Expression"
	if len(operatorNames) > 0 && operatorNames[0] != "" {
		name = operatorNames[0]
	}
	query := memory.QueryBudgetFromContext(ctx)
	if query != nil {
		manager = query.Operator(name, memory.MemoryWorkspace)
	}
	if manager == nil {
		manager = memory.NewBudgetManager(0)
	}
	return &Workspace{program: program, query: query, manager: manager}, nil
}
func (w *Workspace) execute(ctx context.Context, batch *memory.Batch, kernels kernel.Set) error {
	if w == nil || w.closed {
		return fmt.Errorf("expression workspace is closed")
	}
	if ctx == nil {
		return fmt.Errorf("expression evaluation context is nil")
	}
	if query := memory.QueryBudgetFromContext(ctx); query != nil && query != w.query {
		return fmt.Errorf("expression workspace cannot change query ownership")
	} else if query == nil && w.query != nil {
		ctx = memory.WithQueryBudget(ctx, w.query)
	}
	w.storage.ctx = ctx
	w.storage.manager = w.manager
	err := evaluateInstructions(w.program.instructions, batch, &w.storage, kernels)
	if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
		_ = w.Close()
	}
	return err
}

// Evaluate returns the named outputs, or one root value for a scalar program.
func (w *Workspace) Evaluate(ctx context.Context, batch *memory.Batch, kernels kernel.Set) (values []ScalarValue, err error) {
	if err := w.execute(ctx, batch, kernels); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
			_ = w.Close()
		}
	}()
	count := len(w.program.outputs)
	if count == 0 {
		count = 1
	}
	if err := w.storage.ensureOutputs(count); err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		root := w.program.root
		if len(w.program.outputs) > 0 {
			root = w.program.outputs[i].root
		}
		value, err := w.storage.outputValue(root, batch.Rows)
		if err != nil {
			return nil, err
		}
		w.storage.outputs[i] = value
	}
	return w.storage.outputs, nil
}

// EvaluateBool returns a BOOL root without expanding its compact truth bitmap.
// It does not apply WHERE semantics or mutate the returned value.
func (w *Workspace) EvaluateBool(ctx context.Context, batch *memory.Batch, kernels kernel.Set) (result BoolVector, err error) {
	if err := w.execute(ctx, batch, kernels); err != nil {
		return BoolVector{}, err
	}
	defer func() {
		if err != nil && !errors.Is(err, memory.ErrBudgetExceeded) {
			_ = w.Close()
		}
	}()
	root := w.program.root
	if root < 0 || root >= len(w.storage.regs) {
		return BoolVector{}, fmt.Errorf("program has no scalar root")
	}
	value := w.storage.regs[root]
	if value.typ != memory.TypeBool {
		return BoolVector{}, fmt.Errorf("expression root must be bool, got %s", value.typ)
	}
	return BoolVector{Truth: value.Truth, Validity: value.validity}, nil
}

// MemoryBytes reports retained buffer capacities and admitted immutable literal
// IN set payloads, excluding input payloads and other compiled program metadata.
func (w *Workspace) MemoryBytes() int64 {
	if w == nil {
		return 0
	}
	return w.storage.MemoryBytes() + w.selected.bytes + w.selected.metadataBytes
}

// Close idempotently releases all owned buffers and execution references.
func (w *Workspace) Close() error {
	if w == nil {
		return nil
	}
	_ = w.storage.Close()
	w.selected.close()
	w.storage = evaluationState{}
	w.program = Program{}
	w.query = nil
	w.manager = nil
	w.closed = true
	return nil
}
