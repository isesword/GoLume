package expr

import (
	"fmt"
	"gocolumn/kernel"
	"gocolumn/memory"
)

// These helpers exercise the private typed executor directly for kernel and
// allocation regressions. Public API/ownership tests use NewWorkspace.
// Evaluate executes one scalar program over batch and returns a typed register
// owned by workspace. The register remains valid until the next evaluation with
// the same workspace.
func evaluateValueTest(p Program, batch *memory.Batch, w *evaluationState, kernels kernel.Set) (ScalarValue, error) {
	if p.root < 0 || p.root >= len(p.instructions) {
		_ = w.Close()
		return ScalarValue{}, fmt.Errorf("invalid scalar program root")
	}
	if err := evaluateInstructions(p.instructions, batch, w, kernels); err != nil {
		_ = w.Close()
		return ScalarValue{}, err
	}
	return w.outputValue(p.root, batch.Rows)
}

// EvaluateInt64 executes p and returns an int64 physical vector. Semantic
// date/timestamp results must use Evaluate so their logical type is preserved.
func evaluateInt64Test(p Program, batch *memory.Batch, w *evaluationState, kernels kernel.Set) (*memory.Int64Vector, error) {
	v, err := evaluateValueTest(p, batch, w, kernels)
	if err != nil {
		return nil, err
	}
	if v.Type != memory.TypeInt64 {
		_ = w.Close()
		return nil, fmt.Errorf("scalar program result type %s is not int64", v.Type)
	}
	return v.Int64, nil
}

// Evaluate executes a fused projection program and returns values in output
// order. Returned vector views alias workspace or input memory.
func evaluateProjectionTest(p Program, batch *memory.Batch, w *evaluationState, kernels kernel.Set) ([]ScalarValue, error) {
	if err := evaluateInstructions(p.instructions, batch, w, kernels); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.ensureOutputs(len(p.outputs)); err != nil {
		_ = w.Close()
		return nil, err
	}
	out := w.outputs
	for i, o := range p.outputs {
		if o.root < 0 || o.root >= len(w.regs) {
			_ = w.Close()
			return nil, fmt.Errorf("projection output %q has invalid root", o.Name)
		}
		value, err := w.outputValue(o.root, batch.Rows)
		if err != nil {
			_ = w.Close()
			return nil, err
		}
		out[i] = value
	}
	return out, nil
}

// Evaluate is the BOOL result adapter over the shared typed evaluator. Returned
// bitmaps are immutable borrowed views until evaluation or workspace release.
func evaluateBoolTest(p Program, batch *memory.Batch, w *evaluationState, kernels kernel.Set) (BoolVector, error) {
	if p.root < 0 || p.root >= len(p.instructions) {
		return BoolVector{}, fmt.Errorf("invalid expression program root")
	}
	if err := evaluateInstructions(p.instructions, batch, w, kernels); err != nil {
		return BoolVector{}, err
	}
	r := w.regs[p.root]
	if r.typ != memory.TypeBool {
		return BoolVector{}, fmt.Errorf("predicate result is %s, want bool", r.typ)
	}
	return BoolVector{Truth: r.Truth, Validity: r.validity}, nil
}
