package expr

import "time"

type dateEvaluationContext struct{ check func() error }

// Deadline leaves timing to the workspace cancellation callback.
func (c dateEvaluationContext) Deadline() (time.Time, bool) { return time.Time{}, false }

// Done is nil; this adapter uses synchronous Err checkpoints.
func (c dateEvaluationContext) Done() <-chan struct{} { return nil }

// Err delegates to the workspace cancellation callback.
func (c dateEvaluationContext) Err() error {
	if c.check != nil {
		return c.check()
	}
	return nil
}

// Value exposes no context values.
func (c dateEvaluationContext) Value(any) any { return nil }
