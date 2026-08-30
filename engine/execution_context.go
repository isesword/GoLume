package engine

import "context"

type executionPolicyContextKey struct{}

// WithExecutionPolicy binds one immutable LazyFrame override to physical
// lowering and preparation without mutating the shared Engine defaults.
func WithExecutionPolicy(ctx context.Context, policy ExecutionPolicy) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionPolicyContextKey{}, policy)
}

// ExecutionPolicyFromContext returns a query-local policy or fallback when no
// override is present. Invalid values are left to Validate by the caller.
func ExecutionPolicyFromContext(ctx context.Context, fallback ExecutionPolicy) ExecutionPolicy {
	if ctx == nil {
		return fallback
	}
	policy, ok := ctx.Value(executionPolicyContextKey{}).(ExecutionPolicy)
	if !ok {
		return fallback
	}
	return policy
}
