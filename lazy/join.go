package lazy

import (
	"fmt"
	"gocolumn/join"
	"gocolumn/logical"
)

// JoinType is the relational join mode used by LazyFrame.Join.
type JoinType = join.Type

const (
	// InnerJoin emits only matching left/right rows.
	InnerJoin JoinType = join.Inner
	// LeftJoin emits all left rows and uses NULL right payloads for misses.
	LeftJoin JoinType = join.Left
	// SemiJoin emits each left row once when at least one right match exists.
	SemiJoin JoinType = join.Semi
	// AntiJoin emits left rows that have no right-side match.
	AntiJoin JoinType = join.Anti
	// RightJoin emits all right rows and NULL left payloads for misses.
	RightJoin JoinType = join.Right
	// FullJoin emits matched rows and unmatched rows from both inputs.
	FullJoin JoinType = join.Full
)

// JoinOptions configures a lazy equality join. LeftOn and RightOn must contain
// one or more corresponding key columns. Ordinary strings and composites with
// more than two keys currently require MemoryOnly execution. Suffix disambiguates duplicate right
// output column names in the logical schema.
type JoinOptions struct {
	Type    JoinType
	LeftOn  []string
	RightOn []string
	Suffix  string
	// RadixBits forces the experimental fixed-key radix join. Zero keeps the
	// default single-table hash join selected by the current planner.
	RadixBits uint8
	// Workers controls parallel radix build/probe when RadixBits is non-zero.
	// Zero uses the session engine worker count.
	Workers int
}

// Join appends an equality-join node without executing either input.
func (f LazyFrame) Join(right LazyFrame, opts JoinOptions) LazyFrame {
	if f.err != nil {
		return f
	}
	if right.err != nil {
		f.err = right.err
		return f
	}
	if f.session == nil || right.session == nil || f.session != right.session {
		f.err = fmt.Errorf("lazy join inputs must belong to the same Session")
		return f
	}
	if len(opts.LeftOn) != len(opts.RightOn) || len(opts.LeftOn) < 1 {
		f.err = fmt.Errorf("lazy join requires one or more corresponding key columns")
		return f
	}
	keys := make([]logical.JoinKey, len(opts.LeftOn))
	for i := range keys {
		keys[i] = logical.JoinKey{Left: opts.LeftOn[i], Right: opts.RightOn[i]}
	}
	if f.hasExecutionPolicy && right.hasExecutionPolicy && f.executionPolicy != right.executionPolicy {
		f.err = fmt.Errorf("lazy join inputs have conflicting execution policies %s and %s", f.executionPolicy, right.executionPolicy)
		return f
	}
	if !f.hasExecutionPolicy && right.hasExecutionPolicy {
		f.executionPolicy = right.executionPolicy
		f.hasExecutionPolicy = true
	}
	f.plan = &logical.Join{Left: f.plan, Right: right.plan, Type: opts.Type, Keys: keys, Suffix: opts.Suffix, RadixBits: opts.RadixBits, Workers: opts.Workers}
	return f
}
