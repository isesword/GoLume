package planner

import "fmt"

// JoinKind identifies the physical equality-join family selected by planning.
type JoinKind uint8

const (
	// JoinBroadcastHash retains a proven-small build side and streams probes.
	JoinBroadcastHash JoinKind = iota
	// JoinPartitionedHash radix-partitions both inputs and may spill runs.
	JoinPartitionedHash
	// JoinMerge consumes two inputs with compatible proven ordering.
	JoinMerge
)

// String returns the stable Explain spelling of a join family.
func (k JoinKind) String() string {
	switch k {
	case JoinBroadcastHash:
		return "broadcast-hash"
	case JoinPartitionedHash:
		return "partitioned-hash"
	case JoinMerge:
		return "merge"
	default:
		return "unknown"
	}
}

// JoinInput contains resource facts used by the join strategy heuristic.
// BuildBytes and MaxOutputBatchBytes must be actual or conservative byte
// measurements; zero means that planning has not proved a bound.
type JoinInput struct {
	Ordered             bool
	Spillable           bool
	RequireBounded      bool
	StableReplay        bool
	MemoryLimitBytes    int64
	BuildBytes          int64
	MaxOutputBatchBytes int64
}

// JoinStrategy is one physical join decision and its diagnostic reason.
type JoinStrategy struct {
	Kind   JoinKind
	Reason string
}

// ChooseJoin selects merge for ordered non-bounded inputs, a proven-small
// broadcast for byte-bounded replayable inputs, and partitioned hash for the
// remaining spill-capable cases.
func ChooseJoin(in JoinInput) (JoinStrategy, error) {
	if in.MemoryLimitBytes < 0 || in.BuildBytes < 0 || in.MaxOutputBatchBytes < 0 {
		return JoinStrategy{}, fmt.Errorf("join planning bytes cannot be negative")
	}
	if in.Ordered && !in.RequireBounded && !in.Spillable {
		return JoinStrategy{Kind: JoinMerge, Reason: "both inputs prove compatible ordering"}, nil
	}
	if in.Spillable {
		quarter := in.MemoryLimitBytes / 4
		if in.StableReplay && quarter > 0 && in.BuildBytes > 0 && in.BuildBytes <= quarter &&
			in.MaxOutputBatchBytes > 0 && in.MaxOutputBatchBytes <= quarter {
			return JoinStrategy{Kind: JoinBroadcastHash, Reason: "actual build and maximum output morsel fit byte threshold"}, nil
		}
		return JoinStrategy{Kind: JoinPartitionedHash, Reason: "unproven or oversized build uses bounded radix spill"}, nil
	}
	return JoinStrategy{Kind: JoinBroadcastHash, Reason: "memory-only resident fast path"}, nil
}
