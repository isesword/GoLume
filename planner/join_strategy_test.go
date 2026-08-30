package planner

import "testing"

func TestChooseJoinUsesOnlyProvenByteBoundsForBroadcast(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   JoinInput
		want JoinKind
	}{
		{"ordered", JoinInput{Ordered: true}, JoinMerge},
		{"bounded-small", JoinInput{Spillable: true, RequireBounded: true, StableReplay: true, MemoryLimitBytes: 1 << 20, BuildBytes: 64 << 10, MaxOutputBatchBytes: 64 << 10}, JoinBroadcastHash},
		{"unknown-bytes", JoinInput{Spillable: true, StableReplay: true, MemoryLimitBytes: 1 << 20}, JoinPartitionedHash},
		{"unstable", JoinInput{Spillable: true, MemoryLimitBytes: 1 << 20, BuildBytes: 64 << 10, MaxOutputBatchBytes: 64 << 10}, JoinPartitionedHash},
		{"large-output", JoinInput{Spillable: true, StableReplay: true, MemoryLimitBytes: 1 << 20, BuildBytes: 64 << 10, MaxOutputBatchBytes: 512 << 10}, JoinPartitionedHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ChooseJoin(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != tc.want {
				t.Fatalf("strategy=%s reason=%s want=%s", got.Kind, got.Reason, tc.want)
			}
		})
	}
}
