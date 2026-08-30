package source

import (
	"fmt"
	"strings"
)

// StreamClass describes the retained-state behavior of one physical node.
type StreamClass uint8

const (
	// StreamPipeline processes bounded batches and can produce output before
	// consuming all input.
	StreamPipeline StreamClass = iota
	// StreamBounded retains state with an explicit input-independent bound.
	StreamBounded
	// StreamBlocking currently retains state proportional to input and has no
	// spill implementation.
	StreamBlocking
	// StreamSpillable may retain global state but can release it through the
	// configured spill manager.
	StreamSpillable
	// StreamMemorySink intentionally retains the complete result in memory.
	StreamMemorySink
)

// String returns the stable Explain label for c.
func (c StreamClass) String() string {
	switch c {
	case StreamPipeline:
		return "PIPELINE"
	case StreamBounded:
		return "BOUNDED"
	case StreamBlocking:
		return "BLOCKING"
	case StreamSpillable:
		return "SPILLABLE"
	case StreamMemorySink:
		return "MEMORY_SINK"
	default:
		return fmt.Sprintf("STREAM_CLASS_%d", c)
	}
}

// StreamPlanNode is one physical execution node and its resource class.
type StreamPlanNode struct {
	Operator          string
	Class             StreamClass
	Detail            string
	PartitionParallel bool
	Children          []StreamPlanNode
}

// StreamPlanProvider exposes a physical source's stream/resource topology.
type StreamPlanProvider interface {
	StreamPlan() StreamPlanNode
}

// StreamPlanOf returns a source-provided plan or a conservative scan leaf for
// legacy and external Source implementations.
func StreamPlanOf(src Source) StreamPlanNode {
	if provider, ok := src.(StreamPlanProvider); ok {
		return provider.StreamPlan()
	}
	detail := ""
	if describer, ok := src.(Describer); ok {
		detail = describer.Description()
	}
	partitionParallel := true
	if _, native := src.(BatchStreamProvider); native {
		_, partitionParallel = src.(PreparedBatchStreamProvider)
	}
	return StreamPlanNode{Operator: "Scan", Class: StreamPipeline, Detail: detail, PartitionParallel: partitionParallel}
}

// CanParallelizePartitions reports whether every node can execute independent
// contiguous partition ranges without changing its semantics.
func CanParallelizePartitions(root StreamPlanNode) bool {
	if !root.PartitionParallel {
		return false
	}
	for _, child := range root.Children {
		if !CanParallelizePartitions(child) {
			return false
		}
	}
	return true
}

// FirstBlocking returns the first non-spillable input-proportional node and its
// root-to-node operator path.
func FirstBlocking(root StreamPlanNode) (StreamPlanNode, []string, bool) {
	var visit func(StreamPlanNode, []string) (StreamPlanNode, []string, bool)
	visit = func(node StreamPlanNode, path []string) (StreamPlanNode, []string, bool) {
		path = append(path, node.Operator)
		if node.Class == StreamBlocking {
			return node, path, true
		}
		for _, child := range node.Children {
			if found, foundPath, ok := visit(child, path); ok {
				return found, foundPath, true
			}
		}
		return StreamPlanNode{}, nil, false
	}
	return visit(root, nil)
}

// FormatStreamPlan renders a stable tree for ExplainPhysical.
func FormatStreamPlan(root StreamPlanNode) string {
	var b strings.Builder
	var write func(StreamPlanNode, int)
	write = func(node StreamPlanNode, depth int) {
		b.WriteString(strings.Repeat("  ", depth))
		fmt.Fprintf(&b, "STREAM %s %s", node.Class, node.Operator)
		if node.Detail != "" {
			fmt.Fprintf(&b, " %s", node.Detail)
		}
		b.WriteByte('\n')
		for _, child := range node.Children {
			write(child, depth+1)
		}
	}
	write(root, 0)
	return strings.TrimSuffix(b.String(), "\n")
}
