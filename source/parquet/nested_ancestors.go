package parquet

import (
	"context"
	"fmt"
	"gocolumn/memory"
)

// A bounded value path identifies occurrences, never dot-concatenated names.
type nestedAncestorChain struct {
	ids        [memory.MaxStructDepth]int
	thresholds [memory.MaxStructDepth]int16
	count      int
}

func (c nestedAncestorChain) optionalCount() int16 {
	if c.count == 0 {
		return 0
	}
	return c.thresholds[c.count-1]
}
func (c nestedAncestorChain) parentID() int {
	if c.count == 0 {
		return -1
	}
	return c.ids[c.count-1]
}

type nestedStructNode struct {
	name, root    string
	parent, depth int
	threshold     int16
	children      int
}

func (s *Source) recursiveForestMetadata(ctx context.Context) (int64, error) {
	var total int64
	for _, n := range s.structNodes {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if n.depth <= 1 {
			continue
		}
		if _, ok := s.schema.Field(n.root); !ok {
			continue
		}
		if n.children < 0 || int64(n.children) > (int64(^uint64(0)>>1)-4096)/4096 {
			return 0, fmt.Errorf("recursive Struct metadata overflow")
		}
		bytes := int64(4096) + int64(n.children)*4096
		if total > int64(^uint64(0)>>1)-bytes {
			return 0, fmt.Errorf("recursive Struct metadata overflow")
		}
		total += bytes
	}
	return total, nil
}

func remapNestedAncestors(chain nestedAncestorChain, ids map[int]int) nestedAncestorChain {
	for i := 0; i < chain.count; i++ {
		chain.ids[i] = ids[chain.ids[i]]
	}
	return chain
}

func (s *nestedListStream) observeAncestors(chain nestedAncestorChain, witness [memory.MaxStructDepth]bool) error {
	for i := 0; i < chain.count; i++ {
		if err := s.observeParent(chain.ids[i], witness[i]); err != nil {
			return err
		}
	}
	return nil
}
