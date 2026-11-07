package engine

import (
	"encoding/binary"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
)

// CompiledDistinct binds repeated distinct outputs to one exact input lane.
// Values use the same equivalence as grouping: null is a value, all NaNs are
// equal, and signed zeros compare equal. The output count is UInt64.
type CompiledDistinct struct {
	Fields     []memory.Field
	OutputLane []int
}

func compileDistinct(schema memory.Schema, aggs []expr.Aggregate) (*CompiledDistinct, error) {
	var d *CompiledDistinct
	for i, a := range aggs {
		if a.Kind != expr.AggNUnique {
			continue
		}
		f, ok := schema.Field(a.Column)
		if !ok {
			return nil, fmt.Errorf("distinct input %q not found", a.Column)
		}
		if _, ok := canonicalGroupWidth(f.Type); !ok {
			return nil, fmt.Errorf("distinct input %q has unsupported type %s", a.Column, f.Type)
		}
		if d == nil {
			d = &CompiledDistinct{OutputLane: make([]int, len(aggs))}
			for j := range d.OutputLane {
				d.OutputLane[j] = -1
			}
		}
		lane := -1
		for j, existing := range d.Fields {
			if existing.Name == f.Name {
				lane = j
				break
			}
		}
		if lane < 0 {
			lane = len(d.Fields)
			d.Fields = append(d.Fields, f)
		}
		d.OutputLane[i] = lane
	}
	return d, nil
}

func (p *PreparedQuery) distinct() *CompiledDistinct {
	if p.cq.MultiAggregate == nil {
		return nil
	}
	return p.cq.MultiAggregate.Distinct
}

// Distinct streams use the existing rawStates transport slot for count bytes;
// ordinary values use live numeric/statistics bundles. This keeps the legacy
// hot aggregateRecord layout unchanged.
func distinctRecordCount(r aggregateRecord, lane int) uint64 {
	return binary.LittleEndian.Uint64(r.rawStates[lane*8 : lane*8+8])
}
