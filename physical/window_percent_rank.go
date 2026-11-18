package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
	"sync/atomic"
	"unsafe"
)

// validatePercentRankPayload is intentionally narrower than the older window
// APIs. It admits the existing flat native codec profile, not opaque/nested or
// Decimal payloads whose end-to-end profile has not been established here.
func validatePercentRankPayload(schema memory.Schema) error {
	for _, f := range schema.Fields {
		if f.Type == memory.TypeString || f.Type == memory.TypeBinary {
			if f.Encoding != memory.EncodingPlain && f.Encoding != memory.EncodingView && f.Encoding != memory.EncodingDictionary {
				return fmt.Errorf("percent_rank payload %q has unsupported encoding", f.Name)
			}
		} else if f.Encoding != memory.EncodingPlain {
			return fmt.Errorf("percent_rank payload %q requires plain numeric encoding", f.Name)
		}
		if len(f.Children) != 0 {
			return fmt.Errorf("percent_rank payload %q requires a flat native field", f.Name)
		}
		switch f.Type {
		case memory.TypeBool, memory.TypeInt8, memory.TypeInt16, memory.TypeInt32, memory.TypeInt64,
			memory.TypeUint8, memory.TypeUint16, memory.TypeUint32, memory.TypeUint64, memory.TypeFloat32, memory.TypeFloat64,
			memory.TypeDate32, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime32, memory.TypeTime64, memory.TypeDuration,
			memory.TypeString, memory.TypeBinary:
		default:
			return fmt.Errorf("percent_rank payload %q type %s is unsupported", f.Name, f.Type)
		}
	}
	return nil
}

// preparePercentRankWindow is plan-local. The returned lease owns every cloned
// descriptor until the ordered evaluator closes; source streams separately own
// execution state. Unrelated window queries allocate nothing here.
func preparePercentRankWindow(ctx context.Context, owner *WindowSource, input source.Source) (*WindowSource, source.Source, []string, memory.Reservation, error) {
	needRank, needCume := false, false
	tileCount := 0
	for i, e := range owner.expressions {
		if e.Function == WindowNTile {
			if i >= len(owner.buckets) || owner.buckets[i] <= 0 {
				return nil, nil, nil, nil, fmt.Errorf("ntile requires a positive configured bucket count")
			}
			unique := true
			for j := 0; j < i; j++ {
				if owner.expressions[j].Function == WindowNTile && owner.buckets[j] == owner.buckets[i] {
					unique = false
					break
				}
			}
			if unique {
				tileCount++
			}
		}
		needRank = needRank || e.Function == WindowPercentRank
		needCume = needCume || e.Function == WindowCumeDist
	}
	if !needRank && !needCume && tileCount == 0 {
		return owner, input, nil, nil, nil
	}
	hiddenCount := tileCount
	if needRank {
		hiddenCount++
	}
	if needCume {
		hiddenCount++
	}
	schema := input.Schema()
	if err := validatePercentRankPayload(schema); err != nil {
		return nil, nil, nil, nil, err
	}
	// Two independent Schema field/index graphs, a temporary field array,
	// copied expressions/key slices, source/owner descriptors and hidden name.
	// Names are immutable strings, but charge visible retained bytes as well.
	bytes, err := futureBudgetAdd(8192, int64(len(schema.Fields)+hiddenCount), 6*int64(unsafe.Sizeof(memory.Field{}))+1024)
	if err == nil {
		bytes, err = futureBudgetAdd(bytes, int64(tileCount), int64(unsafe.Sizeof(ntileSpoolPlan{}))+256)
	}
	// Execution-local positional bucket copy is retained alongside deduplicated plans.
	if err == nil && tileCount > 0 {
		bytes, err = futureBudgetAdd(bytes, int64(len(owner.buckets)), 10)
		if err == nil {
			bytes, err = sortWorkspaceSize(bytes, 64)
		}
	}
	if err == nil {
		bytes, err = futureBudgetAdd(bytes, int64(len(owner.expressions)), 2*int64(unsafe.Sizeof(WindowExpression{}))+256)
	}
	if err == nil {
		bytes, err = futureBudgetAdd(bytes, int64(len(owner.partitionBy)+len(owner.orderBy)), 256)
	}
	for _, f := range schema.Fields {
		if err == nil {
			bytes, err = futureBudgetAdd(bytes, int64(len(f.Name)), 4)
		}
		if err == nil {
			bytes, err = futureBudgetAdd(bytes, int64(len(f.ArrowType)), 4)
		}
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}
	manager := owner.manager
	if q := memory.QueryBudgetFromContext(ctx); q != nil {
		manager = q.Operator("PercentRankPreparation", memory.MemoryRetainedState)
	}
	if manager == nil {
		return nil, nil, nil, nil, fmt.Errorf("percent_rank requires memory accounting")
	}
	lease, err := manager.Reserve(ctx, bytes)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	hiddenNames := make([]string, 0, hiddenCount)
	chooseName := func(base, other string) string {
		name := base
		for n := 0; ; n++ {
			_, exists := schema.Field(name)
			for _, e := range owner.expressions {
				exists = exists || e.Alias == name
			}
			for _, used := range hiddenNames {
				exists = exists || used == name
			}
			if !exists && name != other {
				return name
			}
			name = fmt.Sprintf("%s_%d", base, n+1)
		}
	}
	hidden, cumeHidden := "", ""
	if needRank {
		hidden = chooseName("__golume_percent_rank", "")
	}
	if needCume {
		cumeHidden = chooseName("__golume_cume_dist", hidden)
	}
	fields := append([]memory.Field(nil), schema.Fields...)
	if needRank {
		fields = append(fields, memory.Field{Name: hidden, Type: memory.TypeFloat64})
		hiddenNames = append(hiddenNames, hidden)
	}
	if needCume {
		fields = append(fields, memory.Field{Name: cumeHidden, Type: memory.TypeFloat64})
		hiddenNames = append(hiddenNames, cumeHidden)
	}
	plans := make([]ntileSpoolPlan, 0, tileCount)
	for i, e := range owner.expressions {
		if e.Function != WindowNTile {
			continue
		}
		b := owner.buckets[i]
		found := false
		for _, p := range plans {
			if p.buckets == b {
				found = true
				break
			}
		}
		if found {
			continue
		}
		name := chooseName("__golume_ntile", "")
		plans = append(plans, ntileSpoolPlan{buckets: b, hidden: name})
		hiddenNames = append(hiddenNames, name)
		fields = append(fields, memory.Field{Name: name, Type: memory.TypeInt64})
	}
	wrapped := &percentRankSource{metrics: new(percentRankCounters), input: input, schema: memory.NewSchema(fields...), original: memory.NewSchema(schema.Fields...), hidden: hidden, cumeHidden: cumeHidden, manager: manager, ntilePlans: plans}
	wrapped.partition = append([]string(nil), owner.partitionBy...)
	wrapped.order = append([]SortKey(nil), owner.orderBy...)
	copyOwner := *owner
	copyOwner.input = wrapped
	copyOwner.expressions = append([]WindowExpression(nil), owner.expressions...)
	if tileCount > 0 {
		copyOwner.buckets = append([]int64(nil), owner.buckets...)
	}
	for i, e := range copyOwner.expressions {
		if e.Function == WindowPercentRank || e.Function == WindowCumeDist || e.Function == WindowNTile {
			e.Column = hidden
			if e.Function == WindowCumeDist {
				e.Column = cumeHidden
			}
			if e.Function == WindowNTile {
				copyOwner.buckets[i] = 0
				for _, p := range plans {
					if p.buckets == owner.buckets[i] {
						e.Column = p.hidden
						break
					}
				}
			}
			e.Function = WindowShift
			e.Frame = WindowFrame{Start: WindowBound{Kind: WindowCurrentRow}, End: WindowBound{Kind: WindowCurrentRow}}
			copyOwner.expressions[i] = e
		}
	}
	return &copyOwner, wrapped, hiddenNames, lease, nil
}

type percentRankCounters struct {
	pages, bytes, partitions atomic.Int64
	peekPages, replayPages   atomic.Int64
}

type ntileSpoolPlan struct {
	buckets int64
	hidden  string
}

type percentRankSource struct {
	ntilePlans       []ntileSpoolPlan
	metrics          *percentRankCounters
	input            source.Source
	original, schema memory.Schema
	partition        []string
	order            []SortKey
	hidden           string
	cumeHidden       string
	manager          memory.Manager
}

// Schema returns the prepared original fields and private ratio field.
func (s *percentRankSource) Schema() memory.Schema { return s.schema }

// Stats forwards cardinality estimates, which execution never trusts as partition totals.
func (s *percentRankSource) Stats(ctx context.Context) (source.Stats, error) {
	return s.input.Stats(ctx)
}

// Partitions forwards physical source partition information.
func (s *percentRankSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	return s.input.Partitions(ctx)
}

// StreamPlan identifies the blocking per-partition spool and its live-page metadata.
func (s *percentRankSource) StreamPlan() source.StreamPlanNode {
	operator := "PercentRank"
	if s.cumeHidden != "" {
		operator = "CumeDist"
		if s.hidden != "" {
			operator = "PercentRank+CumeDist"
		}
	}
	if len(s.ntilePlans) > 0 {
		if s.hidden == "" && s.cumeHidden == "" {
			operator = "NTile"
		} else {
			operator += "+NTile"
		}
	}
	return source.StreamPlanNode{Operator: operator, Class: source.StreamSpillable, Detail: "partition-spool-replay; metadata grows with live pages", Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)}}
}

// NewBatchStream opens one source-consumption and partition-replay lifecycle.
func (s *percentRankSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	return newPercentRankPartitionStream(ctx, s, opts)
}

// PercentRankDiagnostics exposes only this wrapper's cumulative physical writes,
// including through a future-window child which otherwise hides stream metrics.
func (s *percentRankSource) PercentRankDiagnostics() map[string]string {
	if s.metrics == nil {
		return map[string]string{}
	}
	out := map[string]string{"strategy": "partition-spool-replay", "partition_spool_pages": fmt.Sprint(s.metrics.pages.Load()), "partition_spool_bytes": fmt.Sprint(s.metrics.bytes.Load()), "partition_spool_partitions": fmt.Sprint(s.metrics.partitions.Load())}
	if s.cumeHidden != "" {
		out["peer_lookahead_pages"] = fmt.Sprint(s.metrics.peekPages.Load())
	}
	if s.cumeHidden != "" || len(s.ntilePlans) > 0 {
		out["partition_replay_pages"] = fmt.Sprint(s.metrics.replayPages.Load())
	}
	return out
}
