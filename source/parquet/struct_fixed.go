package parquet

import (
	"context"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
)

// Options selects explicit Parquet schema decoding. Zero retains the flat reader.
type Options struct {
	// NativeNested enables a bounded flat forest: primitive/String/
	// primitive list top-level fields and recursive Structs up to memory.MaxStructDepth.
	// A single-level root List may store local TIME(nanos), but deeper TIME paths
	// and UTC-adjusted or lower-resolution TIME list elements are unsupported.
	NativeNested bool
}

type fixedLeaf struct {
	ancestors                     nestedAncestorChain
	binding                       columnBinding
	parent                        string
	parentOptional, childOptional bool
	maxDef                        int16
}
type fixedPlan struct {
	leaves []fixedLeaf
}

// OpenFileWithOptions discovers a Parquet schema without decoding row values.
// NativeNested validates the complete schema before projection. It does not
// interpret stored Arrow schema metadata as additional native types.
func OpenFileWithOptions(path string, options Options) (*Source, error) {
	if options.NativeNested {
		return openNativeNested(path)
	}
	return OpenFile(path)
}

func (s *Source) fixedPushdown(req source.PushdownRequest) (source.Source, source.PushdownReport, error) {
	need := map[string]bool{}
	for _, name := range req.Projection {
		need[name] = true
	}
	if len(req.Projection) == 0 {
		for _, f := range s.baseSchema.Fields {
			need[f.Name] = true
		}
	}
	if req.Predicate != nil {
		b, e := expr.Bind(s.baseSchema, req.Predicate)
		if e != nil {
			return nil, source.PushdownReport{}, e
		}
		if b.Field.Type != memory.TypeBool {
			return nil, source.PushdownReport{}, fmt.Errorf("pushdown predicate must be Bool")
		}
		for _, name := range b.Columns {
			need[name] = true
		}
	}
	fields := []memory.Field{}
	names := []string{}
	for _, f := range s.baseSchema.Fields {
		if need[f.Name] {
			fields = append(fields, f)
			names = append(names, f.Name)
			delete(need, f.Name)
		}
	}
	if len(need) > 0 {
		return nil, source.PushdownReport{}, fmt.Errorf("unknown native fixed Struct projection column")
	}
	copy := *s
	copy.schema = memory.NewSchema(fields...)
	report := source.PushdownReport{Projection: names, TotalPartitions: len(s.allParts), SelectedPartitions: len(s.parts), TotalRows: s.stats.Rows, SelectedRows: s.stats.Rows, TotalBytes: s.fileBytes, SelectedBytes: s.fileBytes}
	if req.Predicate != nil {
		report.Predicate = req.Predicate.String()
	}
	copy.report = report
	return &copy, report, nil
}

func fixedContext(base, call context.Context) error {
	if err := base.Err(); err != nil {
		return err
	}
	return call.Err()
}
