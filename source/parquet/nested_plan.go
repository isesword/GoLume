package parquet

import "gocolumn/memory"

type nestedListPlan struct {
	array          *nestedArrayPlan
	group          *nestedStructListPlan
	ancestors      nestedAncestorChain
	parent         string
	parentOptional bool
	scalar         bool
	plan           int64ListPlan
	kind           memory.DataType
}

func openNativeNested(path string) (*Source, error) {
	s, err := openMetadata(path)
	if err != nil {
		return nil, err
	}
	return planNativeForest(s)
}
