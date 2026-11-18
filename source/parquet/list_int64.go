package parquet

type int64ListPlan struct {
	name                      string
	index                     int
	optional, elementOptional bool
	maxDef                    int16
}
