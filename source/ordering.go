package source

// OrderingColumn describes one proven physical ordering prefix. Grouped
// aggregation only depends on key adjacency, so null placement is diagnostic
// while direction may be ascending or descending.
type OrderingColumn struct {
	Name       string
	Descending bool
	NullsLast  bool
}

// OrderingProvider exposes ordering guaranteed across every batch and source
// partition, not merely ordering observed in one sample.
type OrderingProvider interface {
	Ordering() []OrderingColumn
}

// OrderedByPrefix reports whether source guarantees the requested direct
// columns as its leading ordering keys.
func OrderedByPrefix(src Source, columns []string) bool {
	provider, ok := src.(OrderingProvider)
	if !ok || len(columns) == 0 {
		return false
	}
	ordering := provider.Ordering()
	if len(ordering) < len(columns) {
		return false
	}
	for index, column := range columns {
		if ordering[index].Name != column {
			return false
		}
	}
	return true
}
