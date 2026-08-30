package source

import "fmt"

// Replayability describes whether independent readers observe the same stable
// source snapshot for the lifetime of a prepared query.
type Replayability uint8

const (
	// ReplayUnknown is conservative and requires a bounded multiplexer for fan-out.
	ReplayUnknown Replayability = iota
	// ReplayStable permits fan-out branches to rescan instead of caching input.
	ReplayStable
)

// String returns the stable Explain spelling for replayability.
func (r Replayability) String() string {
	switch r {
	case ReplayUnknown:
		return "unknown"
	case ReplayStable:
		return "stable"
	default:
		return fmt.Sprintf("replayability-%d", r)
	}
}

// ReplayabilityProvider declares stable prepared-query replay semantics.
type ReplayabilityProvider interface {
	Replayability() Replayability
}

// ReplayabilityOf returns a source declaration or the conservative unknown
// default. Independent Reader creation alone does not prove snapshot identity.
func ReplayabilityOf(src Source) Replayability {
	if provider, ok := src.(ReplayabilityProvider); ok {
		return provider.Replayability()
	}
	return ReplayUnknown
}
