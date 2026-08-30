// Package pipeline implements query-owned pull pipelines, byte-bounded
// exchanges, stable fan-in, and adaptive morsel sizing.
package pipeline

import (
	"fmt"
	"gocolumn/memory"
)

const (
	defaultTargetMorselBytes = int64(1 << 20)
	defaultMaxMorselBytes    = int64(8 << 20)
	defaultMaxInFlightBytes  = int64(64 << 20)
	defaultMinMorselRows     = 256
	defaultMaxMorselRows     = 64 << 10
)

// Ordering declares whether fan-in must reproduce input partition order.
type Ordering uint8

const (
	// OrderingStable emits contiguous worker ranges in original partition order.
	OrderingStable Ordering = iota
	// OrderingRelaxed emits morsels as workers finish and is valid only for plans
	// whose logical contract does not observe row order.
	OrderingRelaxed
)

// String returns the stable Explain spelling for an ordering contract.
func (o Ordering) String() string {
	switch o {
	case OrderingStable:
		return "stable"
	case OrderingRelaxed:
		return "relaxed"
	default:
		return fmt.Sprintf("ordering-%d", o)
	}
}

// Options configures one query-owned pipeline graph. Zero values are replaced
// by conservative defaults through Normalize; negative values remain invalid.
type Options struct {
	Workers           int
	TargetMorselBytes int64
	MaxMorselBytes    int64
	MaxInFlightBytes  int64
	MinMorselRows     int
	MaxMorselRows     int
	Ordering          Ordering
}

// Normalize fills zero-valued settings without hiding invalid negative values.
func (o Options) Normalize(defaultWorkers int) Options {
	if defaultWorkers < 1 {
		defaultWorkers = 1
	}
	if o.Workers == 0 {
		o.Workers = defaultWorkers
	}
	if o.TargetMorselBytes == 0 {
		o.TargetMorselBytes = defaultTargetMorselBytes
	}
	if o.MaxMorselBytes == 0 {
		o.MaxMorselBytes = defaultMaxMorselBytes
		if o.TargetMorselBytes > 0 && o.MaxMorselBytes < o.TargetMorselBytes*4 {
			o.MaxMorselBytes = o.TargetMorselBytes * 4
		}
	}
	if o.MaxInFlightBytes == 0 {
		o.MaxInFlightBytes = defaultMaxInFlightBytes
		workerTarget := int64(o.Workers) * o.TargetMorselBytes * 2
		if workerTarget > o.MaxInFlightBytes {
			o.MaxInFlightBytes = workerTarget
		}
		if o.MaxMorselBytes > o.MaxInFlightBytes {
			o.MaxInFlightBytes = o.MaxMorselBytes
		}
	}
	if o.MinMorselRows == 0 {
		o.MinMorselRows = defaultMinMorselRows
	}
	if o.MaxMorselRows == 0 {
		o.MaxMorselRows = defaultMaxMorselRows
	}
	return o
}

// Validate rejects configurations that cannot make forward progress while
// preserving the byte hard limits.
func (o Options) Validate() error {
	if o.Workers < 1 {
		return fmt.Errorf("pipeline workers must be positive")
	}
	if o.TargetMorselBytes <= 0 {
		return fmt.Errorf("target morsel bytes must be positive")
	}
	if o.MaxMorselBytes < o.TargetMorselBytes {
		return fmt.Errorf("max morsel bytes %d is below target %d", o.MaxMorselBytes, o.TargetMorselBytes)
	}
	if o.MaxInFlightBytes < o.MaxMorselBytes {
		return fmt.Errorf("max in-flight bytes %d is below max morsel bytes %d", o.MaxInFlightBytes, o.MaxMorselBytes)
	}
	if o.MinMorselRows < 1 {
		return fmt.Errorf("minimum morsel rows must be positive")
	}
	if o.MaxMorselRows < o.MinMorselRows {
		return fmt.Errorf("maximum morsel rows %d is below minimum %d", o.MaxMorselRows, o.MinMorselRows)
	}
	if o.Ordering > OrderingRelaxed {
		return fmt.Errorf("invalid pipeline ordering %d", o.Ordering)
	}
	return nil
}

// EstimateSchemaRowBytes returns a conservative initial row-width estimate
// before any physical Batch has been observed.
func EstimateSchemaRowBytes(schema memory.Schema) int64 {
	var bytes int64
	for _, field := range schema.Fields {
		bytes += estimateFieldRowBytes(field)
		if field.Nullable {
			bytes++
		}
	}
	if bytes < 1 {
		return 1
	}
	return bytes
}

func estimateFieldRowBytes(field memory.Field) int64 {
	switch field.Type {
	case memory.TypeInt8, memory.TypeUint8, memory.TypeBool:
		return 1
	case memory.TypeInt16, memory.TypeUint16:
		return 2
	case memory.TypeInt32, memory.TypeUint32, memory.TypeFloat32, memory.TypeDate32, memory.TypeTime32:
		return 4
	case memory.TypeInt64, memory.TypeUint64, memory.TypeFloat64, memory.TypeDate64, memory.TypeTimestamp, memory.TypeTime64, memory.TypeDuration:
		return 8
	case memory.TypeString, memory.TypeBinary:
		return 36
	case memory.TypeList:
		return 68
	case memory.TypeStruct:
		bytes := int64(1)
		for _, child := range field.Children {
			bytes += estimateFieldRowBytes(child)
		}
		return bytes
	case memory.TypeDecimal, memory.TypeArrow:
		return 32
	default:
		return 16
	}
}
