package parquet

import (
	"context"
	"fmt"

	arrowparquet "github.com/apache/arrow-go/v18/parquet"
)

const dayNanoseconds = int64(86400) * 1_000_000_000

func time64ValueEncodingSupported(physical arrowparquet.Type, encoding arrowparquet.Encoding) bool {
	if physical != arrowparquet.Types.Int64 {
		return false
	}
	switch encoding {
	case arrowparquet.Encodings.Plain, arrowparquet.Encodings.PlainDict, arrowparquet.Encodings.RLEDict:
		return true
	default:
		return false
	}
}

// TIME is a local wall clock, not a signed duration. Discard physically
// meaningless values before publishing this batch, without checking NULL
// payload slots or introducing casts or timezone interpretation.
func (s *batchStream) validateWallClock(ctx context.Context, b columnBinding) error {
	v := s.batch.Int64s[b.Name]
	for row, clock := range v.Values {
		if row%64 == 0 {
			if err := s.checkContext(ctx); err != nil {
				return err
			}
		}
		if v.IsValid(row) && (clock < 0 || clock >= dayNanoseconds) {
			return fmt.Errorf("parquet wall-clock time column %q batch row %d is out of range [0,86400000000000)", b.Name, row)
		}
	}
	return nil
}
