package expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"gocolumn/memory"
)

// Keep an independent copy of the pre-cache row loop to compare partial writes,
// reservation requests and error rows, not only successful result strings.
func reviewRowwiseStringCase(out, in *scalarRegister, op Op, rows int) error {
	offsets := ensureOffsets(out, rows)
	offsets[0] = 0
	out.data = out.data[:0]
	for row := 0; row < rows; row++ {
		if isValidRegister(out, row) {
			value, err := registerString(in, row)
			if err != nil {
				return err
			}
			if err := appendScalarCase(out, value, op); err != nil {
				return fmt.Errorf("%s at row %d: %w", op, row, err)
			}
		}
		offsets[row+1] = uint32(len(out.data))
	}
	return nil
}

func TestDictionaryStringCaseReviewFailureParity(t *testing.T) {
	const rows = 257
	budgetErr := errors.New("review budget exhausted")
	for _, op := range []Op{OpLower, OpUpper} {
		for _, failure := range []string{"budget", "context"} {
			for failAt := 0; failAt <= 12; failAt++ {
				t.Run(fmt.Sprintf("%s/%s/%d", op, failure, failAt), func(t *testing.T) {
					v := caseDictionary(rows, 8)
					// Invalid offsets and invalid codes must retain DictionaryValue's
					// empty-value semantics, including subsequent cache hits.
					v.Offsets[2] = uint32(len(v.Data) + 1)
					v.Indices[9], v.Indices[17] = 250, 250
					v.Indices[10], v.Indices[18] = ^uint32(0), ^uint32(0)
					type result struct {
						data     []byte
						offsets  []uint32
						requests []int64
						checks   int
						err      string
					}
					var results [2]result
					for path := range results {
						r := &results[path]
						out := scalarRegister{typ: memory.TypeString}
						out.acquire = func(n int64) (memory.Reservation, error) {
							return scalarNoopReservation{}, nil
						}
						if err := prepareScalarBuffers(&out, rows); err != nil {
							t.Fatal(err)
						}
						fillAll(out.validity, rows)
						for row := 0; row < rows; row += 7 {
							out.validity[row/64] &^= uint64(1) << uint(row%64)
						}
						out.acquire = func(n int64) (memory.Reservation, error) {
							r.requests = append(r.requests, n)
							if failure == "budget" && len(r.requests) == failAt {
								return nil, budgetErr
							}
							return scalarNoopReservation{}, nil
						}
						out.checkContext = func() error {
							r.checks++
							if failure == "context" && r.checks == failAt {
								return context.Canceled
							}
							return nil
						}
						in := scalarRegister{typ: memory.TypeString, strDict: v}
						eval := reviewRowwiseStringCase
						if path == 1 {
							eval = evalStringCase
						}
						if err := eval(&out, &in, op, rows); err != nil {
							r.err = err.Error()
						}
						r.data = bytes.Clone(out.data)
						r.offsets = append([]uint32(nil), out.offsets...)
					}
					if !reflect.DeepEqual(results[0], results[1]) {
						t.Fatalf("rowwise/cache differ: rowwise=%+v cache=%+v", results[0], results[1])
					}
				})
			}
		}
	}
}
