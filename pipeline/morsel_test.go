package pipeline

import (
	"errors"
	"gocolumn/memory"
	"strings"
	"testing"
)

func TestByteSizerUsesActualVariableWidthAndHardLimit(t *testing.T) {
	const rows = 32
	batch := memory.NewBatch()
	offsets := make([]uint32, rows+1)
	var data []byte
	for row := 0; row < rows; row++ {
		value := strings.Repeat("x", 8+row%5)
		data = append(data, value...)
		offsets[row+1] = uint32(len(data))
	}
	batch.Strings["s"] = &memory.StringViewVector{Offsets: offsets, Data: data, Encoding: memory.EncodingView}
	batch.Reset(rows)
	opts := Options{Workers: 2, TargetMorselBytes: 128, MaxMorselBytes: 256, MaxInFlightBytes: 1024, MinMorselRows: 2, MaxMorselRows: 16}
	sizer, err := NewByteSizer(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), opts)
	if err != nil {
		t.Fatal(err)
	}
	initial := sizer.ObservedBytesPerRow()
	ranges, err := sizer.Split(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) < 2 || sizer.ObservedBytesPerRow() == initial {
		t.Fatalf("ranges=%v initial_width=%f observed=%f", ranges, initial, sizer.ObservedBytesPerRow())
	}
	next := 0
	for _, r := range ranges {
		if r.Offset != next || r.Rows < 1 || r.Bytes > opts.MaxMorselBytes {
			t.Fatalf("invalid range=%+v next=%d", r, next)
		}
		estimated, err := memory.EstimateBatchRangeBytes(batch, r.Offset, r.Rows)
		if err != nil || estimated != r.Bytes {
			t.Fatalf("range=%+v estimate=%d err=%v", r, estimated, err)
		}
		next += r.Rows
	}
	if next != rows {
		t.Fatalf("covered rows=%d want=%d", next, rows)
	}
}

func TestByteSizerRejectsOneOversizedValue(t *testing.T) {
	batch := memory.NewBatch()
	batch.Strings["s"] = &memory.StringViewVector{Offsets: []uint32{0, 1024}, Data: []byte(strings.Repeat("x", 1024)), Encoding: memory.EncodingView}
	batch.Reset(1)
	opts := Options{Workers: 1, TargetMorselBytes: 128, MaxMorselBytes: 512, MaxInFlightBytes: 1024, MinMorselRows: 1, MaxMorselRows: 1024}
	sizer, err := NewByteSizer(memory.NewSchema(memory.Field{Name: "s", Type: memory.TypeString}), opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sizer.Split(batch)
	var tooLarge *MorselTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Row != 0 || tooLarge.Bytes <= tooLarge.LimitBytes {
		t.Fatalf("oversized error=%T %v", err, err)
	}
}

func TestOptionsNormalizeAndValidate(t *testing.T) {
	opts := (Options{}).Normalize(4)
	if err := opts.Validate(); err != nil {
		t.Fatal(err)
	}
	if opts.Workers != 4 || opts.TargetMorselBytes <= 0 || opts.MaxInFlightBytes < opts.MaxMorselBytes {
		t.Fatalf("normalized options=%+v", opts)
	}
	for _, invalid := range []Options{
		{Workers: -1},
		{Workers: 1, TargetMorselBytes: 10, MaxMorselBytes: 9, MaxInFlightBytes: 100, MinMorselRows: 1, MaxMorselRows: 1},
		{Workers: 1, TargetMorselBytes: 10, MaxMorselBytes: 20, MaxInFlightBytes: 19, MinMorselRows: 1, MaxMorselRows: 1},
	} {
		if err := invalid.Normalize(1).Validate(); err == nil {
			t.Fatalf("invalid options accepted: %+v", invalid)
		}
	}
}
