package fixturegen

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"

	"gocolumn/memory"
)

// Table is a generated in-memory columnar dataset. The batch is owned by the
// table and stays valid for the table's lifetime.
type Table struct {
	schema memory.Schema
	batch  *memory.Batch
}

// Schema returns the logical schema of the generated dataset.
func (t *Table) Schema() memory.Schema { return t.schema }

// Batch returns the generated columnar batch owned by the table.
func (t *Table) Batch() *memory.Batch { return t.batch }

// Rows returns the number of generated rows.
func (t *Table) Rows() int { return t.batch.Rows }

// stringAlphabet is the character set for random string content.
const stringAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// base62Alphabet encodes dictionary indexes with a fixed digit order so
// zero-padded codes keep insertion order sortable.
const base62Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Generate validates spec and deterministically materializes rows x columns
// of fixture data. Identical (seed, spec) pairs always produce identical
// tables; each column draws from its own seeded sub-stream derived from the
// seed, column index, and column name.
func Generate(spec DatasetSpec, seed uint64) (*Table, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	rows := int(spec.Rows)
	batch := memory.NewBatch()
	batch.Reset(rows)
	fields := make([]memory.Field, len(spec.Columns))
	for i, column := range spec.Columns {
		resolved, err := resolveColumn(column, spec.Rows)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", column.Name, err)
		}
		rng := columnRNG(seed, i, column.Name)
		switch column.Type {
		case ColumnTypeInt64:
			fields[i] = memory.Field{Name: column.Name, Type: memory.TypeInt64, Nullable: resolved.spec.NullRate > 0, Encoding: memory.EncodingPlain}
			if err := generateInt64(batch, resolved, rng, rows); err != nil {
				return nil, fmt.Errorf("column %q: %w", column.Name, err)
			}
		case ColumnTypeFloat64:
			fields[i] = memory.Field{Name: column.Name, Type: memory.TypeFloat64, Nullable: resolved.spec.NullRate > 0, Encoding: memory.EncodingPlain}
			generateFloat64(batch, resolved, rng, rows)
		case ColumnTypeString:
			fields[i] = memory.Field{Name: column.Name, Type: memory.TypeString, Nullable: resolved.spec.NullRate > 0, Encoding: memory.EncodingView}
			if err := generateString(batch, resolved, rng, rows); err != nil {
				return nil, fmt.Errorf("column %q: %w", column.Name, err)
			}
		case ColumnTypeBool:
			fields[i] = memory.Field{Name: column.Name, Type: memory.TypeBool, Nullable: resolved.spec.NullRate > 0, Encoding: memory.EncodingPlain}
			generateBool(batch, resolved, rng, rows)
		}
	}
	return &Table{schema: memory.NewSchema(fields...), batch: batch}, nil
}

// columnRNG derives the deterministic per-column sub-stream. Folded names and
// indexes keep columns independent of each other's draw consumption.
func columnRNG(seed uint64, index int, name string) *rand.Rand {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(name))
	return rand.New(rand.NewPCG(seed, hash.Sum64()^uint64(index+1)))
}

// drawValidity draws per-row NULL decisions when nullRate > 0 and returns a
// bitmap with valid rows set, or nil when every row is valid. Invalid rows
// keep the zero value of their vector.
func drawValidity(rng *rand.Rand, rows int, nullRate float64) memory.Bitmap {
	if nullRate <= 0 {
		return nil
	}
	validity := memory.EnsureBitmap(nil, rows)
	for row := 0; row < rows; row++ {
		if rng.Float64() >= nullRate {
			memory.Set(validity, row)
		}
	}
	return validity
}

func generateInt64(batch *memory.Batch, column resolvedColumn, rng *rand.Rand, rows int) error {
	vector := &memory.Int64Vector{Values: make([]int64, rows), Encoding: memory.EncodingPlain}
	batch.Int64s[column.spec.Name] = vector
	var zipf *zipfSampler
	if column.distribution == DistributionZipf {
		sampler, err := newZipfSampler(rng, column.zipfS, column.zipfV, uint64(column.cardinality))
		if err != nil {
			return err
		}
		zipf = sampler
	}
	vector.Validity = drawValidity(rng, rows, column.spec.NullRate)
	for row := 0; row < rows; row++ {
		if !vector.IsValid(row) {
			continue
		}
		if zipf != nil {
			vector.Values[row] = column.spec.IntMin + int64(zipf.next())
		} else {
			vector.Values[row] = column.spec.IntMin + rng.Int64N(column.cardinality)
		}
	}
	return nil
}

func generateFloat64(batch *memory.Batch, column resolvedColumn, rng *rand.Rand, rows int) {
	vector := &memory.Float64Vector{Values: make([]float64, rows), Encoding: memory.EncodingPlain}
	batch.Float64s[column.spec.Name] = vector
	vector.Validity = drawValidity(rng, rows, column.spec.NullRate)
	nanRate := column.spec.NaNRate
	for row := 0; row < rows; row++ {
		if !vector.IsValid(row) {
			continue
		}
		if nanRate > 0 {
			if rng.Float64() < nanRate {
				vector.Values[row] = math.NaN()
				continue
			}
		}
		if column.distribution == DistributionNormal {
			vector.Values[row] = rng.NormFloat64()*column.stddev + column.mean
		} else {
			vector.Values[row] = column.floatMin + rng.Float64()*(column.floatMax-column.floatMin)
		}
	}
}

func generateBool(batch *memory.Batch, column resolvedColumn, rng *rand.Rand, rows int) {
	vector := &memory.Uint32Vector{Values: make([]uint32, rows), Encoding: memory.EncodingPlain}
	batch.Uint32s[column.spec.Name] = vector
	vector.Validity = drawValidity(rng, rows, column.spec.NullRate)
	for row := 0; row < rows; row++ {
		if !vector.IsValid(row) {
			continue
		}
		if rng.Float64() < column.trueRatio {
			vector.Values[row] = 1
		}
	}
}

// generateString fills a StringViewVector. With a positive cardinality it
// first materializes a dictionary of distinct fixed-format values so repeated
// draws share spellings; otherwise every row draws an independent random
// string.
func generateString(batch *memory.Batch, column resolvedColumn, rng *rand.Rand, rows int) error {
	vector := &memory.StringViewVector{Offsets: make([]uint32, rows+1), Encoding: memory.EncodingView}
	batch.Strings[column.spec.Name] = vector
	var dictionary []string
	var zipf *zipfSampler
	if column.cardinality > 0 {
		// A dedicated sub-stream keeps dictionary construction independent of
		// per-row draws.
		dictRNG := rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64()))
		dictionary = buildStringDictionary(dictRNG, column)
		if column.distribution == DistributionZipf {
			sampler, err := newZipfSampler(rng, column.zipfS, column.zipfV, uint64(column.cardinality))
			if err != nil {
				return err
			}
			zipf = sampler
		}
	}
	vector.Validity = drawValidity(rng, rows, column.spec.NullRate)
	data := vector.Data
	for row := 0; row < rows; row++ {
		if vector.IsValid(row) {
			if dictionary != nil {
				index := 0
				if zipf != nil {
					index = int(zipf.next())
				} else {
					index = rng.IntN(len(dictionary))
				}
				data = append(data, dictionary[index]...)
			} else {
				length := column.minLen
				if span := column.maxLen - column.minLen; span > 0 {
					length += rng.IntN(span + 1)
				}
				for i := 0; i < length; i++ {
					data = append(data, stringAlphabet[rng.IntN(len(stringAlphabet))])
				}
			}
		}
		if uint64(len(data)) > math.MaxUint32 {
			return fmt.Errorf("string data exceeds the 4 GiB column limit")
		}
		vector.Offsets[row+1] = uint32(len(data))
	}
	vector.Data = data
	return nil
}

// buildStringDictionary materializes cardinality distinct values. Every value
// ends with a fixed-width base-62 encoding of its index, guaranteeing
// distinctness; the remaining prefix is random padding. The padded length is
// the same for every entry: max(minLen, indexWidth).
func buildStringDictionary(rng *rand.Rand, column resolvedColumn) []string {
	cardinality := int(column.cardinality)
	width := base62Width(uint64(column.cardinality - 1))
	length := column.minLen
	if width > length {
		length = width
	}
	dictionary := make([]string, cardinality)
	buffer := make([]byte, 0, length)
	for index := 0; index < cardinality; index++ {
		buffer = buffer[:0]
		for i := 0; i < length-width; i++ {
			buffer = append(buffer, stringAlphabet[rng.IntN(len(stringAlphabet))])
		}
		buffer = appendBase62(buffer, uint64(index), width)
		dictionary[index] = string(buffer)
	}
	return dictionary
}

// base62Width returns the number of base-62 digits needed to encode n.
func base62Width(n uint64) int {
	width := 1
	for n >= 62 {
		n /= 62
		width++
	}
	return width
}

func appendBase62(dst []byte, n uint64, width int) []byte {
	var digits [11]byte
	for i := width - 1; i >= 0; i-- {
		digits[i] = base62Alphabet[n%62]
		n /= 62
	}
	return append(dst, digits[:width]...)
}
