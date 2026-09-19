package fixturegen

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"

	"gocolumn/memory"
)

// contentHash folds the schema and every vector of the table into one digest
// so determinism tests compare complete content instead of samples.
func contentHash(t *testing.T, table *Table) [32]byte {
	t.Helper()
	hash := sha256.New()
	var scratch [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(scratch[:], v)
		hash.Write(scratch[:])
	}
	for _, field := range table.Schema().Fields {
		hash.Write([]byte(field.Name))
		put(uint64(field.Type))
		batch := table.Batch()
		switch field.Type {
		case memory.TypeInt64:
			vector := batch.Int64s[field.Name]
			for _, value := range vector.Values {
				put(uint64(value))
			}
			for _, word := range vector.Validity {
				put(word)
			}
		case memory.TypeFloat64:
			vector := batch.Float64s[field.Name]
			for _, value := range vector.Values {
				put(math.Float64bits(value))
			}
			for _, word := range vector.Validity {
				put(word)
			}
		case memory.TypeBool:
			vector := batch.Uint32s[field.Name]
			for _, value := range vector.Values {
				put(uint64(value))
			}
			for _, word := range vector.Validity {
				put(word)
			}
		case memory.TypeString:
			vector := batch.Strings[field.Name]
			for _, offset := range vector.Offsets {
				put(uint64(offset))
			}
			hash.Write(vector.Data)
			for _, word := range vector.Validity {
				put(word)
			}
		default:
			t.Fatalf("unhandled field type %s", field.Type)
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func mixedSpec(rows int64) DatasetSpec {
	return DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInt64, Cardinality: 5000, NullRate: 0.1},
			{Name: "score", Type: ColumnTypeFloat64, Distribution: DistributionNormal, Mean: 100, StdDev: 15, NullRate: 0.05, NaNRate: 0.02},
			{Name: "label", Type: ColumnTypeString, Cardinality: 64, MinLen: 4, MaxLen: 12, NullRate: 0.1},
			{Name: "flag", Type: ColumnTypeBool, NullRate: 0.1},
		},
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	spec := mixedSpec(4096)
	first, err := Generate(spec, 42)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	second, err := Generate(spec, 42)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if contentHash(t, first) != contentHash(t, second) {
		t.Fatal("identical seed and spec produced different content")
	}
}

func TestGenerateChangesWithSeed(t *testing.T) {
	spec := mixedSpec(4096)
	first, err := Generate(spec, 42)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	second, err := Generate(spec, 43)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if contentHash(t, first) == contentHash(t, second) {
		t.Fatal("different seeds produced identical content")
	}
}

func TestGenerateRejectsInvalidSpec(t *testing.T) {
	if _, err := Generate(DatasetSpec{Version: SpecVersion, Rows: 10}, 1); err == nil {
		t.Fatal("expected an error for a spec without columns")
	}
}

func TestGenerateRowCountAndSchema(t *testing.T) {
	table, err := Generate(mixedSpec(1234), 9)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if table.Rows() != 1234 {
		t.Fatalf("rows=%d, want 1234", table.Rows())
	}
	schema := table.Schema()
	wantTypes := map[string]memory.DataType{
		"id":    memory.TypeInt64,
		"score": memory.TypeFloat64,
		"label": memory.TypeString,
		"flag":  memory.TypeBool,
	}
	if len(schema.Fields) != len(wantTypes) {
		t.Fatalf("schema has %d fields, want %d", len(schema.Fields), len(wantTypes))
	}
	for name, want := range wantTypes {
		field, ok := schema.Field(name)
		if !ok {
			t.Fatalf("column %q missing from schema", name)
		}
		if field.Type != want {
			t.Fatalf("column %q type %s, want %s", name, field.Type, want)
		}
		if !field.Nullable {
			t.Fatalf("column %q should be nullable", name)
		}
	}
}

func countNulls(validity memory.Bitmap, rows int) int {
	if validity == nil {
		return 0
	}
	nulls := 0
	for row := 0; row < rows; row++ {
		if !memory.IsSet(validity, row) {
			nulls++
		}
	}
	return nulls
}

func TestNullRatesWithinTolerance(t *testing.T) {
	const rows = 100000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "a", Type: ColumnTypeInt64, NullRate: 0.25},
			{Name: "b", Type: ColumnTypeString, NullRate: 0.5},
			{Name: "c", Type: ColumnTypeFloat64, NullRate: 0.0},
		},
	}
	table, err := Generate(spec, 3)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cases := []struct {
		column   string
		validity memory.Bitmap
		want     float64
	}{
		{"a", table.Batch().Int64s["a"].Validity, 0.25},
		{"b", table.Batch().Strings["b"].Validity, 0.5},
		{"c", table.Batch().Float64s["c"].Validity, 0.0},
	}
	for _, tc := range cases {
		got := float64(countNulls(tc.validity, rows)) / rows
		if math.Abs(got-tc.want) > 0.02 {
			t.Fatalf("column %q null rate %f outside %f +/- 0.02", tc.column, got, tc.want)
		}
	}
	if table.Batch().Float64s["c"].Validity != nil {
		t.Fatal("zero null rate should keep a nil validity bitmap")
	}
}

func TestInt64CardinalityWithinTolerance(t *testing.T) {
	const rows = 100000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "bounded", Type: ColumnTypeInt64, Cardinality: 997, IntMin: -100},
		},
	}
	table, err := Generate(spec, 11)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	values := table.Batch().Int64s["bounded"].Values
	distinct := make(map[int64]struct{}, 1024)
	for _, value := range values {
		if value < -100 || value >= -100+997 {
			t.Fatalf("value %d outside domain [-100, 897)", value)
		}
		distinct[value] = struct{}{}
	}
	if len(distinct) < 990 || len(distinct) > 997 {
		t.Fatalf("distinct values %d outside [990, 997]", len(distinct))
	}
}

func TestFloat64DistributionsAndNaNRate(t *testing.T) {
	const rows = 100000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "u", Type: ColumnTypeFloat64, FloatMin: -2, FloatMax: 4},
			{Name: "n", Type: ColumnTypeFloat64, Distribution: DistributionNormal, Mean: 50, StdDev: 2, NaNRate: 0.1},
		},
	}
	table, err := Generate(spec, 17)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	for _, value := range table.Batch().Float64s["u"].Values {
		if value < -2 || value >= 4 {
			t.Fatalf("uniform value %f outside [-2, 4)", value)
		}
	}
	normal := table.Batch().Float64s["n"].Values
	nans := 0
	var sum float64
	for _, value := range normal {
		if math.IsNaN(value) {
			nans++
			continue
		}
		sum += value
	}
	nanRate := float64(nans) / rows
	if math.Abs(nanRate-0.1) > 0.015 {
		t.Fatalf("NaN rate %f outside 0.1 +/- 0.015", nanRate)
	}
	mean := sum / float64(rows-nans)
	if math.Abs(mean-50) > 0.5 {
		t.Fatalf("normal mean %f outside 50 +/- 0.5", mean)
	}
}

func TestBoolTrueRatioWithinTolerance(t *testing.T) {
	const rows = 100000
	truthy := 0.7
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "b", Type: ColumnTypeBool, TrueRatio: &truthy},
		},
	}
	table, err := Generate(spec, 23)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	trues := 0
	for _, value := range table.Batch().Uint32s["b"].Values {
		if value != 0 {
			trues++
		}
	}
	got := float64(trues) / rows
	if math.Abs(got-0.7) > 0.02 {
		t.Fatalf("true ratio %f outside 0.7 +/- 0.02", got)
	}
}

func TestStringConstraintsAndCardinality(t *testing.T) {
	const rows = 50000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "dict", Type: ColumnTypeString, Cardinality: 500, MinLen: 6, MaxLen: 12},
			{Name: "free", Type: ColumnTypeString, MinLen: 3, MaxLen: 9},
		},
	}
	table, err := Generate(spec, 29)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	dict := table.Batch().Strings["dict"]
	distinct := make(map[string]struct{}, 512)
	for row := 0; row < rows; row++ {
		value := string(dict.ValueBytes(row))
		if len(value) < 6 || len(value) > 12 {
			t.Fatalf("dictionary value %q length outside [6, 12]", value)
		}
		distinct[value] = struct{}{}
	}
	if len(distinct) != 500 {
		t.Fatalf("dictionary distinct values %d, want exactly 500", len(distinct))
	}
	free := table.Batch().Strings["free"]
	for row := 0; row < rows; row++ {
		if length := len(free.ValueBytes(row)); length < 3 || length > 9 {
			t.Fatalf("free value length %d outside [3, 9]", length)
		}
	}
}

// topShare returns the fraction of rows held by the most frequent value.
func topShare(values []int64) float64 {
	counts := make(map[int64]int, 1024)
	top := 0
	for _, value := range values {
		counts[value]++
		if counts[value] > top {
			top = counts[value]
		}
	}
	return float64(top) / float64(len(values))
}

func TestZipfSkewConcentratesHead(t *testing.T) {
	const rows = 200000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "skewed", Type: ColumnTypeInt64, Distribution: DistributionZipf, Cardinality: 10000, ZipfS: 2.0},
			{Name: "flat", Type: ColumnTypeInt64, Cardinality: 10000},
		},
	}
	table, err := Generate(spec, 31)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	skewed := topShare(table.Batch().Int64s["skewed"].Values)
	flat := topShare(table.Batch().Int64s["flat"].Values)
	if skewed < 0.3 {
		t.Fatalf("zipf top-1 share %f, want >= 0.3", skewed)
	}
	if flat > 0.01 {
		t.Fatalf("uniform top-1 share %f, want <= 0.01", flat)
	}
	if skewed < 50*flat {
		t.Fatalf("zipf top-1 share %f is not clearly above uniform %f", skewed, flat)
	}
}

func TestZipfStringSkew(t *testing.T) {
	const rows = 50000
	spec := DatasetSpec{
		Version: SpecVersion,
		Rows:    rows,
		Columns: []ColumnSpec{
			{Name: "s", Type: ColumnTypeString, Distribution: DistributionZipf, Cardinality: 1000, ZipfS: 1.5, MinLen: 8, MaxLen: 8},
		},
	}
	table, err := Generate(spec, 37)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	vector := table.Batch().Strings["s"]
	counts := make(map[string]int, 1024)
	top := 0
	for row := 0; row < rows; row++ {
		value := string(vector.ValueBytes(row))
		counts[value]++
		if counts[value] > top {
			top = counts[value]
		}
	}
	if share := float64(top) / rows; share < 0.05 {
		t.Fatalf("zipf string top-1 share %f, want >= 0.05", share)
	}
}

func TestZipfSamplerValidationAndDeterminism(t *testing.T) {
	if _, err := newZipfSampler(rand.New(rand.NewPCG(1, 2)), 1.0, 1, 10); err == nil {
		t.Fatal("expected an error for s <= 1")
	}
	if _, err := newZipfSampler(rand.New(rand.NewPCG(1, 2)), 1.5, 0.5, 10); err == nil {
		t.Fatal("expected an error for v < 1")
	}
	if _, err := newZipfSampler(rand.New(rand.NewPCG(1, 2)), 1.5, 1, 0); err == nil {
		t.Fatal("expected an error for zero cardinality")
	}
	first, err := newZipfSampler(rand.New(rand.NewPCG(1, 2)), 1.5, 1, 1000)
	if err != nil {
		t.Fatalf("newZipfSampler: %v", err)
	}
	second, err := newZipfSampler(rand.New(rand.NewPCG(1, 2)), 1.5, 1, 1000)
	if err != nil {
		t.Fatalf("newZipfSampler: %v", err)
	}
	for i := 0; i < 10000; i++ {
		a, b := first.next(), second.next()
		if a != b {
			t.Fatalf("draw %d diverged: %d != %d", i, a, b)
		}
		if a >= 1000 {
			t.Fatalf("draw %d out of range: %d", i, a)
		}
	}
}

func TestBase62Width(t *testing.T) {
	cases := []struct {
		n    uint64
		want int
	}{
		{0, 1},
		{61, 1},
		{62, 2},
		{62*62 - 1, 2},
		{62 * 62, 3},
		{math.MaxUint64, 11},
	}
	for _, tc := range cases {
		if got := base62Width(tc.n); got != tc.want {
			t.Fatalf("base62Width(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}
