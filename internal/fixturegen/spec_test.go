package fixturegen

import (
	"math"
	"strings"
	"testing"
)

func validSpec() DatasetSpec {
	return DatasetSpec{
		Version: SpecVersion,
		Rows:    1000,
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInt64},
			{Name: "x", Type: ColumnTypeFloat64, Distribution: DistributionNormal},
			{Name: "s", Type: ColumnTypeString, Cardinality: 100},
			{Name: "b", Type: ColumnTypeBool},
		},
	}
}

func TestValidateAcceptsValidSpec(t *testing.T) {
	if err := validSpec().Validate(); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
}

func TestValidateRejectsInvalidSpecs(t *testing.T) {
	ratio := func(v float64) *float64 { return &v }
	base := []ColumnSpec{{Name: "c", Type: ColumnTypeInt64}}
	cases := []struct {
		name string
		spec DatasetSpec
		want string
	}{
		{
			name: "unsupported version",
			spec: DatasetSpec{Version: 2, Rows: 10, Columns: base},
			want: "version",
		},
		{
			name: "missing version",
			spec: DatasetSpec{Rows: 10, Columns: base},
			want: "version",
		},
		{
			name: "zero rows",
			spec: DatasetSpec{Version: SpecVersion, Rows: 0, Columns: base},
			want: "rows",
		},
		{
			name: "negative rows",
			spec: DatasetSpec{Version: SpecVersion, Rows: -5, Columns: base},
			want: "rows",
		},
		{
			name: "no columns",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10},
			want: "at least one column",
		},
		{
			name: "empty column name",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Type: ColumnTypeInt64}}},
			want: "empty name",
		},
		{
			name: "duplicate column name",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{
				{Name: "a", Type: ColumnTypeInt64},
				{Name: "a", Type: ColumnTypeBool},
			}},
			want: "declared twice",
		},
		{
			name: "unknown type",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: "date32"}}},
			want: "unknown column type",
		},
		{
			name: "null rate above one",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, NullRate: 1.1}}},
			want: "null_rate",
		},
		{
			name: "negative null rate",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, NullRate: -0.1}}},
			want: "null_rate",
		},
		{
			name: "nan null rate",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, NullRate: math.NaN()}}},
			want: "null_rate",
		},
		{
			name: "negative cardinality",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, Cardinality: -1}}},
			want: "cardinality",
		},
		{
			name: "int64 domain overflow",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, IntMin: math.MaxInt64, Cardinality: 2}}},
			want: "overflow",
		},
		{
			name: "int64 rejects float settings",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, StdDev: 2}}},
			want: "another type",
		},
		{
			name: "int64 rejects normal distribution",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, Distribution: DistributionNormal}}},
			want: "distribution",
		},
		{
			name: "zipf skew requires zipf distribution",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, ZipfS: 2}}},
			want: "zipf",
		},
		{
			name: "zipf skew must exceed one",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, Distribution: DistributionZipf, ZipfS: 1}}},
			want: "zipf_s",
		},
		{
			name: "zipf shift must be at least one",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeInt64, Distribution: DistributionZipf, ZipfV: 0.5}}},
			want: "zipf_v",
		},
		{
			name: "float64 rejects cardinality",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, Cardinality: 5}}},
			want: "cardinality",
		},
		{
			name: "float64 rejects zipf",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, Distribution: DistributionZipf}}},
			want: "distribution",
		},
		{
			name: "float64 uniform requires ordered bounds",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, FloatMin: 5, FloatMax: 5}}},
			want: "float_max",
		},
		{
			name: "float64 normal rejects bounds",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, Distribution: DistributionNormal, FloatMax: 9}}},
			want: "float_min/float_max",
		},
		{
			name: "float64 normal rejects negative stddev",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, Distribution: DistributionNormal, StdDev: -1}}},
			want: "stddev",
		},
		{
			name: "float64 uniform rejects mean",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, Mean: 3}}},
			want: "mean/stddev",
		},
		{
			name: "float64 nan rate above one",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeFloat64, NaNRate: 2}}},
			want: "nan_rate",
		},
		{
			name: "string rejects negative length",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeString, MinLen: -1}}},
			want: "length range",
		},
		{
			name: "string rejects inverted length range",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeString, MinLen: 10, MaxLen: 4}}},
			want: "length range",
		},
		{
			name: "string zipf requires cardinality",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeString, Distribution: DistributionZipf}}},
			want: "cardinality",
		},
		{
			name: "string cardinality exceeds length capacity",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeString, Cardinality: 100, MaxLen: 1}}},
			want: "max_len",
		},
		{
			name: "string data exceeds column limit",
			spec: DatasetSpec{Version: SpecVersion, Rows: 1 << 26, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeString, MaxLen: 64}}},
			want: "4 GiB",
		},
		{
			name: "bool rejects cardinality",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeBool, Cardinality: 2}}},
			want: "another type",
		},
		{
			name: "bool rejects zipf distribution",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeBool, Distribution: DistributionZipf}}},
			want: "distribution",
		},
		{
			name: "bool true ratio above one",
			spec: DatasetSpec{Version: SpecVersion, Rows: 10, Columns: []ColumnSpec{{Name: "c", Type: ColumnTypeBool, TrueRatio: ratio(1.5)}}},
			want: "true_ratio",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestParseSpecRejectsMalformedDocuments(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"not json", `{`, "decode"},
		{"unknown field", `{"version":1,"rows":10,"columns":[{"name":"c","type":"int64","bogus":1}]}`, "decode"},
		{"missing version", `{"rows":10,"columns":[{"name":"c","type":"int64"}]}`, "version"},
		{"wrong version", `{"version":99,"rows":10,"columns":[{"name":"c","type":"int64"}]}`, "version"},
		{"invalid column", `{"version":1,"rows":10,"columns":[{"name":"c","type":"int128"}]}`, "unknown column type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSpec(strings.NewReader(tc.doc))
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestParseSpecAcceptsVersionedDocument(t *testing.T) {
	doc := `{
		"version": 1,
		"rows": 500,
		"columns": [
			{"name": "id", "type": "int64", "distribution": "uniform", "cardinality": 100, "null_rate": 0.1},
			{"name": "k", "type": "int64", "distribution": "zipf", "cardinality": 50, "zipf_s": 2.0},
			{"name": "x", "type": "float64", "distribution": "normal", "mean": 10, "stddev": 2, "nan_rate": 0.05},
			{"name": "y", "type": "float64", "float_min": -1, "float_max": 1},
			{"name": "s", "type": "string", "cardinality": 20, "min_len": 4, "max_len": 8},
			{"name": "b", "type": "bool", "true_ratio": 0.25, "null_rate": 0.1}
		]
	}`
	spec, err := ParseSpec(strings.NewReader(doc))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	if spec.Rows != 500 || len(spec.Columns) != 6 {
		t.Fatalf("parsed spec rows=%d columns=%d", spec.Rows, len(spec.Columns))
	}
	table, err := Generate(spec, 7)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if table.Rows() != 500 {
		t.Fatalf("generated %d rows, want 500", table.Rows())
	}
}

func TestParseSpecNilReader(t *testing.T) {
	if _, err := ParseSpec(nil); err == nil {
		t.Fatal("expected an error for a nil reader")
	}
}
