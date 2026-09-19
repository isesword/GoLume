package fixturegen

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
)

// SpecVersion is the only DatasetSpec JSON schema version accepted by
// ParseSpec and Validate.
const SpecVersion = 1

// ColumnType names the logical type of a generated column.
type ColumnType string

const (
	// ColumnTypeInt64 generates signed 64-bit integers.
	ColumnTypeInt64 ColumnType = "int64"
	// ColumnTypeFloat64 generates IEEE-754 64-bit floating-point values.
	ColumnTypeFloat64 ColumnType = "float64"
	// ColumnTypeString generates UTF-8 strings.
	ColumnTypeString ColumnType = "string"
	// ColumnTypeBool generates boolean values.
	ColumnTypeBool ColumnType = "bool"
)

// Distribution names the value distribution of a generated column. The empty
// value selects the uniform default for every column type.
type Distribution string

const (
	// DistributionUniform samples every domain value with equal probability.
	DistributionUniform Distribution = "uniform"
	// DistributionZipf samples domain ranks from a Zipf distribution,
	// concentrating rows on low ranks. It is valid for int64 and string
	// columns and requires a positive Cardinality.
	DistributionZipf Distribution = "zipf"
	// DistributionNormal samples from a Gaussian parameterized by Mean and
	// StdDev. It is valid for float64 columns only.
	DistributionNormal Distribution = "normal"
)

// ColumnSpec describes one generated column. Fields that do not apply to the
// declared Type must keep their zero value; Validate rejects cross-type
// settings so specification mistakes surface as errors instead of silently
// ignored knobs.
type ColumnSpec struct {
	// Name is the column name; it must be non-empty and unique per dataset.
	Name string `json:"name"`
	// Type selects the logical column type.
	Type ColumnType `json:"type"`
	// Distribution selects the value distribution; empty means uniform.
	Distribution Distribution `json:"distribution,omitempty"`
	// NullRate is the probability in [0,1] that a row is NULL.
	NullRate float64 `json:"null_rate,omitempty"`
	// Cardinality bounds the number of distinct values of int64 and string
	// columns. Zero selects the dataset row count for int64 and unbounded
	// per-row generation for string. It is invalid for float64 and bool.
	Cardinality int64 `json:"cardinality,omitempty"`
	// IntMin is the inclusive lower domain bound for int64 columns. Values
	// occupy the half-open domain [IntMin, IntMin+Cardinality).
	IntMin int64 `json:"int_min,omitempty"`
	// FloatMin and FloatMax bound uniform float64 columns. When both are
	// zero the domain defaults to [0,1).
	FloatMin float64 `json:"float_min,omitempty"`
	FloatMax float64 `json:"float_max,omitempty"`
	// Mean and StdDev parameterize normal float64 columns. StdDev defaults
	// to 1 and must be positive when set.
	Mean   float64 `json:"mean,omitempty"`
	StdDev float64 `json:"stddev,omitempty"`
	// NaNRate is the probability in [0,1] that a non-NULL float64 row is
	// NaN instead of a distribution draw.
	NaNRate float64 `json:"nan_rate,omitempty"`
	// MinLen and MaxLen bound string byte lengths. MinLen defaults to 0 and
	// MaxLen defaults to 16.
	MinLen int `json:"min_len,omitempty"`
	MaxLen int `json:"max_len,omitempty"`
	// TrueRatio is the probability in [0,1] of a true value for bool
	// columns. A nil pointer selects the 0.5 default; an explicit 0 or 1
	// yields constant false or true columns.
	TrueRatio *float64 `json:"true_ratio,omitempty"`
	// ZipfS is the Zipf skew parameter s > 1; zero selects 1.5.
	ZipfS float64 `json:"zipf_s,omitempty"`
	// ZipfV is the Zipf shift parameter v >= 1; zero selects 1.
	ZipfV float64 `json:"zipf_v,omitempty"`
}

// DatasetSpec describes a complete fixture dataset. The JSON form is
// versioned; only SpecVersion is accepted.
type DatasetSpec struct {
	// Version must equal SpecVersion.
	Version int `json:"version"`
	// Rows is the number of generated rows; it must be positive.
	Rows int64 `json:"rows"`
	// Columns lists the generated columns in schema order.
	Columns []ColumnSpec `json:"columns"`
}

// ParseSpec decodes and validates a versioned DatasetSpec JSON document.
// Unknown fields are rejected.
func ParseSpec(r io.Reader) (DatasetSpec, error) {
	if r == nil {
		return DatasetSpec{}, fmt.Errorf("dataset spec reader is nil")
	}
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	var spec DatasetSpec
	if err := decoder.Decode(&spec); err != nil {
		return DatasetSpec{}, fmt.Errorf("decode dataset spec: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return DatasetSpec{}, err
	}
	return spec, nil
}

// ParseSpecFile decodes and validates a versioned DatasetSpec JSON file.
func ParseSpecFile(path string) (DatasetSpec, error) {
	file, err := os.Open(path)
	if err != nil {
		return DatasetSpec{}, err
	}
	defer func() { _ = file.Close() }()
	return ParseSpec(file)
}

// Validate checks version, row count, column shape, and every column's
// distribution parameters and cross-type field usage.
func (s DatasetSpec) Validate() error {
	if s.Version != SpecVersion {
		return fmt.Errorf("dataset spec version %d is not supported; want %d", s.Version, SpecVersion)
	}
	if s.Rows <= 0 {
		return fmt.Errorf("dataset spec rows must be positive, got %d", s.Rows)
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("dataset spec requires at least one column")
	}
	seen := make(map[string]struct{}, len(s.Columns))
	for i, column := range s.Columns {
		if column.Name == "" {
			return fmt.Errorf("column %d has an empty name", i)
		}
		if _, ok := seen[column.Name]; ok {
			return fmt.Errorf("column %q is declared twice", column.Name)
		}
		seen[column.Name] = struct{}{}
		if _, err := resolveColumn(column, s.Rows); err != nil {
			return fmt.Errorf("column %q: %w", column.Name, err)
		}
	}
	return nil
}

// resolvedColumn is a ColumnSpec with defaults applied and per-type settings
// checked. Generation reads only resolved columns.
type resolvedColumn struct {
	spec         ColumnSpec
	distribution Distribution
	cardinality  int64
	floatMin     float64
	floatMax     float64
	mean         float64
	stddev       float64
	minLen       int
	maxLen       int
	trueRatio    float64
	zipfS        float64
	zipfV        float64
}

const (
	defaultFloatMin = 0.0
	defaultFloatMax = 1.0
	defaultStdDev   = 1.0
	defaultTrueRate = 0.5
	defaultMaxLen   = 16
	defaultZipfS    = 1.5
	defaultZipfV    = 1.0
)

func resolveColumn(spec ColumnSpec, rows int64) (resolvedColumn, error) {
	out := resolvedColumn{spec: spec}
	if err := checkRate("null_rate", spec.NullRate); err != nil {
		return out, err
	}
	switch spec.Type {
	case ColumnTypeInt64:
		if err := resolveInt64(&out, rows); err != nil {
			return out, err
		}
	case ColumnTypeFloat64:
		if err := resolveFloat64(&out); err != nil {
			return out, err
		}
	case ColumnTypeString:
		if err := resolveString(&out, rows); err != nil {
			return out, err
		}
	case ColumnTypeBool:
		if err := resolveBool(&out); err != nil {
			return out, err
		}
	default:
		return out, fmt.Errorf("unknown column type %q", spec.Type)
	}
	return out, nil
}

func checkRate(name string, rate float64) error {
	if math.IsNaN(rate) || rate < 0 || rate > 1 {
		return fmt.Errorf("%s must be in [0,1], got %v", name, rate)
	}
	return nil
}

func resolveDistribution(spec ColumnSpec, allowed ...Distribution) (Distribution, error) {
	distribution := spec.Distribution
	if distribution == "" {
		return DistributionUniform, nil
	}
	for _, candidate := range allowed {
		if distribution == candidate {
			return distribution, nil
		}
	}
	return "", fmt.Errorf("distribution %q is not valid for type %s", spec.Distribution, spec.Type)
}

func resolveZipf(spec ColumnSpec, out *resolvedColumn) error {
	out.zipfS = spec.ZipfS
	if out.zipfS == 0 {
		out.zipfS = defaultZipfS
	}
	out.zipfV = spec.ZipfV
	if out.zipfV == 0 {
		out.zipfV = defaultZipfV
	}
	if math.IsNaN(out.zipfS) || out.zipfS <= 1 {
		return fmt.Errorf("zipf_s must be > 1, got %v", out.zipfS)
	}
	if math.IsNaN(out.zipfV) || out.zipfV < 1 {
		return fmt.Errorf("zipf_v must be >= 1, got %v", out.zipfV)
	}
	return nil
}

func resolveInt64(out *resolvedColumn, rows int64) error {
	spec := out.spec
	if spec.FloatMin != 0 || spec.FloatMax != 0 || spec.Mean != 0 || spec.StdDev != 0 || spec.NaNRate != 0 ||
		spec.MinLen != 0 || spec.MaxLen != 0 || spec.TrueRatio != nil {
		return fmt.Errorf("int64 column has settings for another type")
	}
	distribution, err := resolveDistribution(spec, DistributionUniform, DistributionZipf)
	if err != nil {
		return err
	}
	out.distribution = distribution
	out.cardinality = spec.Cardinality
	if out.cardinality < 0 {
		return fmt.Errorf("cardinality must be non-negative, got %d", spec.Cardinality)
	}
	if out.cardinality == 0 {
		out.cardinality = rows
	}
	if out.cardinality > 0 && spec.IntMin > math.MaxInt64-(out.cardinality-1) {
		return fmt.Errorf("int64 domain lower bound %d with cardinality %d overflows int64", spec.IntMin, out.cardinality)
	}
	if distribution == DistributionZipf {
		return resolveZipf(spec, out)
	}
	if spec.ZipfS != 0 || spec.ZipfV != 0 {
		return fmt.Errorf("zipf_s/zipf_v require the zipf distribution")
	}
	return nil
}

func resolveFloat64(out *resolvedColumn) error {
	spec := out.spec
	if spec.IntMin != 0 || spec.MinLen != 0 || spec.MaxLen != 0 || spec.TrueRatio != nil ||
		spec.ZipfS != 0 || spec.ZipfV != 0 {
		return fmt.Errorf("float64 column has settings for another type")
	}
	if spec.Cardinality != 0 {
		return fmt.Errorf("cardinality is not supported for float64 columns")
	}
	if err := checkRate("nan_rate", spec.NaNRate); err != nil {
		return err
	}
	distribution, err := resolveDistribution(spec, DistributionUniform, DistributionNormal)
	if err != nil {
		return err
	}
	out.distribution = distribution
	switch distribution {
	case DistributionUniform:
		if spec.Mean != 0 || spec.StdDev != 0 {
			return fmt.Errorf("mean/stddev require the normal distribution")
		}
		out.floatMin, out.floatMax = spec.FloatMin, spec.FloatMax
		if out.floatMin == 0 && out.floatMax == 0 {
			out.floatMin, out.floatMax = defaultFloatMin, defaultFloatMax
		}
		if math.IsNaN(out.floatMin) || math.IsNaN(out.floatMax) || math.IsInf(out.floatMin, 0) || math.IsInf(out.floatMax, 0) {
			return fmt.Errorf("uniform float64 bounds must be finite")
		}
		if out.floatMax <= out.floatMin {
			return fmt.Errorf("float_max must be greater than float_min, got [%v, %v)", out.floatMin, out.floatMax)
		}
	case DistributionNormal:
		if spec.FloatMin != 0 || spec.FloatMax != 0 {
			return fmt.Errorf("float_min/float_max require the uniform distribution")
		}
		out.mean, out.stddev = spec.Mean, spec.StdDev
		if out.stddev == 0 {
			out.stddev = defaultStdDev
		}
		if math.IsNaN(out.mean) || math.IsInf(out.mean, 0) || math.IsNaN(out.stddev) || out.stddev <= 0 {
			return fmt.Errorf("normal float64 requires a finite mean and positive stddev, got mean=%v stddev=%v", out.mean, out.stddev)
		}
	}
	return nil
}

func resolveString(out *resolvedColumn, rows int64) error {
	spec := out.spec
	if spec.IntMin != 0 || spec.FloatMin != 0 || spec.FloatMax != 0 || spec.Mean != 0 || spec.StdDev != 0 ||
		spec.NaNRate != 0 || spec.TrueRatio != nil {
		return fmt.Errorf("string column has settings for another type")
	}
	if spec.Cardinality < 0 {
		return fmt.Errorf("cardinality must be non-negative, got %d", spec.Cardinality)
	}
	out.cardinality = spec.Cardinality
	distribution, err := resolveDistribution(spec, DistributionUniform, DistributionZipf)
	if err != nil {
		return err
	}
	out.distribution = distribution
	out.minLen, out.maxLen = spec.MinLen, spec.MaxLen
	if out.maxLen == 0 {
		out.maxLen = defaultMaxLen
	}
	if out.minLen < 0 || out.maxLen < out.minLen {
		return fmt.Errorf("string length range [%d, %d] is invalid", out.minLen, out.maxLen)
	}
	if rows > 0 && out.maxLen > 0 && rows > int64(math.MaxUint32)/int64(out.maxLen) {
		return fmt.Errorf("string data of %d rows with max_len %d may exceed the 4 GiB column limit", rows, out.maxLen)
	}
	if distribution == DistributionZipf {
		if out.cardinality <= 0 {
			return fmt.Errorf("zipf string columns require a positive cardinality")
		}
		if err := resolveZipf(spec, out); err != nil {
			return err
		}
	} else if spec.ZipfS != 0 || spec.ZipfV != 0 {
		return fmt.Errorf("zipf_s/zipf_v require the zipf distribution")
	}
	if out.cardinality > 0 && base62Width(uint64(out.cardinality-1)) > out.maxLen {
		return fmt.Errorf("cardinality %d cannot be encoded within max_len %d", out.cardinality, out.maxLen)
	}
	return nil
}

func resolveBool(out *resolvedColumn) error {
	spec := out.spec
	if spec.Cardinality != 0 || spec.IntMin != 0 || spec.FloatMin != 0 || spec.FloatMax != 0 || spec.Mean != 0 ||
		spec.StdDev != 0 || spec.NaNRate != 0 || spec.MinLen != 0 || spec.MaxLen != 0 || spec.ZipfS != 0 || spec.ZipfV != 0 {
		return fmt.Errorf("bool column has settings for another type")
	}
	distribution, err := resolveDistribution(spec, DistributionUniform)
	if err != nil {
		return err
	}
	out.distribution = distribution
	out.trueRatio = defaultTrueRate
	if spec.TrueRatio != nil {
		if err := checkRate("true_ratio", *spec.TrueRatio); err != nil {
			return err
		}
		out.trueRatio = *spec.TrueRatio
	}
	return nil
}
