package platformcal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SchemaVersion is the platform profile schema version written by this
// build. Load rejects any other version so a stale or future profile can
// never silently feed wrong constants into strategy selection.
const SchemaVersion = 1

// DefaultPath is the conventional profile location: a platform-profile.json
// file in the process working directory.
const DefaultPath = "platform-profile.json"

// ErrUnsupportedSchemaVersion marks a profile whose schema version this
// build cannot interpret. It wraps into the Load error chain.
var ErrUnsupportedSchemaVersion = errors.New("platformcal: unsupported profile schema version")

// PlatformProfile is the versioned result of one on-machine calibration run.
// Every section carries its measured value plus the method that produced it;
// a section with Calibrated=false holds zero values and consumers must keep
// their shipped defaults for it.
type PlatformProfile struct {
	// SchemaVersion must equal SchemaVersion.
	SchemaVersion int `json:"schema_version"`
	// GOOS and GOARCH identify the target platform of the calibration host.
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
	// CPUModel is the processor brand string, or "unknown" when the host does
	// not expose one.
	CPUModel string `json:"cpu_model"`
	// NumCPU is the logical CPU count seen at calibration time.
	NumCPU int `json:"num_cpu"`
	// CalibratedAt is the UTC start time of the calibration run.
	CalibratedAt time.Time `json:"calibrated_at"`
	// ElapsedSeconds is the wall-clock duration of the calibration run.
	ElapsedSeconds float64 `json:"elapsed_seconds"`

	Aggregation AggregationCalibration `json:"aggregation"`
	RadixJoin   RadixJoinCalibration   `json:"radix_join"`
	Workers     WorkerCalibration      `json:"workers"`
}

// AggregationCalibration carries the measured thresholds at which parallel
// radix aggregation starts to beat serial aggregation.
type AggregationCalibration struct {
	// RadixRowThreshold is the smallest selected-row count where the measured
	// parallel strategy durably beat the serial one.
	RadixRowThreshold int64 `json:"radix_row_threshold"`
	// RadixCardinalityThreshold is the smallest distinct-key count where the
	// measured parallel strategy durably beat the serial one.
	RadixCardinalityThreshold int64 `json:"radix_cardinality_threshold"`
	// Calibrated reports whether the values come from measurements. False
	// means the section holds zero values and shipped defaults apply.
	Calibrated bool `json:"calibrated"`
	// Samples is the total number of timed repetitions behind the section.
	Samples int `json:"samples"`
	// Method documents the workload and decision rule that produced the values.
	Method string `json:"method"`
}

// RadixJoinCalibration carries the measured minimums for automatic parallel
// radix join selection.
type RadixJoinCalibration struct {
	// MinWorkers is the smallest worker count with durable parallel gain.
	MinWorkers int `json:"min_workers"`
	// MinBuildRows is the smallest build side where parallel radix join
	// durably beat the single-table join at MinWorkers.
	MinBuildRows int64 `json:"min_build_rows"`
	// MinProbeRows is the smallest probe side where parallel radix join
	// durably beat the single-table join at MinWorkers.
	MinProbeRows int64 `json:"min_probe_rows"`
	// Calibrated reports whether the values come from measurements.
	Calibrated bool `json:"calibrated"`
	// Samples is the total number of timed repetitions behind the section.
	Samples int `json:"samples"`
	// Method documents the workload and decision rule that produced the values.
	Method string `json:"method"`
}

// WorkerCalibration carries the measured worker scaling knee: the largest
// worker count that still pays for its scheduling cost.
type WorkerCalibration struct {
	// Knee is the largest worker count whose marginal speedup over the
	// previous count cleared the calibration gain threshold.
	Knee int `json:"knee"`
	// PipelineCap is the recommended ceiling for pipeline worker counts.
	PipelineCap int `json:"pipeline_cap"`
	// Calibrated reports whether the values come from measurements.
	Calibrated bool `json:"calibrated"`
	// Samples is the total number of timed repetitions behind the section.
	Samples int `json:"samples"`
	// Method documents the workload and decision rule that produced the values.
	Method string `json:"method"`
}

// Validate rejects profiles that are structurally inconsistent: a wrong
// schema version, a calibrated section with non-positive values, or an
// uncalibrated section that still carries values.
func (p *PlatformProfile) Validate() error {
	if p == nil {
		return fmt.Errorf("platformcal: profile is nil")
	}
	if p.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: got %d, want %d", ErrUnsupportedSchemaVersion, p.SchemaVersion, SchemaVersion)
	}
	if p.NumCPU < 0 {
		return fmt.Errorf("platformcal: num_cpu %d is negative", p.NumCPU)
	}
	if err := p.Aggregation.validate(); err != nil {
		return fmt.Errorf("platformcal: aggregation: %w", err)
	}
	if err := p.RadixJoin.validate(); err != nil {
		return fmt.Errorf("platformcal: radix join: %w", err)
	}
	if err := p.Workers.validate(); err != nil {
		return fmt.Errorf("platformcal: workers: %w", err)
	}
	return nil
}

func (c *AggregationCalibration) validate() error {
	if !c.Calibrated {
		if c.RadixRowThreshold != 0 || c.RadixCardinalityThreshold != 0 {
			return fmt.Errorf("uncalibrated section must carry zero thresholds")
		}
		return nil
	}
	if c.RadixRowThreshold <= 0 || c.RadixCardinalityThreshold <= 0 {
		return fmt.Errorf("calibrated thresholds must be positive, got rows=%d cardinality=%d", c.RadixRowThreshold, c.RadixCardinalityThreshold)
	}
	return nil
}

func (c *RadixJoinCalibration) validate() error {
	if !c.Calibrated {
		if c.MinWorkers != 0 || c.MinBuildRows != 0 || c.MinProbeRows != 0 {
			return fmt.Errorf("uncalibrated section must carry zero minimums")
		}
		return nil
	}
	if c.MinWorkers < 1 || c.MinBuildRows <= 0 || c.MinProbeRows <= 0 {
		return fmt.Errorf("calibrated minimums must be positive, got workers=%d build=%d probe=%d", c.MinWorkers, c.MinBuildRows, c.MinProbeRows)
	}
	return nil
}

func (c *WorkerCalibration) validate() error {
	if !c.Calibrated {
		if c.Knee != 0 || c.PipelineCap != 0 {
			return fmt.Errorf("uncalibrated section must carry zero values")
		}
		return nil
	}
	if c.Knee < 1 || c.PipelineCap < 1 {
		return fmt.Errorf("calibrated knee and pipeline cap must be positive, got knee=%d cap=%d", c.Knee, c.PipelineCap)
	}
	return nil
}

// Load reads and validates the profile at path. A missing file reports
// os.ErrNotExist; malformed JSON, a wrong schema version, or an inconsistent
// section report a descriptive error wrapping ErrUnsupportedSchemaVersion
// where applicable.
func Load(path string) (*PlatformProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var profile PlatformProfile
	if err := json.Unmarshal(data, &profile); err != nil {
		return nil, fmt.Errorf("platformcal: parse %s: %w", path, err)
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("platformcal: validate %s: %w", path, err)
	}
	return &profile, nil
}

// Save writes the profile at path atomically: the encoded JSON lands in a
// temporary sibling file that is then renamed over path, so a crash never
// leaves a half-written profile behind.
func Save(path string, profile *PlatformProfile) error {
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("platformcal: refuse to save invalid profile: %w", err)
	}
	data, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return fmt.Errorf("platformcal: encode profile: %w", err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("platformcal: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("platformcal: rename %s over %s: %w", tmp, filepath.Base(path), err)
	}
	return nil
}
