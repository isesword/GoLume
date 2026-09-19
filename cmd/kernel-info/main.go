// Command kernel-info reports exact per-operation dispatch decisions.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gocolumn/kernel"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type capabilityReport struct {
	ID               string `json:"id"`
	SelectedBackend  string `json:"selected_backend"`
	AvailableBackend string `json:"available_backend"`
	AutoAdmitted     bool   `json:"auto_admitted"`
	VectorBits       int    `json:"vector_bits"`
	HardwareSIMD     bool   `json:"hardware_simd"`
	Emulated         bool   `json:"emulated"`
}

type report struct {
	Arch    string             `json:"arch"`
	Mode    string             `json:"mode"`
	Kernels []capabilityReport `json:"kernels"`
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("kernel-info", flag.ContinueOnError)
	flags.SetOutput(stderr)
	modeName := flags.String("mode", "auto", "kernel dispatch mode: scalar, auto, or simd")
	jsonOutput := flags.Bool("json", false, "emit the selected per-kernel capabilities as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	mode, err := parseMode(*modeName)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	set := kernel.New(mode)
	autoSet := kernel.New(kernel.ModeAuto)
	forcedSet := kernel.New(kernel.ModeSIMD)
	entries := make([]capabilityReport, 0, len(kernel.KernelIDs()))
	for _, id := range kernel.KernelIDs() {
		capability, ok := set.Capability(id)
		if !ok {
			fmt.Fprintf(stderr, "missing capability for %s\n", id)
			return 1
		}
		autoCapability, autoOK := autoSet.Capability(id)
		forcedCapability, forcedOK := forcedSet.Capability(id)
		if !autoOK || !forcedOK {
			fmt.Fprintf(stderr, "missing comparison capability for %s\n", id)
			return 1
		}
		entries = append(entries, capabilityReport{
			ID:               id.String(),
			SelectedBackend:  capability.Backend,
			VectorBits:       capability.VectorBits,
			HardwareSIMD:     capability.HardwareSIMD,
			Emulated:         capability.Emulated,
			AvailableBackend: forcedCapability.Backend,
			AutoAdmitted:     autoCapability.Backend != "scalar",
		})
	}
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(report{Arch: set.Arch(), Mode: set.Mode().String(), Kernels: entries}); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "arch=%s mode=%s\n", set.Arch(), set.Mode())
	for _, entry := range entries {
		fmt.Fprintf(stdout, "%-30s backend=%-28s vector=%3d hardware=%-5t emulated=%-5t\n",
			entry.ID, entry.SelectedBackend, entry.VectorBits, entry.HardwareSIMD, entry.Emulated)
	}
	return 0
}

func parseMode(name string) (kernel.Mode, error) {
	switch name {
	case "scalar":
		return kernel.ModeScalar, nil
	case "auto":
		return kernel.ModeAuto, nil
	case "simd", "forced", "forced-simd":
		return kernel.ModeSIMD, nil
	default:
		return 0, fmt.Errorf("unknown mode %q", name)
	}
}
