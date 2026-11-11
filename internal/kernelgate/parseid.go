package kernelgate

import (
	"fmt"
	"strings"
)

// kernelFamilies are the KernelKind names as rendered by
// kernel.KernelKind.String; they double as the registry's family
// vocabulary.
var kernelFamilies = map[string]bool{
	"comparison":    true,
	"arithmetic":    true,
	"reduction":     true,
	"math-unary":    true,
	"math-binary":   true,
	"bitwise":       true,
	"boolean":       true,
	"hash":          true,
	"scale":         true,
	"string-length": true,
	"compact":       true,
	"string-match":  true,
}

// ParseKernelID splits a kernel.KernelID string (for example
// "compare-uint32-ge" or "string-match-prefix") into its family, data
// type, and operation. Type or operation are empty for kernels whose
// identity carries neither (for example "boolean-and").
func ParseKernelID(id string) (family, typ, op string, err error) {
	if id == "" {
		return "", "", "", fmt.Errorf("kernelgate: empty kernel id")
	}
	if rest, ok := strings.CutPrefix(id, "compare-"); ok {
		typ, op, ok = cutLast(rest)
		if !ok {
			return "", "", "", fmt.Errorf("kernelgate: malformed comparison id %q", id)
		}
		return "comparison", typ, op, nil
	}
	if rest, ok := strings.CutPrefix(id, "arithmetic-"); ok {
		typ, op, ok = cutLast(rest)
		if !ok {
			return "", "", "", fmt.Errorf("kernelgate: malformed arithmetic id %q", id)
		}
		return "arithmetic", typ, op, nil
	}
	if rest, ok := strings.CutPrefix(id, "minmax-"); ok {
		typ, op, ok = cutLast(rest)
		if !ok || op != "masked" {
			return "", "", "", fmt.Errorf("kernelgate: malformed masked min/max id %q", id)
		}
		switch typ {
		case "int8", "int16", "uint8", "uint16", "int32", "int64", "uint32", "uint64", "float32", "float64":
			return "reduction", typ, "minmax-masked", nil
		default:
			return "", "", "", fmt.Errorf("kernelgate: unsupported masked min/max type in %q", id)
		}
	}
	if rest, ok := strings.CutPrefix(id, "sum-"); ok {
		typ, op, ok = cutLast(rest)
		if !ok || op != "masked" {
			return "", "", "", fmt.Errorf("kernelgate: malformed masked sum id %q", id)
		}
		switch typ {
		case "int8", "int16", "uint8", "uint16", "int32", "uint32", "int64", "uint64":
			return "reduction", typ, "sum-masked", nil
		default:
			return "", "", "", fmt.Errorf("kernelgate: unsupported masked sum type in %q", id)
		}
	}
	if rest, ok := strings.CutPrefix(id, "bitwise-"); ok {
		typ, op, ok = cutLast(rest)
		if !ok {
			return "", "", "", fmt.Errorf("kernelgate: malformed bitwise id %q", id)
		}
		switch typ {
		case "int32", "uint32", "int64", "uint64":
		default:
			return "", "", "", fmt.Errorf("kernelgate: unsupported bitwise type in %q", id)
		}
		switch op {
		case "and", "or", "xor", "not":
			return "bitwise", typ, op, nil
		default:
			return "", "", "", fmt.Errorf("kernelgate: unknown bitwise operation in %q", id)
		}
	}
	if rest, ok := strings.CutPrefix(id, "math-"); ok {
		typ, op, ok = strings.Cut(rest, "-")
		if !ok || typ == "" || op == "" {
			return "", "", "", fmt.Errorf("kernelgate: malformed math id %q", id)
		}
		switch op {
		case "abs", "round", "floor", "ceil", "sqrt", "log":
			return "math-unary", typ, op, nil
		case "mod", "floor-div", "pow":
			return "math-binary", typ, op, nil
		default:
			return "", "", "", fmt.Errorf("kernelgate: unknown math operation in %q", id)
		}
	}
	if rest, ok := strings.CutPrefix(id, "boolean-"); ok && rest != "" {
		return "boolean", "", rest, nil
	}
	if rest, ok := strings.CutPrefix(id, "string-match-"); ok && rest != "" {
		return "string-match", "string", rest, nil
	}
	if rest, ok := strings.CutPrefix(id, "compact-"); ok && rest != "" {
		return "compact", rest, "", nil
	}
	switch id {
	case "hash64-batch":
		return "hash", "uint64", "batch", nil
	case "hash128-batch":
		return "hash", "uint64", "batch128", nil
	case "int64-scale":
		return "scale", "int64", "scale", nil
	case "string-byte-length":
		return "string-length", "string", "byte-length", nil
	case "string-utf8-length":
		return "string-length", "string", "utf8-length", nil
	}
	return "", "", "", fmt.Errorf("kernelgate: unrecognized kernel id %q", id)
}

// cutLast splits s at its final dash, requiring both halves non-empty.
func cutLast(s string) (head, tail string, ok bool) {
	i := strings.LastIndexByte(s, '-')
	if i <= 0 || i == len(s)-1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

// ValidFamily reports whether name is a known kernel family.
func ValidFamily(name string) bool { return kernelFamilies[name] }
