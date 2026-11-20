package memory

import "fmt"

// MaxRepeatedListDepth bounds checked transport to three repeated List axes.
// MaxNestedListDepth remains the legacy expression/sort/spill consumer limit.
const MaxRepeatedListDepth = 3

func repeatedListDepth(f Field, check func() error) (int, error) {
	return repeatedListDepthMinimum(f, check, 2)
}

func repeatedListDepthMinimum(f Field, check func() error, minimum int) (int, error) {
	depth := 0
	provenance := false
	for f.Type == TypeList {
		if err := recursiveCheck(check); err != nil {
			return 0, err
		}
		if depth >= MaxRepeatedListDepth || f.Name == "" || depth > 0 && f.Name != "item" || len(f.Children) != 1 || !structListEncoding(f.Encoding) || f.DictionaryDomain != "" || f.DecimalPrecision != 0 || f.DecimalScale != 0 {
			return 0, fmt.Errorf("invalid checked repeated List axis")
		}
		provenance = provenance || f.ArrowType != ""
		depth++
		f = f.Children[0]
	}
	if depth < minimum || f.Name != "item" || (f.Type != TypeInt64 && f.Type != TypeString) || len(f.Children) != 0 || !structListEncoding(f.Encoding) || f.DictionaryDomain != "" || f.DecimalPrecision != 0 || f.DecimalScale != 0 {
		return 0, fmt.Errorf("repeated List terminal must be native Int64 or String")
	}
	if depth > MaxNestedListDepth && (provenance || f.ArrowType != "") {
		return 0, fmt.Errorf("depth-three List transport requires canonical metadata")
	}
	return depth, recursiveCheck(check)
}

// ValidateRepeatedListDepth imposes a consumer-specific repeated-axis ceiling.
// It does not grant schema admission or expand arbitrary List/Struct mixing.
func ValidateRepeatedListDepth(fields []Field, max int) error {
	if max < 1 || max > MaxRepeatedListDepth {
		return fmt.Errorf("invalid repeated List consumer ceiling")
	}
	var walk func(Field, int, int) error
	walk = func(f Field, axis, structure int) error {
		if structure > MaxStructDepth+MaxRepeatedListDepth+1 {
			// This is an axis guard, not a new admission rule for opaque metadata.
			// Existing typed validators own outer-Struct depth validation.
			return nil
		}
		if f.ArrowType != "" {
			return nil
		}
		if f.Type == TypeList {
			axis++
			if axis > max {
				return fmt.Errorf("consumer supports at most %d repeated List axes", max)
			}
		} else {
			axis = 0
		}
		for _, c := range f.Children {
			if err := walk(c, axis, structure+1); err != nil {
				return err
			}
		}
		return nil
	}
	for _, f := range fields {
		if err := walk(f, 0, 0); err != nil {
			return err
		}
	}
	return nil
}
