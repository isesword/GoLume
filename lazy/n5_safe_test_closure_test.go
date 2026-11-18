//go:build n5_safe_explicit

package lazy

// Exact helper from the permitted result_terminal_test.go. Keeping the helper
// here lets the explicit sink suite include unchanged analyze_stream_test.go
// without importing unrelated result/window test dependencies. Named-file test
// commands include this file explicitly; normal package discovery excludes it
// so the original helper declaration cannot collide.
type resultCloseFunc func() error

func (f resultCloseFunc) Close() error { return f() }
