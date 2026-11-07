# Variable-width endpoint integration validation

String/binary First/Last is integrated with owned state, transactional row/merge
preflight, V5 spill, typed/columnar output and mixed exact-distinct tag 2.
Independent review found no blocking defect after adversarial retry, changed
codebook/filtered parallel provenance, ownership and framing checks. Mixed V5
codec fuzz ran 23,086 inputs. Regression tests are included in the stage.

The frozen source layers over reviewed numeric fills and exact distinct, without
concurrent quantile changes. full-stage-tests.txt records full scalar and actual
GOEXPERIMENT=simd suites, related SIMD race, vet, GoDoc, generated-kernel and
registry checks, and a Linux/arm64 SIMD cross-build. Worker logs are supplementary;
worker-simd.txt used a build tag rather than enabling the compiler experiment and
is not by itself evidence of SIMD execution. The coordinator's experiment-enabled
gates provide that check. ARM64 is compile-only, not runtime or performance proof.

benchmark.txt contains the raw three-sample descriptive measurements summarized
in docs/VARIABLE_ENDPOINTS_DESIGN.md. No parallel speedup or new SIMD dispatch
gate is claimed. Remote CI success is not implied by local validation.
