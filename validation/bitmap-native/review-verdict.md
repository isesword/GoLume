# Independent comparison bitmap review

Verdict: no correctness or ISA-dispatch blockers found in reviewed production source. This is a focused safety review, not a performance or full-repository approval.

## Independent evidence

- Go 1.27.3 experimental SIMD, native amd64 AVX-512 host. An overlay adds reviewer-owned tests without editing production or repository tests.
- All 165 generated variants exercised directly: 7 literal types and 4 column types, 5 comparisons, widths 128/256/512. This direct invocation deliberately avoids confusing an uninstalled narrower path with scalar fallback.
- Independent scalar-expression oracle covers lengths 0–130 and 255–259, signed/unsigned extrema, high-bit unsigned ordering, NaNs (including signaling encoding), infinities, negative zero, subnormal values, output sentinel preservation, and seven null patterns including asymmetric nil bitmaps.
- Exactly one matching row at every position 0–128 checks mask bit order and widening before shifts.
- The modified portable Int32/Uint32/Float32 column paths are separately invoked and checked against the same oracle.
- Runtime-width installation guards checked for scalar/auto/SIMD and widths 0/64/128/256/512/1024 using an independent expected predicate.
- Final tests pass for GODEBUG=simd=0, simd=128, simd=256, simd=512, cpu.avx2=off,simd=128, cpu.avx512f=off,simd=256, and cpu.avx=off,simd=0. Native tests correctly skip when emulated. Logs are review-guard-*.txt.
- Generator freshness check passed (review-generator-check.txt).
- Disassembly samples of Float32 GT, Int8 GT and Uint64 GE at all widths show VMOVMSKPS/PD or VPMOVMSKB at 128/256, and KMOV at 512; vector comparisons are present. See review-objdump.txt.

## Source review

Reviewed generator/main integration, generated amd64 functions, excluded-architecture build tag, installer order, and portable column32 null handling. The exact selected width and !simd.Emulated guards are present; AVX/AVX2/AVX512 feature gates match toolchain API requirements. Mask16 is excluded. The architecture installer runs after all portable comparison installers. Literal and column short inputs fall back before broadcast/load. Column length mismatch checks precede processing. Output words are cleared before all-null skips. Tail masking bounds output, and uint64(mask.ToBits()) widens before shifting. No type switches or runtime width switches occur inside generated vector loops.

Exact reviewed production SHA-256 hashes are in review-source-sha256.txt. Main generated amd64 hash: 11d40923284f6abd508a9398ceb9a21feed3458f3c3b19d72b64e19b73e8571b.

## Reproduction and limits

With Go 1.27.3 and dependencies available, from the repository root:
GOEXPERIMENT=simd go test -overlay=validation/bitmap-native/review-overlay.json -c ./kernel -o validation/bitmap-native/review-kernel.test
GODEBUG=simd=512 validation/bitmap-native/review-kernel.test -test.run '^TestReview' -test.v

The retained test source is review-overlay-test.go.txt, mapped by review-overlay.json to the virtual kernel/review_overlay_test.go path. The .txt suffix prevents discovery as a standalone package during go test ./.... The overlay currently contains checkout-local absolute paths; update its Replace mapping for another checkout. The compiled review-kernel.test binary is local evidence only and should not be published. Independent tests and logs are text artifacts.

No arm64 runtime claim: this review ran on amd64 only. Parent owns arm64 cross-compilation and the full test matrix. Feature masking on this host is not a substitute for testing physically older CPUs. No performance claim is made; parent owns isolated before/after and whole-query timing. Existing bitmap non-aliasing and in-range validity contracts are unchanged. Public numeric types and overflow policy were not modified in the reviewed implementation.
