#!/usr/bin/env python3
"""Regenerate/check the pinned reference; never used by normal Go tests.

Install polars==1.35.2, then run this script from any directory. Special IEEE
values use tagged JSON strings; null remains JSON null. Errors record their
class rather than unstable human-readable diagnostics.
"""
import argparse
import json
import math
from pathlib import Path
import polars as pl

VERSION = "1.35.2"
if pl.__version__ != VERSION:
    raise SystemExit(f"expected polars {VERSION}, found {pl.__version__}")


def encode(value):
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else ("+Inf" if value > 0 else "-Inf")
    if isinstance(value, list):
        return [encode(v) for v in value]
    if isinstance(value, dict):
        return {k: encode(v) for k, v in value.items()}
    return value


def evaluate(frame, expression):
    try:
        result = frame.select(expression.alias("result")).get_column("result")
        return {"dtype": str(result.dtype), "values": encode(result.to_list())}
    except Exception as error:
        return {"error": type(error).__name__}


cases = []
for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
              pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
    for label, values in (("empty", []), ("null", [None, None]),
                          ("mixed", [1, None, 3])):
        frame = pl.DataFrame({"v": pl.Series(values, dtype=dtype)})
        expressions = {"sum": pl.col("v").sum(), "count": pl.col("v").count(),
                       "len": pl.len(), "min": pl.col("v").min(),
                       "max": pl.col("v").max(), "avg": pl.col("v").mean(),
                       "var_ddof1": pl.col("v").var(ddof=1),
                       "std_ddof1": pl.col("v").std(ddof=1),
                       "first": pl.col("v").first(), "last": pl.col("v").last(),
                       "n_unique": pl.col("v").n_unique()}
        cases.append({"id": f"aggregate/{dtype}/{label}", "input": values,
                      "input_dtype": str(dtype),
                      "results": {k: evaluate(frame, v) for k, v in expressions.items()}})
for label, dtype, values in (
    ("nan", pl.Float64, [float("nan"), 1.0, None]),
    ("infinities", pl.Float64, [float("inf"), float("-inf")]),
    ("int64_overflow", pl.Int64, [(1 << 63) - 1, 1]),
    ("uint64_overflow", pl.UInt64, [(1 << 64) - 1, 1]),
):
    frame = pl.DataFrame({"v": pl.Series(values, dtype=dtype)})
    cases.append({"id": f"edge/{label}", "input": encode(values),
                  "input_dtype": str(dtype), "results": {
                      k: evaluate(frame, e) for k, e in {
                          "sum": pl.col("v").sum(), "avg": pl.col("v").mean(),
                          "min": pl.col("v").min(), "max": pl.col("v").max(),
                          "count": pl.col("v").count()}.items()}})
strings = ["0", "-1", "+1", " 1", "1 ", "1.5", "1e2", "NaN", "inf", "-inf",
           "", "+NaN", "-NaN", "+nan", "-nan", "1.", ".5", "+.5", "01e2", "-0", "nan", "NAN", "Inf", "Infinity", "infinity", "+Inf", "-Infinity",
           "1e999", "1e-999", "1e40", "0x1p2", "0x10", "1_000", "18446744073709551615", "18446744073709551616", None]
for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
              pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
    for value in strings:
        frame = pl.DataFrame({"v": pl.Series([value], dtype=pl.String)})
        cases.append({"id": f"cast/{dtype}/{value!r}", "input": [value],
                      "target_dtype": str(dtype), "results": {
                          "strict": evaluate(frame, pl.col("v").cast(dtype, strict=True)),
                          "try": evaluate(frame, pl.col("v").cast(dtype, strict=False))}})
frame = pl.DataFrame({"v": [-2.5, -1.5, -0.0, 0.0, 1.5, 2.5, None, float("nan"), float("inf")]})
cases.append({"id": "math/float64", "input": encode(frame["v"].to_list()),
              "results": {k: evaluate(frame, e) for k, e in {
                  "abs": pl.col("v").abs(), "round": pl.col("v").round(0),
                  "floor": pl.col("v").floor(), "ceil": pl.col("v").ceil(),
                  "sqrt": pl.col("v").sqrt(), "log": pl.col("v").log(),
                  "pow2": pl.col("v").pow(2), "mod2": pl.col("v") % 2,
                  "floordiv2": pl.col("v") // 2}.items()}})
frame = pl.DataFrame({"k": [None, None, 1, 1], "v": [None, None, 2, 4]}, schema={"k": pl.Int64, "v": pl.Int64})
cases.append({"id": "grouped/null_keys", "input": frame.to_dict(as_series=False),
              "results": encode(frame.group_by("k", maintain_order=True).agg(
                  pl.col("v").sum().alias("sum"), pl.col("v").count().alias("count"),
                  pl.col("v").mean().alias("avg"), pl.len().alias("len")
              ).to_dict(as_series=False))})
for label, dtype, values in (
    ("int64", pl.Int64, [-(1 << 63), -5, -1, 0, 1, 5, None]),
    ("float64", pl.Float64, [-5.0, -1.0, -0.0, 0.0, 1.0, 5.0, None]),
):
    frame = pl.DataFrame({"v": pl.Series(values, dtype=dtype)})
    expressions = {"abs": pl.col("v").abs(), "floor": pl.col("v").floor(),
                   "ceil": pl.col("v").ceil(), "pow_neg1": pl.col("v").pow(-1),
                   "pow_half": pl.col("v").pow(0.5)}
    for divisor in (-2, 0, 2):
        expressions[f"mod_{divisor}"] = pl.col("v") % divisor
        expressions[f"floordiv_{divisor}"] = pl.col("v") // divisor
    cases.append({"id": f"math/{label}_domains", "input": values,
                  "results": {k: evaluate(frame, e) for k, e in expressions.items()}})
for label, values in (("empty", []), ("singleton", [2.0]),
                      ("null_edges", [None, 1.0, 3.0, None]),
                      ("nan", [float("nan"), float("nan"), None, 2.0])):
    frame = pl.DataFrame({"v": pl.Series(values, dtype=pl.Float64)})
    expressions = {"first": pl.col("v").first(), "last": pl.col("v").last(),
                   "n_unique": pl.col("v").n_unique(), "median": pl.col("v").median(),
                   "shift1": pl.col("v").shift(1), "shift_neg1": pl.col("v").shift(-1),
                   "diff1": pl.col("v").diff(), "fill_null": pl.col("v").fill_null(0),
                   "fill_nan": pl.col("v").fill_nan(0)}
    for ddof in (0, 1, 2):
        expressions[f"var_ddof{ddof}"] = pl.col("v").var(ddof=ddof)
        expressions[f"std_ddof{ddof}"] = pl.col("v").std(ddof=ddof)
    for method in ("nearest", "lower", "higher", "midpoint", "linear", "equiprobable"):
        expressions[f"quantile_0.25_{method}"] = pl.col("v").quantile(0.25, interpolation=method)
    cases.append({"id": f"statistics/{label}", "input": encode(values),
                  "results": {k: evaluate(frame, e) for k, e in expressions.items()}})
frame = pl.DataFrame({"v": [-5.0, -0.0, 0.0, 5.0, float("inf"), float("-inf"), float("nan"), None]})
cases.append({"id": "math/infinite_divisors", "input": encode(frame["v"].to_list()),
              "results": {k: evaluate(frame, e) for k, e in {
                  "mod_posinf": pl.col("v") % float("inf"),
                  "mod_neginf": pl.col("v") % float("-inf"),
                  "floordiv_posinf": pl.col("v") // float("inf"),
                  "floordiv_neginf": pl.col("v") // float("-inf")}.items()}})
frame = pl.DataFrame({"v": [0]})
cases.append({"id": "math/overflow_and_large_remainder", "results": {
    k: evaluate(frame, e) for k, e in {
        "int64_min_floordiv_neg1": pl.lit(-(1 << 63), dtype=pl.Int64) // -1,
        "int64_min_mod_neg1": pl.lit(-(1 << 63), dtype=pl.Int64) % -1,
        "int64_pow63": pl.lit(2, dtype=pl.Int64).pow(63),
        "int64_pow64": pl.lit(2, dtype=pl.Int64).pow(64),
        "uint64_pow64": pl.lit(2, dtype=pl.UInt64).pow(64),
        "large_mod3": pl.lit(1e20) % 3,
        "negative_large_mod3": pl.lit(-1e20) % 3}.items()}})
frame = pl.DataFrame(schema={"k": pl.Int64, "v": pl.Int64})
cases.append({"id": "grouped/empty", "input": frame.to_dict(as_series=False),
              "results": frame.group_by("k").agg(pl.col("v").sum()).to_dict(as_series=False)})
result = {"oracle": "polars", "version": VERSION, "cases": cases}
text = json.dumps(result, indent=2, allow_nan=False) + "\n"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
path = Path(__file__).resolve().parents[1] / "testdata/fundamentals/polars-1.35.2.json"
if args.check:
    if path.read_text() != text:
        raise SystemExit("Polars reference changed")
    print(f"Verified {len(cases)} cases against Polars {VERSION}")
else:
    path.write_text(text)
    print(f"Wrote {len(cases)} cases to {path}")
