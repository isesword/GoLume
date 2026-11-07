#!/usr/bin/env python3
"""Generate observed Polars 1.35.2 numeric quantile semantics, not GoLume policy."""
import argparse
import json
import math
from pathlib import Path
import polars as pl

assert pl.__version__ == '1.35.2', pl.__version__
MODES = ['nearest', 'lower', 'higher', 'midpoint', 'linear', 'equiprobable']
TYPES = ['Int8','Int16','Int32','Int64','UInt8','UInt16','UInt32','UInt64','Float32','Float64']
def encode(v):
    if isinstance(v, float) and not math.isfinite(v):
        return 'NaN' if math.isnan(v) else '+Inf' if v > 0 else '-Inf'
    return v
cases=[]
for dtype in TYPES:
    fixtures=[[],[None],[1],[None,1,3,None],[1,1,3,7],[0,1,2,3]]
    if dtype.startswith('Float'):
        fixtures += [[float('nan'),float('nan'),None,2], [float('-inf'),float('inf')], [float('inf'),float('inf')], [1,float('inf')],[-0.,0.]]
    if dtype=='Int64': fixtures += [[2**63-3,2**63-2,2**63-1],[-2**63,2**63-1]]
    if dtype=='UInt64': fixtures += [[2**64-3,2**64-2,2**64-1],[0,2**64-1]]
    for values in fixtures:
        frame=pl.DataFrame({'x':pl.Series(values,dtype=getattr(pl,dtype))})
        for q in [0.,.1,.25,.5,.75,1.]:
            for mode in MODES:
                result=frame.select(pl.col('x').quantile(q,interpolation=mode)).to_series()
                cases.append(dict(dtype=dtype,values=[encode(x) for x in values],q=q,mode=mode,result=encode(result.item()),output_dtype=str(result.dtype)))
        result=frame.select(pl.col('x').median()).to_series()
        cases.append(dict(dtype=dtype,values=[encode(x) for x in values],q=.5,mode='median',result=encode(result.item()),output_dtype=str(result.dtype)))
path=Path(__file__).resolve().parents[1]/'testdata/fundamentals/quantile-polars-1.35.2.json'
parser=argparse.ArgumentParser()
parser.add_argument('--check',action='store_true')
args=parser.parse_args()
payload=json.dumps(dict(version=pl.__version__,cases=cases),indent=2,allow_nan=False)+'\n'
if args.check:
    if path.read_text()!=payload:
        raise SystemExit('quantile oracle differs; regenerate explicitly')
else:
    path.write_text(payload)
print(f'{len(cases)} cases: {path}')
