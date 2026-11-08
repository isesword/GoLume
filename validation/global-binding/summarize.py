#!/usr/bin/env python3
"""Summarize alternating frozen-binary samples without third-party packages."""
import csv, glob, json, math, pathlib, re, statistics, sys
root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
if root.is_file(): root = root.parent
samples = {}
for label in ('before','after'):
    samples[label] = {}
    for file in sorted(root.glob(label+'-round-*.txt')):
        round_ = int(re.search(r'round-(\d+)',file.name)[1])
        for line in file.read_text().splitlines():
            if not line.startswith('Benchmark') or 'ns/op' not in line: continue
            parts = line.split(); name = re.sub(r'-\d+$','',parts[0])
            metrics = {parts[i+1]: float(parts[i]) for i in range(2,len(parts)-1,2)}
            samples[label].setdefault(name,{})[round_] = metrics
if not samples['before'] or set(samples['before']) != set(samples['after']):
    raise SystemExit('Missing or mismatched before/after benchmarks')
rows=[]
for name in samples['before']:
    a,b = samples['before'][name],samples['after'][name]
    if set(a)!=set(b): raise SystemExit('Mismatched rounds: '+name)
    rounds=sorted(a)
    get=lambda s,k: [s[r][k] for r in rounds]
    med=lambda s,k: statistics.median(get(s,k))
    before,after=med(a,'ns/op'),med(b,'ns/op')
    ratios=[b[r]['ns/op']/a[r]['ns/op'] for r in rounds]
    logs=[math.log(x) for x in ratios]
    # Exact paired sign-flip test: descriptive evidence, not a multiple-test
    # adjusted acceptance gate. Six pairs have a minimum two-sided p=0.03125.
    observed=abs(sum(logs))
    permutations=[abs(sum(v if mask&(1<<i) else -v for i,v in enumerate(logs))) for mask in range(1<<len(logs))]
    p=sum(x>=observed-1e-12 for x in permutations)/len(permutations)
    row={'benchmark':name,'pairs':len(rounds),'before_ns':before,'after_ns':after,
         'change_pct':100*(after/before-1),'speedup':before/after,
         'paired_ratio_geomean':math.exp(statistics.mean(logs)),'paired_ratio_min':min(ratios),'paired_ratio_max':max(ratios),
         'paired_signflip_p':p,'before_B':med(a,'B/op'),'after_B':med(b,'B/op'),
         'delta_B':med(b,'B/op')-med(a,'B/op'),'before_allocs':med(a,'allocs/op'),'after_allocs':med(b,'allocs/op'),
         'delta_allocs':med(b,'allocs/op')-med(a,'allocs/op')}
    if 'peak-charged-B' in a[rounds[0]]:
        row.update(before_peak=med(a,'peak-charged-B'),after_peak=med(b,'peak-charged-B'),delta_peak=med(b,'peak-charged-B')-med(a,'peak-charged-B'))
    rows.append(row)
fields=list(rows[0])
for row in rows:
    for key in row:
        if key not in fields: fields.append(key)
with (root/'paired-summary.csv').open('w') as f:
    writer=csv.DictWriter(f,fields); writer.writeheader(); writer.writerows(rows)
(root/'paired-analysis.json').write_text(json.dumps({'method':'Median ns/op; exact paired log-ratio sign-flip p is descriptive and unadjusted. No broad speedup claim from controls or microbenchmarks.','samples':samples,'summary':rows},indent=2)+'\n')
with (root/'paired-summary.txt').open('w') as f:
    for row in rows:
        f.write('{benchmark}: {before_ns:,.0f} -> {after_ns:,.0f} ns/op ({change_pct:+.1f}%, {speedup:.2f}x); heap {before_B:,.0f} -> {after_B:,.0f} B/op ({delta_B:+.0f}); allocs {before_allocs:g} -> {after_allocs:g} ({delta_allocs:+g})'.format(**row))
        if 'delta_peak' in row: f.write('; peak {before_peak:g} -> {after_peak:g} ({delta_peak:+g})'.format(**row))
        f.write('\n')
print('Summarized',len(rows),'workloads:',root/'paired-summary.csv')
for row in rows:
    if row['change_pct']>5:
        print('Follow-up candidate: {benchmark} ({change_pct:+.1f}%; pairs {paired_ratio_min:.3f}..{paired_ratio_max:.3f})'.format(**row))
