#!/usr/bin/env python3
"""Manual frozen-binary regression audit; never builds or promotes a baseline."""
import argparse, hashlib, json, os, pathlib, random, re, statistics, subprocess, platform

CELLS = ['global/lanes=4/nullable/rows=63/batch=63/mode=auto',
         'global/lanes=4/nullable/rows=262147/batch=262147/mode=auto']
PAIRS = 6
BUILD_KEYS = ('toolchain', 'go_sha256', 'build_env', 'harness_sha256', 'host')

def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def observe_host():
    cpu = ''
    path = pathlib.Path('/proc/cpuinfo')
    if path.exists():
        for line in path.read_text().splitlines():
            if line.startswith('model name'):
                cpu = line.split(':',1)[1].strip(); break
    return {'uname':dict(platform.uname()._asdict()), 'cpu_model':cpu,
            'affinity':sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None}

def capture(cmd, prefix, env=None, timeout=660):
    prefix.with_suffix('.command.json').write_text(json.dumps(cmd))
    with prefix.with_suffix('.log').open('wb') as log:
        try:
            result = subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=timeout)
            code = result.returncode
        except subprocess.TimeoutExpired:
            prefix.with_suffix('.status').write_text('TIMEOUT\n')
            raise ValueError('process timeout; partial raw log retained')
    prefix.with_suffix('.status').write_text(str(code)+'\n')
    text = prefix.with_suffix('.log').read_text(errors='replace')
    if code: raise ValueError('process failed; raw log retained')
    return text

def check_build_info(text, fp):
    first = text.splitlines()[0] if text else ''
    if not first.endswith(': '+fp['toolchain']): raise ValueError('actual binary toolchain mismatch')
    settings = {}
    for entry in re.findall(r'^\s*build\s+(\S.*)$', text, re.M):
        if '=' not in entry: raise ValueError('malformed actual build setting')
        key, value = entry.split('=', 1)
        if not key or key in settings: raise ValueError('duplicate/empty actual build key')
        settings[key] = value
    for k in ('GOEXPERIMENT','GOAMD64','GOOS','GOARCH','CGO_ENABLED'):
        if settings.get(k) != fp['build_env'].get(k) or k not in fp['build_env']:
            raise ValueError('actual build setting mismatch: '+k)
    return settings

def comparable_actual_settings(a, b):
    ignored = {'vcs.revision', 'vcs.time', 'vcs.modified'}
    return {k:v for k,v in a.items() if k not in ignored} == {k:v for k,v in b.items() if k not in ignored}

def check_gate(text):
    names = ('TestItem7AvgBatchBudget', 'TestItem7AvgBatchBudget/nonavg',
             'TestItem7AvgBatchBudget/nonavg/exact', 'TestItem7AvgBatchBudget/nonavg/minusone')
    if any(not re.search(r'^\s*--- PASS: '+re.escape(n)+r' \(',text,re.M) for n in names) or re.search(r'--- (SKIP|FAIL):',text) or not re.search(r'^PASS$',text,re.M):
        raise ValueError('resource gate missing/failed/skipped')

def safe_relative(name):
    p = pathlib.PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts or p.as_posix() == 'lazy/phase28_test.go' or p.as_posix() != name:
        raise ValueError('unsafe manifest entry')
    return p

def verify(fp):
    if observe_host() != fp['host']: raise ValueError('stale host fingerprint')
    if sha(fp['go_path']) != fp['go_sha256']: raise ValueError('mutated toolchain executable')
    if sha(fp['binary']) != fp['binary_sha256']:
        raise ValueError('mutated binary')
    if sha(fp['source_manifest']) != fp['source_manifest_sha256']:
        raise ValueError('mutated source manifest')
    if sha(fp['build_receipt']) != fp['build_receipt_sha256']: raise ValueError('mutated build receipt')
    receipt = json.loads(pathlib.Path(fp['build_receipt']).read_text())
    for k in ('binary_sha256','source_manifest_sha256','go_sha256','toolchain','build_env','harness_sha256'):
        if receipt.get(k) != fp.get(k): raise ValueError('build receipt identity mismatch: '+k)
    command = receipt.get('command',[])
    if not command or command[0] != fp['go_path'] or 'test' not in command or '-c' not in command:
        raise ValueError('missing actual compile command receipt')
    manifest = json.loads(pathlib.Path(fp['source_manifest']).read_text())
    if not manifest or 'engine/global_binding_benchmark_test.go' not in fp['harness_sha256'] or not fp.get('toolchain') or not fp.get('host'):
        raise ValueError('incomplete manifest/fingerprint')
    root = pathlib.Path(fp['source_root']).resolve()
    for name, expected in manifest.items():
        p = safe_relative(name)
        target = root.joinpath(*p.parts).resolve()
        if not target.is_relative_to(root) or sha(target) != expected:
            raise ValueError('source identity mismatch: ' + name)
    for name, expected in fp['harness_sha256'].items():
        if manifest.get(name) != expected:
            raise ValueError('harness not anchored in manifest')
    if fp['build_env'].get('GOEXPERIMENT') != 'simd' or fp['build_env'].get('mode') != 'auto':
        raise ValueError('requires SIMD build / Auto workload')
    return True

def compatible(a, b):
    return all(a.get(k) == b.get(k) for k in BUILD_KEYS)

def classify(samples, a, b, kind='AA', noisy=False, identity_ok=True, gates_ok=True):
    out = {'status': 'INCONCLUSIVE', 'cells': [], 'reasons': []}
    if not identity_ok or not gates_ok or not compatible(a, b) or noisy:
        out['reasons'].append('identity/configuration/gates/noise not comparable')
        return out
    if kind not in ('AA', 'AB') or (kind == 'AA' and (a.get('binary_sha256') != b.get('binary_sha256') or a.get('source_manifest_sha256') != b.get('source_manifest_sha256'))):
        out['reasons'].append('invalid comparison mode')
        return out
    expected = {(c, n, arm) for c in CELLS for n in range(1, PAIRS + 1) for arm in ('A', 'B')}
    keys = [(x['cell'], x['round'], x['arm']) for x in samples]
    if len(keys) != len(expected) or set(keys) != expected:
        out['reasons'].append('missing/extra/duplicate samples')
        return out
    lookup = dict(zip(keys, samples))
    if any(x.get('exit') != 0 or any(not isinstance(x.get(k), (int, float)) or not (0 <= x[k] < float('inf')) for k in ('ns','bytes','allocs','peak')) or x['ns'] == 0 for x in samples):
        out['reasons'].append('failed or invalid sample')
        return out
    regression = False
    uncertain = False
    for c in CELLS:
        ratios = [lookup[c,n,'B']['ns'] / lookup[c,n,'A']['ns'] for n in range(1, PAIRS+1)]
        rng = random.Random(20261119)
        boot = sorted(statistics.median(rng.choices(ratios, k=PAIRS)) for _ in range(100000))
        ci = [boot[2500], boot[97500]]
        growth = any(lookup[c,n,'B'][k] > lookup[c,n,'A'][k] for n in range(1,PAIRS+1) for k in ('bytes','allocs','peak'))
        out['cells'].append({'cell':c,'ratios':ratios,'median':statistics.median(ratios),'range':[min(ratios),max(ratios)],'CI95':ci,'resource_growth':growth})
        regression |= ci[0] > 1.03
        uncertain |= ci[1] > 1.03 or growth
        if kind == 'AA':
            uncertain |= ci[0] < .97 or ci[1] > 1.03
    if kind == 'AB' and regression:
        out['status'] = 'REGRESSION'
    elif not uncertain:
        out['status'] = 'PASS'
    else:
        out['reasons'].append('noise/CI threshold overlap or resource growth; no waiver')
    return out

LINE = re.compile(r'^BenchmarkGlobalBindingWholeQuery/(\S+)\s+(\d+)\s+(.+)$', re.M)
def parse(text, cell):
    matches = LINE.findall(text)
    if len(matches) != 1 or matches[0][0] not in (cell, cell+'-1') or '\nPASS\n' not in text:
        raise ValueError('unexpected benchmark output')
    tokens = matches[0][2].split()
    if len(tokens)%2: raise ValueError('invalid metric pairs')
    metrics = {tokens[i+1]:float(tokens[i]) for i in range(0,len(tokens),2)}
    return {k:metrics[v] for k,v in [('ns','ns/op'),('bytes','B/op'),('allocs','allocs/op'),('peak','peak-charged-B')]}

def run(args):
    out = pathlib.Path(args.out); out.mkdir(parents=True, exist_ok=False)
    result = {'status':'INCONCLUSIVE','reasons':['incomplete run']}
    samples = []
    try:
        (out/'request.json').write_text(json.dumps(vars(args)))
        a = json.loads(pathlib.Path(args.a).read_text()); b = json.loads(pathlib.Path(args.b).read_text())
        verify(a); verify(b)
        if not compatible(a,b): raise ValueError('incompatible fingerprints')
        if args.kind == 'AA' and (a['binary_sha256'] != b['binary_sha256'] or a['source_manifest_sha256'] != b['source_manifest_sha256']): raise ValueError('AA requires identical binary and source manifest')
        (out/'inputs.json').write_text(json.dumps({'A':a,'B':b,'script_sha256':sha(__file__),'kind':args.kind,'pairs':PAIRS,'benchtime':'150ms'},indent=2))
        env = {k:v for k,v in os.environ.items() if k in ('PATH','HOME','TMPDIR','TMP','TEMP','SYSTEMROOT')}
        env.update(GOMAXPROCS='1', GODEBUG=a['build_env'].get('GODEBUG',''))
        actual_settings = {}
        for arm, fp in (('A',a),('B',b)):
            infoenv = dict(env, GOTOOLCHAIN='local', GOENV='off', GOWORK='off', GOPROXY='off', GOSUMDB='off')
            info = capture([fp['go_path'],'version','-m',fp['binary']],out/(arm+'-buildinfo'),infoenv)
            actual_settings[arm] = check_build_info(info,fp)
            (out/(arm+'-actual-build-settings.json')).write_text(json.dumps(actual_settings[arm],indent=2))
            gate = capture([fp['binary'],'-test.run','^TestItem7AvgBatchBudget$/^nonavg$','-test.v','-test.count=1','-test.timeout=10m'],out/(arm+'-resource-gate'),env)
            check_gate(gate)
        if not comparable_actual_settings(actual_settings['A'],actual_settings['B']):
            raise ValueError('actual A/B build settings differ')
        for ci, cell in enumerate(CELLS):
            for n in range(1,PAIRS+1):
                for arm in (('A','B') if n%2 else ('B','A')):
                    fp = a if arm=='A' else b
                    verify(fp)
                    pattern = '^BenchmarkGlobalBindingWholeQuery$/' + '/'.join('^'+re.escape(p)+'$' for p in cell.split('/'))
                    cmd = [fp['binary'],'-test.run','^$','-test.bench',pattern,'-test.benchtime=150ms','-test.count=1','-test.benchmem','-test.timeout=10m']
                    prefix = out/f'cell{ci}-round{n}-{arm}'
                    text = capture(cmd,prefix,env)
                    samples.append({'cell':cell,'round':n,'arm':arm,'exit':0,**parse(text,cell)})
        verify(a); verify(b)
        result = classify(samples,a,b,args.kind,args.noisy)
    except Exception as exc:
        result = {'status':'INCONCLUSIVE','reasons':[str(exc)]}
    finally:
        (out/'samples.json').write_text(json.dumps(samples,indent=2))
        (out/'result.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    return {'PASS':0,'REGRESSION':1,'INCONCLUSIVE':2}[result['status']]

if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--a',required=True);p.add_argument('--b',required=True);p.add_argument('--out',required=True)
    p.add_argument('--kind',choices=['AA','AB'],required=True);p.add_argument('--noisy',action='store_true')
    raise SystemExit(run(p.parse_args()))
