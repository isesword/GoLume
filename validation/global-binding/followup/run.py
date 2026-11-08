#!/usr/bin/env python3
import datetime,json,os,pathlib,re,subprocess,sys
root=pathlib.Path(__file__).resolve().parent
bins=pathlib.Path(sys.argv[1])
cases=json.loads((root/'cases.json').read_text())
env=dict(os.environ,GOMAXPROCS='1')
assert not (root/'order.tsv').exists(), 'Refusing to overwrite follow-up measurements'
(root/'method.txt').write_text('Six alternating pairs, 1 s target per case. Each case runs before/after consecutively, with AB/BA reversed by round. All five apparent >5% regressions in the initial matrix are included, plus the large two/four-lane dense and largest four-lane nullable headline cases. No outliers are discarded. Binaries and harness are the same as the initial matrix.\n')
with (root/'order.tsv').open('w') as order:
 order.write('round\tposition\tvariant\tcase\tstarted_utc\n')
 for round_ in range(1,7):
  labels=['before','after'] if round_%2 else ['after','before']
  sequence=cases if round_%2 else list(reversed(cases))
  for case in sequence:
   pattern='/'.join('^'+re.escape(part)+'$' for part in case.split('/'))
   for position,label in enumerate(labels,1):
    order.write(f'{round_}\t{position}\t{label}\t{case}\t{datetime.datetime.now(datetime.timezone.utc).isoformat()}\n');order.flush()
    with (root/f'{label}-round-{round_}.txt').open('a') as out:
     subprocess.run([str(bins/(label+'.engine.test')),'-test.run','^$','-test.bench',pattern,'-test.benchtime','1s','-test.benchmem','-test.count','1'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
(root/'complete.txt').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n')
