import importlib.util,pathlib,tempfile,unittest,json,sys,argparse
from unittest.mock import patch
p=pathlib.Path(__file__).with_name('item27_manual_regression.py')
s=importlib.util.spec_from_file_location('runner',p);r=importlib.util.module_from_spec(s);s.loader.exec_module(r)
class Contract(unittest.TestCase):
 def fp(self):return dict(source_manifest_sha256='manifest',go_sha256='go',binary_sha256='same',toolchain='go1.27.3',build_env={'GOEXPERIMENT':'simd','mode':'auto'},harness_sha256={'h':'x'},host={'cpu':'same'})
 def samples(self,ratio=1):return [dict(cell=c,round=n,arm=a,exit=0,ns=100*(ratio if a=='B' else 1),bytes=40,allocs=2,peak=1000) for c in r.CELLS for n in range(1,7) for a in ('A','B')]
 def check(self,s,expected,kind='AA',**kw):self.assertEqual(r.classify(s,self.fp(),self.fp(),kind,**kw)['status'],expected)
 def test_aa_pass(self):self.check(self.samples(),'PASS')
 def test_ab_regression(self):self.check(self.samples(1.05),'REGRESSION','AB')
 def test_aa_bias(self):self.check(self.samples(1.05),'INCONCLUSIVE')
 def test_aa_fast_bias(self):self.check(self.samples(.90),'INCONCLUSIVE')
 def test_missing(self):self.check(self.samples()[:-1],'INCONCLUSIVE')
 def test_extra(self):self.check(self.samples()+self.samples()[:1],'INCONCLUSIVE')
 def test_mutated_identity(self):self.check(self.samples(),'INCONCLUSIVE',identity_ok=False)
 def test_noise(self):self.check(self.samples(),'INCONCLUSIVE',noisy=True)
 def test_cross_ci(self):
  s=self.samples()
  for x in s:
   if x['arm']=='B':x['ns'] = 90 if x['round']%2 else 120
  self.check(s,'INCONCLUSIVE')
 def test_resource_growth(self):
  for metric in ('bytes','allocs','peak'):
   s=self.samples();s[1][metric]+=1;self.check(s,'INCONCLUSIVE')
 def test_mismatched_configuration(self):
  for k in r.BUILD_KEYS:
   a=self.fp();b=self.fp();b[k]='different'
   self.assertEqual(r.classify(self.samples(),a,b)['status'],'INCONCLUSIVE')
 def test_failed_sample(self):
  s=self.samples();s[0]['exit']=1;self.check(s,'INCONCLUSIVE')
 def test_real_binary_mutation(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);binary=root/'binary';binary.write_bytes(b'a');manifest=root/'m.json'
   h=root/'engine/global_binding_benchmark_test.go';h.parent.mkdir();h.write_text('fixture');manifest.write_text(json.dumps({'engine/global_binding_benchmark_test.go':r.sha(h)}))
   fp=self.fp();fp.update(binary=str(binary),source_manifest=str(manifest),source_root=d,source_manifest_sha256=r.sha(manifest),binary_sha256=r.sha(binary),harness_sha256={'engine/global_binding_benchmark_test.go':r.sha(h)},host=r.observe_host(),go_path=sys.executable,go_sha256=r.sha(sys.executable))
   receipt=root/'receipt.json';receipt.write_text(json.dumps({**fp,'command':[sys.executable,'test','-c']}));fp.update(build_receipt=str(receipt),build_receipt_sha256=r.sha(receipt))
   self.assertTrue(r.verify(fp));binary.write_bytes(b'b')
   with self.assertRaisesRegex(ValueError,'mutated binary'):r.verify(fp)
 def test_stale_host(self):
  with self.assertRaisesRegex(ValueError,'stale host'):r.verify({'host':{'stale':True}})
 def test_build_info(self):
  fp=self.fp();fp['toolchain']='go1.27.3';fp['build_env']={'GOEXPERIMENT':'simd','GOAMD64':'v1','GOOS':'linux','GOARCH':'amd64','CGO_ENABLED':'1'}
  text='binary: go1.27.3\n'+''.join('\tbuild\t'+k+'='+v+'\n' for k,v in fp['build_env'].items())
  r.check_build_info(text,fp)
  with self.assertRaises(ValueError):r.check_build_info(text.replace('simd','none'),fp)
 def test_actual_flags_comparison(self):
  fp=self.fp();fp['toolchain']='go1.27.3';fp['build_env']={'GOEXPERIMENT':'simd','GOAMD64':'v1','GOOS':'linux','GOARCH':'amd64','CGO_ENABLED':'1'}
  base='binary: go1.27.3\n'+''.join('\tbuild\t'+k+'='+v+'\n' for k,v in fp['build_env'].items())
  parse=lambda extra:r.check_build_info(base+extra,fp)
  for left,right in [('-race=true','-race=false'),('-gcflags=all=-N -l','-gcflags=all=-l'),('', '-newflag=true')]:
   a=parse(('\tbuild\t'+left+'\n') if left else '')
   b=parse('\tbuild\t'+right+'\n')
   self.assertFalse(r.comparable_actual_settings(a,b))
  a=parse('\tbuild\t-gcflags=all=-N -l\n\tbuild\tvcs.revision=old\n\tbuild\tvcs.time=old\n\tbuild\tvcs.modified=false\n')
  b=parse('\tbuild\t-gcflags=all=-N -l\n\tbuild\tvcs.revision=new\n\tbuild\tvcs.time=new\n\tbuild\tvcs.modified=true\n')
  self.assertEqual(a['-gcflags'],'all=-N -l')
  self.assertTrue(r.comparable_actual_settings(a,b))
  b['vcs']='other';self.assertFalse(r.comparable_actual_settings(a,b))
 def test_gate_bool_cannot_substitute(self):
  with self.assertRaises(ValueError):r.check_gate('gates_ok=true\nPASS\n')
  names=['TestItem7AvgBatchBudget','TestItem7AvgBatchBudget/nonavg','TestItem7AvgBatchBudget/nonavg/exact','TestItem7AvgBatchBudget/nonavg/minusone']
  r.check_gate(''.join('--- PASS: '+n+' (0.01s)\n' for n in names)+'PASS\n')
 def test_aa_manifest(self):
  a=self.fp();b=self.fp();b['source_manifest_sha256']='other'
  self.assertEqual(r.classify(self.samples(),a,b)['status'],'INCONCLUSIVE')
 def test_low_ci_cross(self):
  s=self.samples()
  for x in s:
   if x['arm']=='B':x['ns']=94 if x['round']%2 else 100
  self.check(s,'INCONCLUSIVE')
 def test_missing_input_saved(self):
  with tempfile.TemporaryDirectory() as d:
   out=pathlib.Path(d)/'out'
   code=r.run(argparse.Namespace(out=str(out),a=d+'/missing',b=d+'/missing2',kind='AA',noisy=False))
   self.assertEqual(code,2);self.assertEqual(json.loads((out/'result.json').read_text())['status'],'INCONCLUSIVE')
 def test_timeout_retains_raw(self):
  with tempfile.TemporaryDirectory() as d:
   prefix=pathlib.Path(d)/'timeout'
   with self.assertRaises(ValueError):r.capture([sys.executable,'-u','-c','import time;print("partial");time.sleep(3)'],prefix,timeout=.2)
   self.assertIn('partial',prefix.with_suffix('.log').read_text())
 def test_actual_benchmark_formats(self):
  cell=r.CELLS[0]
  for suffix in ('','-1'):
   text=f'BenchmarkGlobalBindingWholeQuery/{cell}{suffix} 10 100 ns/op 1000 peak-charged-B 40 B/op 2 allocs/op\nPASS\n'
   self.assertEqual(r.parse(text,cell)['ns'],100)
  with self.assertRaises(ValueError):r.parse(text.replace('-1 ','-2 '),cell)
 def test_unsafe_alias_rejected_before_read(self):
  with patch.object(pathlib.Path,'read_bytes',side_effect=AssertionError('must not read')):
   for name in ('./lazy/phase28_test.go','lazy//phase28_test.go','/tmp/file','../file'):
    with self.assertRaises(ValueError):r.safe_relative(name)
 def test_parse_extra(self):
  cell=r.CELLS[0];line=f'BenchmarkGlobalBindingWholeQuery/{cell}-1 10 100 ns/op 1000 peak-charged-B 40 B/op 2 allocs/op\n'
  self.assertEqual(r.parse(line+'\nPASS\n',cell)['ns'],100)
  with self.assertRaises(ValueError):r.parse(line+line+'\nPASS\n',cell)
if __name__=='__main__':unittest.main()
