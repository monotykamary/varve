import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import assert from 'node:assert/strict';
const work='/tmp/varve-runtime.E9ihKN';
const root='/Users/monotykamary/VCS/working-remote/open-source/varve';
const read=name=>readFileSync(work+'/'+name,'utf8');
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
assert.equal(read('runtime-final.exit').trim(),'0');assert.equal(read('final.exit').trim(),'0');
execFileSync('node',[work+'/freeze.mjs','assert','qualified2'],{stdio:'pipe'});
const targets=[];let active;
for(const line of read('runtime-final.log').split('\n')){
  const start=line.match(/^\s*Running (.+?) \((.+)\)$/);
  if(start){active={target:start[1],binary:start[2]};targets.push(active);}
  const result=line.match(/^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;/);
  if(result&&active)Object.assign(active,{passed:+result[2],failed:+result[3],ignored:+result[4],ok:result[1]==='ok'});
}
assert.equal(targets.length,14,'expected library plus thirteen integration targets');
assert(targets.every(target=>target.ok&&target.failed===0&&target.ignored===0&&target.passed>0));
for(const name of ['default-http.log','default-restart.log'])assert(read(name).includes('test result: ok. 1 passed; 0 failed;'));
for(const name of ['clippy-all.log','clippy-default.log'])assert(read(name).includes('Finished `dev` profile'));
assert(read('driver-tests-qualified.log').includes('Ran 21 tests'));assert(read('driver-tests-qualified.log').trim().endsWith('OK'));
const review=read('WRITE_FIX_REVIEW.md');assert(review.includes('no demonstrated production-code defect'),'production review not accepted');
assert(/B1 resolved/i.test(read('ROOT_WITNESS_REVIEW.md')),'root witness review still blocked');
assert(read('root-only-final.log').includes('test result: ok. 1 passed; 0 failed;'));
assert(read('clippy-root-test.log').includes('Finished `dev` profile'));
const baseline=JSON.parse(read('source-qualified.json'));
const source=JSON.parse(read('source-qualified2.json'));
const changed=Object.keys(source.files).filter(path=>source.files[path]!==baseline.files[path]);
assert.deepEqual(Object.keys(source.files),Object.keys(baseline.files));assert.deepEqual(changed,['src/engine.rs']);
const testName='competing_same_sequence_root_stales_frozen_candidate_without_control_change';
function withoutRefinedTest(text){const start=text.indexOf('    #[test]\n    fn '+testName+'()');assert(start>=0);const end=text.indexOf('    #[test]\n    fn capped_no_progress_retry_still_applies_full_terminal_duplicate_clock()',start);assert(end>start);return text.slice(0,start)+text.slice(end);}
const oldEngine=readFileSync(work+'/write-fix/after/src/engine.rs','utf8');
assert.equal(hash(oldEngine),baseline.files['src/engine.rs']);
assert.equal(withoutRefinedTest(readFileSync(root+'/src/engine.rs','utf8')),withoutRefinedTest(oldEngine),'non-test change after full runtime gate');
const evidence=['runtime-final.log','clippy-all.log','clippy-default.log','fmt.log','default-build.log','default-http.log','default-restart.log','driver-tests-qualified.log','source-gate.log','WRITE_FIX_REVIEW.md','ROOT_WITNESS_REVIEW.md','root-only-final.log','clippy-root-test.log'];
const receipt={passed:true,source_sha256:source.sha256,runtime_baseline_source_sha256:baseline.sha256,test_only_refinement:{file:"src/engine.rs",test:testName,focused_passes:1,production_and_all_other_files_byte_identical:true},rust_tests:targets.reduce((total,target)=>total+target.passed,0),targets,default_binary_smokes:2,driver_tests:21,default_binary_sha256:hash(readFileSync(root+'/target/debug/varve')),toolchain:execFileSync('rustc',['-Vv'],{encoding:'utf8'}).trim(),evidence:Object.fromEntries(evidence.map(name=>[name,hash(readFileSync(work+'/'+name))])),claim:'Selected local runtime/fault correctness, not performance, power-cut, distributed or production certification'};
writeFileSync(work+'/qualification.json',JSON.stringify(receipt,null,2)+'\n',{flag:'wx',mode:0o600});
console.log(JSON.stringify(receipt,null,2));
