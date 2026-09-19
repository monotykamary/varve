import assert from 'node:assert/strict';
import {readFileSync,readdirSync,mkdtempSync,rmSync,lstatSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {gunzipSync} from 'node:zlib';
import {execFileSync} from 'node:child_process';
const root=join(dirname(fileURLToPath(import.meta.url)),'..');
const read=path=>readFileSync(join(root,path));
const json=path=>JSON.parse(read(path));
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const ledger=new Map(read('SHA256SUMS').toString().trim().split('\n').map(line=>{const split=line.indexOf('  ');assert(split===64);return [line.slice(split+2),line.slice(0,split)];}));
function files(base,relative=''){return readdirSync(join(base,relative),{withFileTypes:true}).flatMap(entry=>{const path=join(relative,entry.name);assert(!entry.isSymbolicLink());return entry.isDirectory()?files(base,path):[path];}).sort();}
assert.deepEqual(files(root).filter(path=>path!=='SHA256SUMS'),[...ledger.keys()].sort());
for(const [path,sha]of ledger)assert.equal(hash(read(path)),sha,path);
const summary=json('summary.json'),qualification=json('local/qualification.json'),before=json('local/source-qualified.json'),after=json('local/source-qualified2.json'),stages=json('build/stages.json');
assert.equal(summary.source_sha256,qualification.source_sha256);assert.equal(summary.source_sha256,after.sha256);assert.equal(stages.source_sha256,after.sha256);
for(const source of [before,after])assert.equal(hash(JSON.stringify(source.files)),source.sha256);
assert.deepEqual(Object.keys(after.files),Object.keys(before.files));assert.deepEqual(Object.keys(after.files).filter(path=>after.files[path]!==before.files[path]),['src/engine.rs']);
for(const [file,sha]of Object.entries(qualification.evidence))assert.equal(hash(read('local/'+file)),sha,file);
assert.equal(qualification.targets.reduce((n,target)=>n+target.passed,0),268);assert(qualification.targets.every(target=>target.ok&&target.failed===0&&target.ignored===0));
assert(read('local/ROOT_WITNESS_REVIEW.md').toString().includes('B1 resolved'));
const temp=mkdtempSync(join(tmpdir(),'varve-evidence-'));
try{
 const archive=join(root,'build/qualified-varve-stage.tar.gz');
 const names=execFileSync('tar',['-tzf',archive],{encoding:'utf8'}).trim().split('\n');
 for(const name of names)assert(!name.startsWith('/')&&!name.split('/').includes('..'));
 execFileSync('tar',['-xzf',archive,'-C',temp]);
 const actual=Object.fromEntries(files(temp).map(path=>[path,hash(readFileSync(join(temp,path)))]));
 assert.deepEqual(actual,stages.stages.varve.files);assert.equal(hash(JSON.stringify(actual)),stages.stages.varve.sha256);
 const original=gunzipSync(read('local/test-refinement-before-engine.rs.gz')).toString();assert.equal(hash(original),before.files['src/engine.rs']);
 const current=readFileSync(join(temp,'src/engine.rs'),'utf8');assert.equal(hash(current),after.files['src/engine.rs']);
 function withoutTest(text){const start=text.indexOf('    #[test]\n    fn competing_same_sequence_root_stales_frozen_candidate_without_control_change()');assert(start>=0);const end=text.indexOf('    #[test]\n    fn capped_no_progress_retry_still_applies_full_terminal_duplicate_clock()',start);assert(end>start);return text.slice(0,start)+text.slice(end);}
 assert.equal(withoutTest(current),withoutTest(original));
}finally{rmSync(temp,{recursive:true,force:true});}
const driver=Object.fromEntries(files(join(root,'build/driver')).map(path=>[path,hash(read('build/driver/'+path))]));assert.deepEqual(driver,stages.stages.driver.files);
function validateSamples(value){if(!value||typeof value!=='object')return;if(Array.isArray(value.raw)&&value.summary){const sorted=[...value.raw].sort((a,b)=>a-b);assert(sorted.every(Number.isFinite));assert.equal(value.summary.samples,sorted.length);for(const [field,fraction]of [['p50_ms',0.5],['p95_ms',0.95],['p99_ms',0.99]])if(field in value.summary)assert.equal(value.summary[field],sorted.length?sorted[Math.max(0,Math.ceil(sorted.length*fraction)-1)]:null);}for(const nested of Object.values(value))validateSamples(nested);}
let total=0;
for(const trial of summary.trials){const report=json('reports/'+trial.run_id+'.json');assert.equal(report.state,'passed');assert.equal(report.configuration.query_samples,trial.query_samples);assert.equal(report.configuration.mixed_seconds,trial.mixed_seconds);assert.equal(report.oracles.global.count,trial.rows_per_backend);assert.equal(report.mixed_workload.offered_rows,report.mixed_workload.acknowledged_rows);assert.equal(report.mixed_workload.dropped_rows,0);assert.equal(report.mixed_workload.failed_or_ambiguous_rows,0);for(const role of ['varve','timescale'])assert.equal(report.initial_ingest[role].rows_per_second,trial.ingest_rows_per_second[role]);for(const [file,sha]of Object.entries(report.artifact.files))assert.equal(sha,driver[file]);validateSamples(report);total+=trial.rows_per_backend;}
assert.equal(total,1450000);assert.equal(summary.total_acknowledged_rows_per_backend,total);
const recoveryBefore=json('runtime/recovery-before.json'),recoveryAfter=json('runtime/recovery-after.json');assert(recoveryBefore.passed&&recoveryAfter.passed);assert.deepEqual(recoveryBefore.expected,recoveryAfter.expected);assert.deepEqual(recoveryAfter.varve,recoveryAfter.expected);assert.deepEqual(recoveryAfter.timescale,recoveryAfter.expected);assert.equal(recoveryAfter.status.database_id,recoveryBefore.status.database_id);assert.notEqual(recoveryAfter.postgres.started,recoveryBefore.postgres.started);assert.equal(recoveryAfter.receipts_per_backend,550);
const processIdentity=json('runtime/restart-identities.json');assert.notEqual(processIdentity.varve_process_before.start_ticks,processIdentity.varve_process_after.start_ticks);
const restart=json('reports/runtime_e9ihkn_r1.json');assert.equal(restart.state,'passed');assert.equal(restart.expected_watermark_rows,550000);
const stop=json('runtime/STOP_COMPLETE.json');assert(stop.passed&&stop.original_unchanged&&stop.services_and_volumes_preserved);assert(Object.values(stop.status).every(item=>item.status==='REMOVED'&&item.active===0));
console.log(JSON.stringify({passed:true,hash_bound_files:ledger.size,source_sha256:after.sha256,rust_tests:268,matched_trials:summary.trials.length,rows_per_backend:total,restart_rows_per_backend:550000,benchmark_compute_stopped:true}));
