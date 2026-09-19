import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const root=fileURLToPath(new URL('../',import.meta.url));
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const read=name=>JSON.parse(readFileSync(root+name,'utf8'));
const manifest=read('MANIFEST.json');
for(const file of manifest.files){assert(!file.path.startsWith('/')&&!file.path.split('/').includes('..'));const bytes=readFileSync(root+file.path);assert.equal(bytes.length,file.bytes);assert.equal(hash(bytes),file.sha256);}
const source=read('source-manifest.json');
assert.deepEqual(execFileSync('tar',['-tzf',root+'source.tar.gz'],{encoding:'utf8'}).trim().split('\n').sort(),[...source.files.map(f=>f.path),'SOURCE_MANIFEST.json'].sort());
for(const file of source.files){const bytes=execFileSync('tar',['-xOzf',root+'source.tar.gz','--',file.path],{maxBuffer:2*1024*1024});assert.equal(bytes.length,file.bytes);assert.equal(hash(bytes),file.sha256);}
const config=read('varve-config.json');assert.equal(config.query_retained_inputs,true);
const {query_retained_inputs,...priorConfig}=config;assert.deepEqual(priorConfig,read('previous-profile.json'));
assert.equal(read('campaign-complete.json').performance_win_claimed,false);
const runtime=read('runtime-varve.json');
assert.equal(hash(readFileSync(root+'source-manifest.json')),runtime.source_manifest_sha256);
assert.equal(hash(readFileSync(root+'varve-config.json')),runtime.config_sha256);
assert.equal(runtime.initial_root.format_version,2);assert.equal(runtime.initial_root.checkpoint_sequence,0);assert.equal(runtime.status.sequence,0);
for(const [name,expected]of Object.entries(read('runtime-driver.json').files))assert.equal(hash(readFileSync(root+'driver/'+name)),expected);
let distributions=0,samples=0,queries=0;
function latencies(value){if(!value||typeof value!=='object')return;if(Array.isArray(value.raw)&&value.summary){const values=[...value.raw].sort((a,b)=>a-b);assert(values.every(n=>Number.isFinite(n)&&n>=0));assert.equal(value.summary.samples,values.length);for(const [key,p]of [['p50_ms',.5],['p95_ms',.95],['p99_ms',.99]])assert.equal(value.summary[key],values.length?values[Math.ceil(values.length*p)-1]:null);assert.equal(value.summary.max_ms,values.length?values.at(-1):null);distributions++;samples+=values.length;}for(const child of Object.values(value))latencies(child);}
const baseline=read('resident001.json'),stress=read('resident002.json');
assert.equal(baseline.state,'passed');assert.equal(baseline.manifest.total_committed_watermark_rows,550000);
const mixed=baseline.mixed_workload;assert.equal(mixed.offered_rows,300000);assert.equal(mixed.acknowledged_rows,300000);assert.equal(mixed.dropped_rows,0);assert.equal(mixed.failed_or_ambiguous_rows,0);
for(const report of [baseline,stress])for(const stage of Object.values(report.query_stages))for(const backend of Object.values(stage.backends))for(const query of Object.values(backend)){assert.equal(query.state,'passed');assert.equal(query.verified_samples,query.latency_ms.raw.length);queries+=query.verified_samples;}
assert.equal(stress.state,'overloaded');assert.equal(stress.manifest.total_committed_watermark_rows,1512000);assert.equal(stress.mixed_workload.offered_rows,600000);assert.equal(stress.mixed_workload.acknowledged_rows,512000);assert.equal(stress.mixed_workload.dropped_rows,88000);assert.equal(stress.mixed_workload.failed_or_ambiguous_rows,0);
latencies(baseline);latencies(stress);
const before=read('metrics-before.json'),middle=read('metrics-after-baseline.json'),after=read('metrics-after-stress.json');
assert.equal(before.status.database_id,middle.status.database_id);assert.equal(before.status.database_id,after.status.database_id);
const parse=text=>Object.fromEntries(text.split('\n').filter(line=>line.startsWith('varve_query_workers_')).map(line=>line.split(' ').map((v,i)=>i?Number(v):v)));
const b=parse(before.metrics),m=parse(middle.metrics);assert.equal(m.varve_query_workers_spawned_total-b.varve_query_workers_spawned_total,21);assert.equal(m.varve_query_workers_reused_total-b.varve_query_workers_reused_total,560);
assert.equal(after.status.derived_working_bytes,0);assert(after.status.derived_resident_bytes*2+4*262144<=read('varve-config.json').derived_max_bytes);assert(after.memory_events.includes('oom_kill 0\n'));
assert.equal(hash(execFileSync('tar',['-xOzf',root+'source.tar.gz','--','SOURCE_MANIFEST.json'])),hash(readFileSync(root+'source-manifest.json')));
const last=parse(after.metrics);assert.equal(last.varve_query_workers_reused_total-m.varve_query_workers_reused_total,324);assert.equal(last.varve_query_workers_spawned_total-m.varve_query_workers_spawned_total,25);
const rb=read('recovery-before.json'),ra=read('recovery-after.json');assert(ra.verified_unchanged);assert.deepEqual(rb.fingerprints,ra.fingerprints);assert.notDeepEqual(rb.postgres_start,ra.postgres_start);
let retained=0;for(const [id,count]of [['resident001',550000],['resident002',1512000]]){const f=ra.fingerprints[id];assert.equal(f.expected_watermark_rows,count);assert.equal(f.varve_raw.count,count);assert.equal(f.timescale_raw.count,count);assert.deepEqual(f.varve_raw,f.timescale_raw);assert.deepEqual(f.varve_aggregate,f.timescale_aggregate);retained+=count;}assert.equal(retained,2062000);
for(const role of ['varve','timescale']){const resource=phase=>Object.fromEntries(readFileSync(root+'resources-'+role+'-'+phase+'.txt','utf8').trim().split('\n').map(line=>{const i=line.indexOf('=');return [line.slice(0,i),line.slice(i+1)];}));const pre=resource('pre-restart'),post=resource('after-restart');assert(pre.boot_id!==post.boot_id||pre.process_start_ticks!==post.process_start_ticks);assert.equal(post['cpu.max'],'200000 100000;');assert.equal(post['memory.max'],'1999998976;');assert(pre['memory.events'].includes('oom_kill 0;'));assert(post['memory.events'].includes('oom_kill 0;'));}
assert.equal(runtime.status.database_id,read('metrics-after-restart.json').status.database_id);
const local=read('local-qualification.json');assert.equal(hash(JSON.stringify(local.files)),local.source_digest);assert.equal(source.local_qualified_source,local.source_digest);assert.equal(local.rust_passed,341);assert.equal(local.ignored,1);for(const log of local.logs)assert.equal(hash(readFileSync(root+log.path)),log.sha256);for(const file of source.files){const qualified=local.files.find(f=>f.path===file.path);if(qualified)assert.equal(file.sha256,qualified.sha256);}
assert.deepEqual(execFileSync('tar',['-tzf',root+'local-qualified-source.tar.gz'],{encoding:'utf8'}).trim().split('\n').sort(),local.files.map(file=>file.path).sort());
for(const file of local.files){const bytes=execFileSync('tar',['-xOzf',root+'local-qualified-source.tar.gz','--',file.path],{maxBuffer:2*1024*1024});assert.equal(bytes.length,file.bytes);assert.equal(hash(bytes),file.sha256);}
const log=readFileSync(root+'local-qualification.log','utf8');assert(log.includes('ALL_REQUESTED_GATES_PASSED'));let tests=0;for(const line of log.split('\n'))if(line.startsWith('test result: ok.')&&line.includes('; 0 filtered out;'))tests+=Number(line.split(' ').filter(Boolean)[3]);assert.equal(tests,341);

const cleanup=read('cleanup-verified.json');assert(cleanup.original_unchanged&&cleanup.benchmark_volumes_retained);assert.equal(cleanup.active_benchmark_deployments,0);for(const deployment of Object.values(cleanup.deployments))assert.equal(deployment.status,'REMOVED');
console.log(JSON.stringify({checked_files:manifest.files.length,source_files:source.files.length,latency_distributions:distributions,raw_samples:samples,oracle_verified_timed_queries:queries,baseline:'passed',large:'overloaded',pool_reuses:884,actual_restart_qualified:true,retained_common_rows_per_backend:2062000,active_benchmark_deployments:0,scope:'artifact checksums, source receipts and stored arithmetic; no local DB load or timing'}));
