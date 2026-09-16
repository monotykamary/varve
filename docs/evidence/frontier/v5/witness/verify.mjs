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
const runtime=read('runtime-varve.json');
assert.equal(hash(readFileSync(root+'source-manifest.json')),runtime.source_manifest_sha256);
assert.equal(hash(readFileSync(root+'varve-config.json')),runtime.config_sha256);
assert.equal(runtime.initial_root.format_version,2);assert.equal(runtime.initial_root.checkpoint_sequence,0);assert.equal(runtime.status.sequence,0);
for(const [name,expected]of Object.entries(read('runtime-driver.json').files))assert.equal(hash(readFileSync(root+'driver/'+name)),expected);
let distributions=0,samples=0,queries=0;
function latencies(value){if(!value||typeof value!=='object')return;if(Array.isArray(value.raw)&&value.summary){const values=[...value.raw].sort((a,b)=>a-b);assert(values.every(n=>Number.isFinite(n)&&n>=0));assert.equal(value.summary.samples,values.length);for(const [key,p]of [['p50_ms',.5],['p95_ms',.95],['p99_ms',.99]])assert.equal(value.summary[key],values.length?values[Math.ceil(values.length*p)-1]:null);assert.equal(value.summary.max_ms,values.length?values.at(-1):null);distributions++;samples+=values.length;}for(const child of Object.values(value))latencies(child);}
const baseline=read('reuse001.json'),stress=read('reuse002.json');
assert.equal(baseline.state,'passed');assert.equal(baseline.manifest.total_committed_watermark_rows,550000);
const mixed=baseline.mixed_workload;assert.equal(mixed.offered_rows,300000);assert.equal(mixed.acknowledged_rows,300000);assert.equal(mixed.dropped_rows,0);assert.equal(mixed.failed_or_ambiguous_rows,0);
for(const stage of Object.values(baseline.query_stages))for(const backend of Object.values(stage.backends))for(const query of Object.values(backend)){assert.equal(query.state,'passed');assert.equal(query.verified_samples,query.latency_ms.raw.length);queries+=query.verified_samples;}
assert.equal(stress.state,'failed');assert(stress.failure.includes('derived resident/working byte budget exceeded'));assert.equal(Object.keys(stress.query_stages??{}).length,0);
latencies(baseline);latencies(stress);
const before=read('metrics-before.json'),middle=read('metrics-after-baseline.json'),after=read('metrics-after-stress.json');
assert.equal(before.status.database_id,middle.status.database_id);assert.equal(before.status.database_id,after.status.database_id);
const parse=text=>Object.fromEntries(text.split('\n').filter(line=>line.startsWith('varve_query_workers_')).map(line=>line.split(' ').map((v,i)=>i?Number(v):v)));
const b=parse(before.metrics),m=parse(middle.metrics);assert.equal(m.varve_query_workers_spawned_total-b.varve_query_workers_spawned_total,15);assert.equal(m.varve_query_workers_reused_total-b.varve_query_workers_reused_total,569);
assert.equal(after.status.derived_working_bytes,0);assert(after.status.derived_resident_bytes*2>read('varve-config.json').derived_max_bytes);assert(after.memory_events.includes('oom_kill 0\n'));
const cleanup=read('cleanup-verified.json');assert(cleanup.original_unchanged&&cleanup.benchmark_volumes_retained);assert.equal(cleanup.active_benchmark_deployments,0);for(const deployment of Object.values(cleanup.deployments))assert.equal(deployment.status,'REMOVED');
console.log(JSON.stringify({checked_files:manifest.files.length,source_files:source.files.length,latency_distributions:distributions,raw_samples:samples,oracle_verified_timed_queries:queries,baseline:'passed',large:'failed_derived_budget',pool_reuses:569,actual_restart_qualified:false,active_benchmark_deployments:0,scope:'artifact checksums, source receipts and stored arithmetic; no local DB load or timing'}));
