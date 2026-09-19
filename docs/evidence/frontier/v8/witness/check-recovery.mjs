import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const root='/tmp/varve-resident.xUy7GB';
const read=name=>JSON.parse(readFileSync(root+'/'+name,'utf8'));
const before=read('recovery-before.json'),after=read('recovery-after.json');
assert(after.verified_unchanged);assert.deepEqual(before.fingerprints,after.fingerprints);assert.notDeepEqual(before.postgres_start,after.postgres_start);
let rows=0;for(const id of ['resident001','resident002']){const report=read(id+'.json');assert(['passed','overloaded'].includes(report.state));assert.equal(report.mixed_workload.failed_or_ambiguous_rows,0);const count=report.manifest.total_committed_watermark_rows;assert.equal(after.fingerprints[id].expected_watermark_rows,count);rows+=count;}
for(const role of ['varve','timescale']){
 const parse=phase=>Object.fromEntries(readFileSync(root+'/resources-'+role+'-'+phase+'.txt','utf8').trim().split('\n').map(line=>{const i=line.indexOf('=');return [line.slice(0,i),line.slice(i+1)];}));
 const b=parse('pre-restart'),a=parse('after-restart');assert(b.boot_id!==a.boot_id||b.process_start_ticks!==a.process_start_ticks);assert.equal(a['cpu.max'],'200000 100000;');assert.equal(a['memory.max'],'1999998976;');assert(b['memory.events'].includes('oom_kill 0;'));assert(a['memory.events'].includes('oom_kill 0;'));
}
assert.equal(read('runtime-varve.json').status.database_id,read('metrics-after-restart.json').status.database_id);
assert.equal(read('metrics-after-restart.json').status.fenced,null);
console.log(JSON.stringify({actual_database_restarts:2,exact_common_rows_retained_per_backend:rows,raw_aggregate_timestamp_fingerprints_unchanged:true,varve_database_identity_unchanged:true,no_oom:true}));
