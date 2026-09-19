import {readFileSync,writeFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const root='/tmp/varve-scoped.0bj4bw',phase=process.argv[2];
assert(['baseline','stress'].includes(phase),'Specify baseline or stress');
const names=phase==='baseline'?['metrics-before.json','metrics-after-baseline.json']:['metrics-after-baseline.json','metrics-after-stress.json'];
const [before,after]=names.map(name=>JSON.parse(readFileSync(root+'/'+name,'utf8')));
assert.equal(before.status.database_id,after.status.database_id);
function parse(text){
 const phases={},workers={};
 for(const line of text.split('\n')){
  const match=line.match(/^varve_phase_duration_seconds_(count|sum)\{phase="([^"]+)"\} (.+)$/);
  if(match){const [,kind,name,value]=match;(phases[name]??={})[kind]=Number(value);}
  if(/^varve_(query_workers_|query_resident_|ingest_)/.test(line)){const split=line.lastIndexOf(' ');workers[line.slice(0,split)]=Number(line.slice(split+1));}
 }
 return {phases,workers};
}
const previous=parse(before.metrics),current=parse(after.metrics);
assert.equal(Object.keys(current.phases).length,30);
assert.deepEqual(Object.keys(previous.phases).sort(),Object.keys(current.phases).sort());
const phases=Object.entries(current.phases).map(([name,data])=>{
 const count=data.count-previous.phases[name].count,seconds=data.sum-previous.phases[name].sum;
 assert(Number.isSafeInteger(count)&&count>=0&&Number.isFinite(seconds)&&seconds>=0);
 return {phase:name,observations:count,cumulative_seconds:seconds,mean_ms:count?1000*seconds/count:null};
}).sort((a,b)=>b.cumulative_seconds-a.cumulative_seconds);
const workers=Object.fromEntries(Object.entries(current.workers).map(([key,value])=>{
 assert(Number.isFinite(value));
 const result=key.endsWith('_total')?value-previous.workers[key]:value;
 assert(Number.isFinite(result));if(key.endsWith('_total'))assert(result>=0,'Counter reset '+key);
 return [key,result];
}));
for(const name of ['varve_query_resident_dynamic_loads_total','varve_query_resident_dynamic_staged_bytes_total'])assert(name in workers);
const result={scope:'Whole workload interval including boundary delay and idle maintenance. Timers overlap; do not add them as exclusive CPU/wall time. Gauges are end values, not peaks. Counter deltas do not isolate individual query patterns.',phase,before_at:before.at,after_at:after.at,observed_window_seconds:(Date.parse(after.at)-Date.parse(before.at))/1000,phases,workers,sizes:Object.fromEntries(['metadata_bytes','control_root_bytes','derived_encoded_bytes','derived_resident_bytes','derived_working_bytes'].map(key=>[key,after.status[key]])),memory_peak:after.memory_peak,memory_events:after.memory_events};
writeFileSync(root+'/analysis-'+phase+'.json',JSON.stringify(result,null,2)+'\n',{mode:0o600});
console.log(JSON.stringify(result));
