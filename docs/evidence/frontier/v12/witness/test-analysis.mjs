import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const source=readFileSync(root+'/analyze-phases.mjs','utf8');
const phases=[...readFileSync(repo+'/src/metrics.rs','utf8').matchAll(/Self::\w+ => "([^"]+)"/g)].map(m=>m[1]);
assert.equal(phases.length,31);assert.equal(phases.at(-1),'append_accounting');
for(const mode of ['valid','missing_phase','counter_reset','wrong_database']){
 const fixtures=[0,1].map(index=>({at:'2026-09-17T00:00:0'+index+'.000Z',status:{database_id:mode==='wrong_database'&&index?'other':'fixture',metadata_bytes:0,control_root_bytes:0,derived_encoded_bytes:0,derived_resident_bytes:0,derived_working_bytes:0},metrics:(mode==='missing_phase'?phases.slice(0,-1):phases).map(p=>'varve_phase_duration_seconds_count{phase="'+p+'"} '+(index+1)+'\nvarve_phase_duration_seconds_sum{phase="'+p+'"} '+(index+1)+'\n').join('')+'varve_query_resident_dynamic_loads_total '+(mode==='counter_reset'?2-index:index)+'\nvarve_query_resident_dynamic_staged_bytes_total '+index+'\n',memory_peak:0,memory_events:'oom_kill 0'}));
 const writes=[];let failure;
 try{vm.runInNewContext(source.replace(/^import .*;\n/gm,''),{assert,process:{argv:['node','analyze','baseline']},console:{log(){}},readFileSync(path){assert([root+'/metrics-before.json',root+'/metrics-after-baseline.json'].includes(path));return JSON.stringify(fixtures[path.endsWith('metrics-before.json')?0:1]);},writeFileSync(path,text){assert.equal(path,root+'/analysis-baseline.json');writes.push(JSON.parse(text));}},{timeout:1000});}catch(error){failure=error;}
 if(mode==='valid'){assert.equal(failure,undefined);assert.equal(writes[0].phases.length,31);assert.equal(writes[0].phases.find(p=>p.phase==='append_accounting').observations,1);assert.equal(writes[0].observed_window_seconds,1);}else{assert(failure,mode);assert.equal(writes.length,0);}
}
const result={cases:4,actual_analyzer_source_exercised:true,external_commands:0,infra_mutations:0,analyzer_sha256:createHash('sha256').update(source).digest('hex')};
writeFileSync(root+'/analysis-tests.json',JSON.stringify(result,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(result));
