import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot';
const before=JSON.parse(readFileSync(root+'/before.json','utf8'));
let source=readFileSync(root+'/stop-attempt.mjs','utf8').replace(/^import .*;\n/gm,'');
assert(source.includes('function api(query,variables={})'));
source=source.replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const ids={varve:'owned-varve',timescale:'owned-timescale',driver:'owned-driver'};
const cases=[
 {name:'completed normal cleanup',mutations:1},
 {name:'unfinished normal refused',unfinished:true,error:'Workload unfinished'},
 {name:'unfinished abort allowed',unfinished:true,aborting:true,mutations:1},
 {name:'foreign active refused',aborting:true,foreign:true,error:'Unexpected active deployment'},
 {name:'wrong scope refused',aborting:true,wrongScope:true,error:'Cleanup deployment ownership mismatch'},
 {name:'inactive failed build abort allowed',aborting:true,inactive:true,mutations:1},
 {name:'existing intent not retried',aborting:true,intent:true,error:'Stop already attempted'},
 {name:'unrecorded active deployment refused',aborting:true,unrecorded:true,error:'Unrecorded active deployment'},
];
for(const test of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){
  raw[role]=structuredClone(before[role].instance);
  raw[role].activeDeployments=test.inactive?[]:[{id:ids[role],status:'SUCCESS'}];
  raw[role+'Limits']=structuredClone(before[role].limits);
 }
 if(test.foreign)raw.varve.activeDeployments=[{id:'not-owned',status:'SUCCESS'}];
 const recorded=test.unrecorded?{timescale:ids.timescale}:{timescale:ids.timescale,driver:ids.driver};
 const owned=Object.fromEntries(Object.entries(ids).map(([role,id])=>[role,{id,status:test.inactive?'FAILED':'SUCCESS',serviceId:roles[role],environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}]));
 if(test.wrongScope)owned.varve.serviceId='not-owned-service';
 const mutations=[],writes=[];
 const sandbox={process:{argv:test.aborting?['node','stop','--abort']:['node','stop'],env:{}},console:{log(){}},
  existsSync(path){return path.endsWith('/cleanup-request.json')?Boolean(test.intent):path.endsWith('/redeployed.json');},
  readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/upload-varve.jsonl'))return JSON.stringify({deploymentId:ids.varve});if(path.endsWith('/redeployed.json'))return JSON.stringify({deployments:recorded});if(/\/acct00[12]\.json$/.test(path))return JSON.stringify({finished_at:test.unfinished?null:'finished'});throw Error('Unexpected fixture read '+path);},
  writeFileSync(path,text){writes.push([path,JSON.parse(text)]);},
  execFileSync(){throw Error('External execution forbidden in cleanup test');},
  api(query){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{'))return owned;if(query.startsWith('mutation{')){mutations.push(query);return Object.fromEntries(Object.keys(ids).map(role=>[role,true]));}throw Error('Unexpected mocked operation');}
 };
 let failure;try{vm.runInNewContext(source,sandbox,{timeout:1000});}catch(error){failure=error.message;}
 if(test.error){assert(failure?.includes(test.error),test.name+': '+failure);assert.equal(mutations.length,0);assert.equal(writes.length,0);}
 else{assert.equal(failure,undefined,test.name);assert.equal(mutations.length,test.mutations);assert.equal(writes.length,2);assert.equal(writes[0][1].aborting,Boolean(test.aborting));assert.equal(writes[0][1].volumes_retained,true);}
}
console.log(JSON.stringify({cleanup_guard_cases:cases.length,passed:true,external_commands:0,infra_mutations:0,scope:'VM fixtures execute the actual cleanup source with mocked I/O; not live Railway evidence'}));
