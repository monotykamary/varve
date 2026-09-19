import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-prefix2.2Q0Icf/resume-stress';
const before=JSON.parse(readFileSync(root+'/before.json','utf8'));
let source=readFileSync(root+'/redeploy.mjs','utf8').replace(/^import .*;\n/gm,'');
assert(source.includes('function api(query,variables={})'));
source=source.replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const old={varve:'e840ec7a-7345-4210-88cd-aba4fea3f219',timescale:'7ca4127e-fc63-4ba8-917e-399694579f17',driver:'e23fadfc-f6a2-49d1-8422-1b56c93fdd20'};
const cases=[{name:'exact image reuse'},{name:'intent prevents repeat',intent:true,error:'already attempted'},{name:'prior scope mismatch',wrongOld:true,error:'Old deployment ownership'},{name:'active service refused',active:true,error:'Old deployment ownership'},{name:'returned scope mismatch',wrongNew:true,error:'New deployment ownership'}];
for(const test of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 if(test.active)raw.driver.activeDeployments=[{id:'unexpected',status:'SUCCESS'}];
 const previous=Object.fromEntries(Object.entries(old).map(([role,id])=>[role,{id,status:'REMOVED',serviceId:roles[role],environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}]));
 if(test.wrongOld)previous.driver.serviceId='unexpected';
 const mutations=[],writes=[];
 const sandbox={process:{env:{}},console:{log(){}},existsSync(){return Boolean(test.intent);},readFileSync(path){assert(path.endsWith('/before.json'));return JSON.stringify(before);},writeFileSync(path,text){writes.push({path,value:JSON.parse(text)});},execFileSync(){throw Error('External execution forbidden');},
 api(query,variables){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{'))return previous;if(query.startsWith('mutation(')){assert(query.includes('usePreviousImageTag:true'));const role=Object.keys(old).find(role=>old[role]===variables.id);assert(role);mutations.push(role);return {deploymentRedeploy:{...previous[role],id:'new-'+role,status:'INITIALIZING',serviceId:test.wrongNew?'unexpected':roles[role]}};}throw Error('Unexpected mocked operation');}};
 let failure;try{vm.runInNewContext(source,sandbox,{timeout:1000});}catch(error){failure=error.message;}
 if(test.error){assert(failure?.includes(test.error),test.name+': '+failure);assert.equal(mutations.length,test.wrongNew?1:0);assert.equal(writes.length,test.wrongNew?1:0);}
 else{assert.equal(failure,undefined);assert.deepEqual(mutations,['varve','timescale','driver']);const upload=writes.find(write=>write.path.endsWith('/upload-varve.jsonl')).value;assert.equal(upload.deploymentId,'new-varve');assert.equal(upload.reused_image_from,old.varve);assert.deepEqual(writes.at(-1).value.deployments,{varve:'new-varve',timescale:'new-timescale',driver:'new-driver'});}
}
console.log(JSON.stringify({redeploy_guard_cases:cases.length,passed:true,external_commands:0,infra_mutations:0,json_receipts_valid:true,scope:'Actual helper source with mocked I/O; not live deployment evidence'}));
