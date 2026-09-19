import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot',before=JSON.parse(readFileSync(root+'/before.json','utf8'));
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const sha=data=>createHash('sha256').update(data).digest('hex');
const cases=[
 ['status.mjs','uploaded_building'],['status.mjs','uploaded_success'],['status.mjs','all_success'],['status.mjs','foreign_active'],['status.mjs','wrong_owner'],['status.mjs','wrong_id'],['status.mjs','unrecorded_active'],['status.mjs','overwrite_upload'],['status.mjs','missing_active'],
 ['stop-attempt.mjs','early_varve_only'],['stop-attempt.mjs','overwrite_upload'],
 ['verify-stop.mjs','early_varve_only'],['verify-stop.mjs','all_removed'],['verify-stop.mjs','unrecorded_active'],['verify-stop.mjs','wrong_id'],['verify-stop.mjs','wrong_owner'],['verify-stop.mjs','wrong_receipt'],['verify-stop.mjs','missing_response']
];
for(const [helper,name]of cases){
 const verify=helper==='verify-stop.mjs',stop=helper==='stop-attempt.mjs',all=['all_success','all_removed','overwrite_upload'].includes(name);
 const expected={varve:'uploaded-varve',...(all?{timescale:'new-timescale',driver:'new-driver'}:{})};
 const refs=all?{timescale:'new-timescale',driver:'new-driver'}:null;if(name==='overwrite_upload')refs.varve='foreign-varve';
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)},infos={};
 for(const role of Object.keys(roles)){
  raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);
  raw[role].activeDeployments=expected[role]&&!verify?[{id:expected[role],status:name==='uploaded_building'?'BUILDING':'SUCCESS'}]:[];
  if(expected[role])infos[role]={id:expected[role],status:verify?'REMOVED':name==='uploaded_building'?'BUILDING':'SUCCESS',serviceId:roles[role],environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'};
 }
 if(name==='foreign_active')raw.varve.activeDeployments[0].id='foreign';
 if(name==='missing_active')raw.varve.activeDeployments=[];
 if(name==='unrecorded_active')raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];
 if(name==='wrong_owner')infos.varve.serviceId='foreign';
 if(name==='wrong_id')infos.varve.id='foreign';
 if(name==='missing_response')delete infos.varve;
 const disk=new Map([[root+'/before.json',JSON.stringify(before)],[root+'/upload-varve.jsonl',JSON.stringify({deploymentId:'uploaded-varve'})]]);
 if(refs)disk.set(root+'/redeployed.json',JSON.stringify({deployments:refs}));
 if(verify)disk.set(root+'/cleanup-request.json',JSON.stringify({expected:name==='wrong_receipt'?{varve:'foreign'}:expected}));
 const writes=[],mutations=[];let failure;
 const source=readFileSync(root+'/'+helper,'utf8').replace(/^import .*;\n/gm,'').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
 try{vm.runInNewContext(source,{process:{env:{},argv:['node',helper,'--abort']},console:{log(){}},existsSync:path=>disk.has(path),readFileSync(path){assert(disk.has(path));return disk.get(path);},writeFileSync(path,text){writes.push([path,JSON.parse(text)]);},execFileSync(){throw Error('External execution forbidden');},api(query){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{'))return infos;if(query.startsWith('mutation{')){mutations.push(query);assert(stop);assert(query.includes('varve:deploymentRemove(id:"uploaded-varve")'));assert(!query.includes('timescale:'));assert(!query.includes('driver:'));return {varve:true};}throw Error('Unexpected API');}},{timeout:1000});}catch(error){failure=error;}
 const positive=['uploaded_building','uploaded_success','all_success','early_varve_only','all_removed'].includes(name);
 if(positive){assert.equal(failure,undefined,helper+'/'+name);assert.equal(mutations.length,stop?1:0);assert(writes.length);if(stop)assert.deepEqual(writes[0][1].expected,{varve:'uploaded-varve'});if(verify)assert.equal(writes[0][1].active_benchmark_deployments,0);}
 else{assert(failure,helper+'/'+name+' must reject');assert.equal(mutations.length,0);assert.equal(writes.length,0);}
}
const helpers=Object.fromEntries([...new Set(cases.map(c=>c[0]))].map(name=>[name,sha(readFileSync(root+'/'+name))]));
const receipt={cases:cases.length,case_names:cases.map(c=>c.join('/')),actual_sources_exercised:true,early_cleanup_only_uploaded_varve:true,external_commands:0,infra_mutations:0,helpers};
writeFileSync(root+'/ownership-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(receipt));
