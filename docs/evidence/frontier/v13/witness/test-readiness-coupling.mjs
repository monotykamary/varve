import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor';
const before=JSON.parse(readFileSync(root+'/before.json','utf8')),sha=b=>createHash('sha256').update(b).digest('hex');
const statusBytes=readFileSync(root+'/status.mjs','utf8'),campaignBytes=readFileSync(root+'/run-campaign.mjs','utf8');
const statusSource=statusBytes.replace(/^import .*;\n/gm,'').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const readySource=campaignBytes.slice(campaignBytes.indexOf('async function ready('),campaignBytes.indexOf('async function workload('));
const referenceCall=campaignBytes.match(/await ready\(\[[^\]]+\],300\);/g);assert.equal(referenceCall?.length,1,'Extract the actual reference-phase call, not an invented role list');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const ids={varve:'new-varve',timescale:'new-timescale',driver:'new-driver'};
const cases=['missing_active','active_deploying','detail_deploying','ready','never_converges'];
for(const mode of cases){
 let now=0,polls=0,sleeps=0;
 class Clock extends Date{constructor(...args){super(...(args.length?args:[now]));}static now(){return now;}}
 function actualStatus(){
  polls++;const pending=mode==='never_converges'||(mode!=='ready'&&polls<3);
  const raw={project:structuredClone(before.project),original:structuredClone(before.original)},details={};
  for(const [role,serviceId]of Object.entries(roles)){
   raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);raw[role].activeDeployments=[{id:ids[role],status:'SUCCESS'}];
   details[role]={id:ids[role],status:'SUCCESS',serviceId,environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'};
  }
  if(pending){if(mode==='active_deploying')raw.varve.activeDeployments[0].status='DEPLOYING';else if(mode==='detail_deploying')details.varve.status='DEPLOYING';else raw.varve.activeDeployments=[];}
  const output=[];
  vm.runInNewContext(statusSource,{Date:Clock,process:{env:{}},randomUUID:()=>mode+'-'+polls,console:{log:s=>output.push(s)},existsSync:p=>p.endsWith('/redeployed.json'),readFileSync(p){if(p.endsWith('/before.json'))return JSON.stringify(before);if(p.endsWith('/activation-varve.json'))return JSON.stringify({deploymentId:ids.varve});if(p.endsWith('/redeployed.json'))return JSON.stringify({deployments:{timescale:ids.timescale,driver:ids.driver}});throw Error('Unexpected read');},writeFileSync(){},appendFileSync(){},unlinkSync(){},execFileSync(){throw Error('External execution forbidden');},api:q=>q.startsWith('query($p:')?raw:details},{timeout:1000});
  assert.equal(output.length,1);assert.deepEqual(JSON.parse(output[0]).readiness,{varve:pending?'PENDING':'READY',timescale:'READY',driver:'READY'});return output[0];
 }
 let failure;try{await vm.runInNewContext('(async()=>{'+readySource+referenceCall[0]+'})()',{Date:Clock,JSON,helper:async n=>{assert.equal(n,'status.mjs');return actualStatus();},event(){},sleep:async ms=>{assert.equal(ms,15000);now+=ms;sleeps++;}},{timeout:1000});}catch(e){failure=e;}
 if(mode==='never_converges'){assert.match(failure?.message??'',/readiness deadline exceeded/);assert.equal(polls,20);assert.equal(sleeps,20);assert.equal(now,300000);}
 else{assert.equal(failure,undefined);assert.equal(polls,mode==='ready'?1:3,mode+': actual reference gate must not advance with Varve pending');assert.equal(sleeps,mode==='ready'?0:2);}
}
const receipt={cases:cases.length,case_names:cases,actual_status_and_controller_call_composed:true,external_commands:0,infra_mutations:0,helpers:{'status.mjs':sha(statusBytes),'run-campaign.mjs':sha(campaignBytes)}};
writeFileSync(root+'/readiness-coupling-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({cases:cases.length,actual_status_and_controller_call_composed:true,external_commands:0}));
