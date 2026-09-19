import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor';
const before=JSON.parse(readFileSync(root+'/before.json','utf8'));
const source=name=>readFileSync(root+'/'+name,'utf8').replace(/^import .*;\n/gm,'');
const sha=data=>createHash('sha256').update(data).digest('hex');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const statusSource=source('status.mjs').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const cases=['ready','missing_active','active_deploying','detail_deploying','both_deploying','foreign','multiple','unrecorded','service_owner','environment_owner','project_owner','wrong_id','missing_detail','caps','workspace','service_name','volumes','original','domain','overwrite_varve','detail_api_failure',...['FAILED','CRASHED','REMOVED','REMOVING','SKIPPED','UNKNOWN',null].flatMap(s=>['detail:'+s,'active:'+s])];
const retained=[],early=['caps','workspace','service_name','volumes','original','domain','overwrite_varve','detail_api_failure'];
for(const name of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 raw.varve.activeDeployments=[{id:'new-varve',status:'SUCCESS'}];
 const info={varve:{id:'new-varve',status:'SUCCESS',serviceId:roles.varve,environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}};
 if(name==='missing_active')raw.varve.activeDeployments=[];
 if(['active_deploying','both_deploying'].includes(name))raw.varve.activeDeployments[0].status='DEPLOYING';
 if(['detail_deploying','both_deploying'].includes(name))info.varve.status='DEPLOYING';
 if(name==='foreign')raw.varve.activeDeployments[0].id='foreign';
 if(name==='multiple')raw.varve.activeDeployments.push({...raw.varve.activeDeployments[0]});
 if(name==='unrecorded')raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];
 for(const [test,key]of Object.entries({service_owner:'serviceId',environment_owner:'environmentId',project_owner:'projectId',wrong_id:'id'}))if(name===test)info.varve[key]='foreign';
 if(name==='missing_detail')info.varve=null;
 if(name==='caps')raw.driverLimits.containers.memoryBytes++;
 if(name==='workspace')raw.project.workspaceId='foreign';
 if(name==='service_name')raw.project.services.edges.find(e=>e.node.id===roles.varve).node.name='foreign';
 if(name==='volumes')raw.project.volumes.edges=[];
 if(name==='original')raw.original.latestDeployment.status='FAILED';
 if(name==='domain')raw.varve.domains.serviceDomains.push({domain:'foreign'});
 if(name.startsWith('detail:'))info.varve.status=name.slice(7)==='null'?null:name.slice(7);
 if(name.startsWith('active:'))raw.varve.activeDeployments[0].status=name.slice(7)==='null'?null:name.slice(7);
 const journal=[],writes=[],logs=[];let failure;
 const sandbox={Date:class extends Date{constructor(){super('2026-09-17T05:00:00Z');}},randomUUID:()=>name,process:{env:{}},console:{log:text=>logs.push(JSON.parse(text))},existsSync:()=>name==='overwrite_varve',readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/activation-varve.json'))return JSON.stringify({deploymentId:'new-varve'});if(path.endsWith('/redeployed.json'))return JSON.stringify({deployments:{varve:'foreign'}});throw Error('Unexpected read');},writeFileSync:(path,text)=>writes.push([path,JSON.parse(text)]),appendFileSync(path,text){assert.equal(path,root+'/status-observations.jsonl');journal.push(JSON.parse(text));retained.push(text);},execFileSync(){throw Error('External execution forbidden');},api(query){if(query.startsWith('query($p:'))return raw;assert.equal(journal.length,1,'Snapshot must be retained before the second read');assert.deepEqual(journal[0].raw,raw);if(name==='detail_api_failure')throw Error('Fixture detail transport failure');return info;}};
 try{vm.runInNewContext(statusSource,sandbox,{timeout:1000});}catch(error){failure=error;}
 assert.equal(journal.length,early.includes(name)?1:2,name);
 assert.equal(journal[0].at,'2026-09-17T05:00:00.000Z');assert.deepEqual(journal[0].raw,raw);
 if(journal.length===2)assert.deepEqual(journal[1].raw,info,'Exact rejected observation retained: '+name);
 const positive=['ready','missing_active','active_deploying','detail_deploying','both_deploying'].includes(name);
 if(positive){assert.equal(failure,undefined,name);assert.equal(logs[0].readiness.varve,name==='ready'?'READY':'PENDING');assert.equal(writes.length,1);}
 else{assert(failure,name+' must reject');assert.equal(logs.length,0);assert.equal(writes.length,0);}
}
assert.equal(retained.length,cases.length*2-early.length,'All failed and successful observations retained, never replaced by latest');
const apiCases=['snapshot_caps','snapshot_graphql','detail_graphql','snapshot_malformed','detail_transport','detail_owner'];
for(const name of apiCases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 raw.varve.activeDeployments=[{id:'new-varve',status:'SUCCESS'}];
 if(name==='snapshot_caps')raw.varveLimits.containers.cpu=3;
 const journals=[],outputs=[],writes=[];let apiCalls=0,failure,uuid=0;
 const sandbox={Date:class extends Date{constructor(){super('2026-09-17T05:00:00Z');}},randomUUID:()=> 'fixture-'+uuid++,process:{env:{}},console:{log:text=>outputs.push(text)},existsSync:()=>false,readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);assert(path.endsWith('/activation-varve.json'));return JSON.stringify({deploymentId:'new-varve'});},writeFileSync:(path,text)=>writes.push([path,text]),unlinkSync(){},appendFileSync(path,text){journals.push(JSON.parse(text));},execFileSync(command,args){assert.equal(command,'railway');assert.equal(args[0],'api');apiCalls++;if(apiCalls===2){assert(journals.some(j=>j.kind==='snapshot'));if(name==='detail_transport')throw Object.assign(Error('Fixture transport failure'),{stdout:'partial actual transport bytes',stderr:'fixture stderr'});if(name==='detail_graphql')return JSON.stringify({errors:[{message:'Fixture detail error'}]});return JSON.stringify({data:{varve:{id:'new-varve',status:'SUCCESS',serviceId:'foreign',environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}}});}if(name==='snapshot_graphql')return JSON.stringify({data:raw,errors:[{message:'Fixture partial snapshot error'}]});if(name==='snapshot_malformed')return 'actually-obtained-malformed-json';return JSON.stringify({data:raw});}};
 try{vm.runInNewContext(source('status.mjs'),sandbox,{timeout:1000});}catch(error){failure=error;}
 assert(failure,name);assert.equal(outputs.length,0);assert(!writes.some(([path])=>path.endsWith('/status-latest.json')));
 assert.equal(apiCalls,name.startsWith('snapshot_')?1:2);
 assert.equal(journals[0].kind,'api_stdout','Exact raw stdout precedes parsing/validation');
 if(name==='snapshot_caps')assert.deepEqual(journals.find(j=>j.kind==='snapshot').raw,raw);
 if(name==='snapshot_malformed')assert.equal(journals[0].raw.stdout,'actually-obtained-malformed-json');
 if(name==='detail_transport')assert.equal(journals.find(j=>j.kind==='api_error').raw.stdout,'partial actual transport bytes');
 if(name!=='detail_owner')assert(!journals.some(j=>j.kind==='deployment_detail'),'Never fabricate detail that was not obtained');
 else assert.equal(journals.find(j=>j.kind==='deployment_detail').raw.varve.serviceId,'foreign');
}

const controllerCases=['varve_convergence','reference_convergence','varve_timeout','reference_timeout','missing_readiness','unknown_readiness','terminal_detail','status_guard_error'];
for(const mode of controllerCases){
 let now=Date.parse('2026-09-17T05:00:00Z'),varvePolls=0,referencePolls=0,references=false;
 class Clock extends Date{constructor(...args){super(...(args.length?args:[now]));}static now(){return now;}}
 const calls=[],events=[],disk=new Map(),proc={argv:[],env:{},pid:42,exitCode:0};
 const execute=async(command,args)=>{
  assert.equal(command,'node');const name=args[0].split('/').pop();calls.push([name,...args.slice(1)]);
  if(name==='status.mjs'){
   if(mode==='status_guard_error')throw Error('Fixture fatal ownership guard');
   if(references)referencePolls++;else varvePolls++;
   const waiting=(!references&&mode==='varve_timeout')||(references&&mode==='reference_timeout')||(!references&&mode==='varve_convergence'&&varvePolls<3)||(references&&mode==='reference_convergence'&&referencePolls<3);
   const readiness={varve:'READY',timescale:'READY',driver:'READY'};
   if(waiting)readiness[references?'driver':'varve']='PENDING';
   if(mode==='missing_readiness')delete readiness.varve;
   if(mode==='unknown_readiness')readiness.varve='UNKNOWN';
   const deployments=Object.fromEntries(Object.keys(roles).map(role=>[role,{status:mode==='terminal_detail'?'FAILED':'SUCCESS'}]));
   if(mode==='terminal_detail')readiness.varve='PENDING';
   return {stdout:JSON.stringify({deployments,readiness})};
  }
  if(name==='redeploy.mjs'){references=true;return {stdout:'{}'};}
  if(name==='check-varve.mjs')return {stdout:'{}'};
  if(name==='ssh.mjs'){
   const script=args[2].split('/').pop();
   assert.notEqual(script,'launch.py','No workload may be launched by these readiness fixtures');
   if(args[1]==='varve'&&script==='probe-varve.py'){assert(!mode.endsWith('_timeout')||mode==='reference_timeout');if(mode==='varve_convergence')assert.equal(varvePolls,3);return {stdout:'{}'};}
   assert.equal(references,true);if(mode==='reference_convergence')assert.equal(referencePolls,3);
   throw Error('Fixture stop after witnessed readiness, before any workload');
  }
  assert(['stop-attempt.mjs','verify-stop.mjs'].includes(name));return {stdout:'{}'};
 };
 await vm.runInNewContext('(async()=>{'+source('run-campaign.mjs')+'})()',{assert,process:proc,Date:Clock,Promise,JSON,console:{log:text=>events.push(JSON.parse(text))},promisify:()=>execute,execFile(){throw Error('External execution forbidden');},setTimeout(callback,ms){now+=ms;callback();},existsSync:path=>disk.has(path),readFileSync(path){assert(disk.has(path));return disk.get(path);},writeFileSync:(path,text)=>disk.set(path,text)},{timeout:1000});
 assert.equal(proc.exitCode,1);assert(!calls.some(c=>c[0]==='restart.mjs'||c[2]?.endsWith('/launch.py')));
 assert(calls.some(c=>c[0]==='stop-attempt.mjs'&&c[1]==='--abort'));assert(calls.some(c=>c[0]==='verify-stop.mjs'));
 const error=JSON.parse(disk.get(root+'/campaign-error.json')).message;
 if(mode==='varve_timeout'){assert.equal(varvePolls,80);assert.equal(referencePolls,0);assert(!references);assert(error.includes('deadline exceeded'));}
 if(mode==='reference_timeout'){assert.equal(varvePolls,1);assert.equal(referencePolls,20);assert(error.includes('deadline exceeded'));}
 if(['missing_readiness','unknown_readiness','terminal_detail','status_guard_error'].includes(mode))assert(!calls.some(c=>c[0]==='ssh.mjs'||c[0]==='redeploy.mjs'));
}
const result={cases:cases.length+controllerCases.length+apiCases.length,api_cases:apiCases,status_cases:cases,controller_cases:controllerCases,raw_observations_retained:retained.length,actual_sources_exercised:true,external_commands:0,infra_mutations:0,helpers:Object.fromEntries(['status.mjs','run-campaign.mjs'].map(name=>[name,sha(readFileSync(root+'/'+name))]))};
writeFileSync(root+'/monitor-tests.json',JSON.stringify(result,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({cases:result.cases,status:cases.length,controller:controllerCases.length,external_commands:0}));
