import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot';
const json=path=>JSON.parse(readFileSync(path,'utf8'));
const source=name=>readFileSync(root+'/'+name,'utf8').replace(/^import .*;\n/gm,'');
const sha=data=>createHash('sha256').update(data).digest('hex');
const before=json(root+'/before.json'),prepared=json(root+'/prepared.json');
class FixtureDate extends Date{constructor(...args){super(...(args.length?args:['2026-09-17T00:00:00.000Z']));}static now(){return Date.parse('2026-09-17T00:00:00.000Z');}}
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const old={timescale:'87eb3575-ec4b-4c51-9c62-7eb5285ef41e',driver:'fd3d1f84-8efc-4d27-8296-a67733599d89'};
const redeploySource=source('redeploy.mjs').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const cases=[{}, {wrongId:true,error:'Old deployment ownership'}, {wrongCap:true,error:'Resource caps changed'}, {intent:true,error:'already attempted'}, {wrongOld:true,error:'Old deployment ownership'}, {active:true,error:'Old deployment ownership'}, {wrongNew:true,error:'New deployment ownership'}, {wrongVarve:true,error:'Exact new Varve build is not ready'}];
for(const test of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 raw.varve.activeDeployments=[{id:test.wrongVarve?'foreign':'new-varve',status:'SUCCESS'}]; if(test.wrongCap)raw.driverLimits.containers.cpu=3;
 if(test.active)raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];
 const previous=Object.fromEntries(Object.entries(old).map(([role,id])=>[role,{id,status:'REMOVED',serviceId:roles[role],environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}]));
 if(test.wrongOld)previous.driver.serviceId='foreign'; if(test.wrongId)previous.timescale.id='foreign';
 const mutations=[],writes=[];
 const sandbox={process:{env:{}},console:{log(){}},existsSync(){return Boolean(test.intent);},readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/upload-varve.jsonl'))return JSON.stringify({deploymentId:'new-varve'});throw Error('Unexpected read');},writeFileSync(path,text){writes.push({path,value:JSON.parse(text)});},execFileSync(){throw Error('External execution forbidden');},api(query,variables){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{'))return previous;if(query.startsWith('mutation(')){assert(query.includes('usePreviousImageTag:true'));const role=Object.keys(old).find(role=>old[role]===variables.id);assert(role);mutations.push(role);return {deploymentRedeploy:{...previous[role],id:'new-'+role,status:'INITIALIZING',serviceId:test.wrongNew?'foreign':roles[role]}};}throw Error('Unexpected operation');}};
 let failure;try{vm.runInNewContext(redeploySource,sandbox,{timeout:1000});}catch(error){failure=error.message;}
 if(test.error){assert(failure?.includes(test.error),failure);assert.equal(mutations.length,test.wrongNew?1:0);}
 else{assert.equal(failure,undefined);assert.deepEqual(mutations,['timescale','driver']);assert.deepEqual(writes.at(-1).value.deployments,{timescale:'new-timescale',driver:'new-driver'});assert(!writes.some(write=>write.path.endsWith('/upload-varve.jsonl')));}
}
for(const mode of ['success','parse_failure','failed_workload','wrong_launch','varve_probe_failure','varve_check_failure','root_launch','wrong_launch_gid']){
 const injectParseFailure=mode==='parse_failure',expectedFailure=mode!=='success',earlyFailure=mode.startsWith('varve_');
 const disk=new Map([[root+'/prepared.json',JSON.stringify(prepared)]]),events=[],calls=[],polls={};let restarted=false;
 const proc={argv:['node','campaign'],pid:42,env:{},exitCode:0};
 const result={state:mode==='failed_workload'?'failed':'passed',finished_at:'done',mixed_workload:{failed_or_ambiguous_rows:mode==='failed_workload'?1000:0}};
 const execute=async(command,args)=>{
  assert.equal(command,'node');const name=args[0].split('/').pop();calls.push([name,...args.slice(1)]);
  if(name==='status.mjs')return {stdout:JSON.stringify({deployments:Object.fromEntries(['varve','timescale','driver'].map(role=>[role,{status:'SUCCESS'}]))})};
  if(name==='ssh.mjs'){
   const role=args[1],script=args[2].split('/').pop(),id=args[3];
   if(script==='probe-varve.py'&&mode==='varve_probe_failure')throw Error('fixture Varve probe failure');
   if(script==='launch.py')return {stdout:JSON.stringify({at:new FixtureDate().toISOString(),run_id:id,mode:args[4],uid:mode==='root_launch'?0:10001,gid:mode==='wrong_launch_gid'?0:10001,effective_driver_files:mode==='wrong_launch'?{}:prepared.effective_driver_files,driver_path:'/results/driver-accounting-s13/benchmark.py'})};
   if(script==='collect.py'){polls[id]=(polls[id]??0)+1;return {stdout:JSON.stringify({report_ready:true,files:{[id+'.json']:JSON.stringify(polls[id]===1?{state:'running',finished_at:null}:result)}})};}
   if(script==='resources.sh')return {stdout:'boot_id=same\nprocess_start_ticks='+(restarted?'2':'1')+'\n'};
   if(['metrics.py','probe-varve.py','probe-driver.py','recovery.py','install-driver.py'].includes(script))return {stdout:'{}'};
   throw Error('Unexpected probe '+script);
  }
  if(name==='unpack.mjs'){
   const id=args[1],phase=id==='acct001'?'baseline':'stress';
   assert(disk.has(root+'/metrics-after-'+phase+'.json'),'metrics must precede report parsing');
   assert.equal(polls[id],2,'running report must not be unpacked as final');
   if(injectParseFailure)throw Error('fixture parse failure');
   disk.set(root+'/'+id+'.json',JSON.stringify(result));return {stdout:'{}'};
  }
  if(name==='check-varve.mjs'&&mode==='varve_check_failure')throw Error('fixture Varve check failure');
  if(name==='restart.mjs')restarted=true;
  assert(['redeploy.mjs','check-varve.mjs','check-driver.mjs','restart.mjs','check-recovery.mjs','stop-attempt.mjs','verify-stop.mjs'].includes(name));
  return {stdout:'{}'};
 };
 const sandbox={assert,process:proc,console:{log(text){events.push(JSON.parse(text));}},Date:FixtureDate,Promise,JSON,setTimeout(callback){callback();},promisify(){return execute;},execFile(){throw Error('External execution forbidden');},existsSync(path){return disk.has(path);},readFileSync(path){assert(disk.has(path),'Missing fixture '+path);return disk.get(path);},writeFileSync(path,text){disk.set(path,text);}};
 await vm.runInNewContext('(async()=>{'+source('run-campaign.mjs')+'})()',sandbox,{timeout:1000});
 assert.equal(proc.exitCode,expectedFailure?1:0);assert.equal(calls[0][0],'status.mjs');
 assert.equal(calls[1][0],'ssh.mjs');assert.equal(calls[1][2],root+'/probe-varve.py');
 if(earlyFailure){assert(!calls.some(call=>call[0]==='redeploy.mjs'));assert(!calls.some(call=>call[0]==='ssh.mjs'&&call[1]!=='varve'));assert(!disk.has(root+'/acct001.launch.json'));}
 else{assert.equal(calls[2][0],'check-varve.mjs');assert.equal(calls[3][0],'redeploy.mjs');assert(disk.has(root+'/acct001.launch.json'));assert(calls.findIndex(call=>call[2]===root+'/install-driver.py')<calls.findIndex(call=>call[0]==='check-driver.mjs'));}
 assert(calls.some(call=>call[0]==='stop-attempt.mjs'&&(!expectedFailure||call[1]==='--abort')));
 assert(events.some(event=>event.event===(expectedFailure?'abort_cleanup_verified':'campaign_complete')));
 if(!expectedFailure){assert(disk.has(root+'/acct002.launch.json'));assert(restarted);} else {assert(!disk.has(root+'/acct002.launch.json'));assert(!restarted);}
}
const helpers=['prepare.mjs','configure.mjs','remote.mjs','redeploy.mjs','status.mjs','ssh.mjs','launch.py','unpack.mjs','stop-attempt.mjs','probe-varve.py','check-driver.mjs','probe-driver.py','collect.py','recovery.py','check-recovery.mjs','metrics.py','resources.sh','restart.mjs','verify-stop.mjs','run-campaign.mjs','install-driver.py','check-varve.mjs','verify-stage.mjs','stage.mjs','analyze-phases.mjs','install-driver.py.template'];
const receipt={at:new Date().toISOString(),source_digest:prepared.varve_source_digest,redeploy_fixture_cases:cases.length,campaign_fixture_cases:8,external_commands:0,infra_mutations:0,scope:'Actual controller VM fixtures; no live benchmark qualification',helpers:Object.fromEntries(helpers.map(name=>[name,sha(readFileSync(root+'/'+name))]))};
writeFileSync(root+'/control-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({...receipt,helpers:helpers.length}));
