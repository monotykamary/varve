import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-scoped.0bj4bw',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const json=path=>JSON.parse(readFileSync(path,'utf8'));
const source=name=>readFileSync(root+'/'+name,'utf8').replace(/^import .*;\n/gm,'');
const sha=data=>createHash('sha256').update(data).digest('hex');
const before=json(root+'/before.json'),qualified=json(root+'/qualification.json'),staged=json(root+'/stage/SOURCE_MANIFEST.json');
assert.equal(staged.local_qualified_source,qualified.source_digest);
let qualifiedStageFiles=0;
for(const file of staged.files){
 const data=readFileSync(root+'/stage/'+file.path);assert.equal(sha(data),file.sha256);
 const input=qualified.files.find(item=>item.path===file.path);
 if(input){assert.equal(file.sha256,input.sha256);qualifiedStageFiles++;}
 else if(file.path==='Dockerfile')assert.equal(data.toString().replace('COPY SOURCE_MANIFEST.json /usr/share/doc/varve/benchmark-source.json\n',''),readFileSync(repo+'/Dockerfile','utf8'));
 else assert.equal(file.sha256,sha(readFileSync(repo+'/'+file.path)));
}
assert.deepEqual(json(root+'/benchmark-config.json'),json(repo+'/docs/evidence/frontier/v9/varve-config.json'));
assert.equal(sha(readFileSync(root+'/runtime-config.json')),'215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089');
const probe=readFileSync(root+'/probe-varve.py','utf8');assert(probe.includes(sha(readFileSync(root+'/stage/SOURCE_MANIFEST.json'))));
const phaseNames=['disk_lock_wait','disk_lock_hold','wal_disk_lock_wait','group_prepare','derived_verify','derived_publish','raw_verify','raw_publish'];
for(const phase of phaseNames){assert(readFileSync(root+'/metrics.py','utf8').includes("'"+phase+"'"));assert(readFileSync(repo+'/src/metrics.rs','utf8').includes('=> "'+phase+'"'));}
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const old={timescale:'3bb14b95-0468-455c-9e3d-7f55d483ec45',driver:'b6849f53-4623-4a1d-b21f-6940fab9e235'};
const redeploySource=source('redeploy.mjs').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const cases=[{}, {intent:true,error:'already attempted'}, {wrongOld:true,error:'Old deployment ownership'}, {active:true,error:'Old deployment ownership'}, {wrongNew:true,error:'New deployment ownership'}, {wrongVarve:true,error:'Exact new Varve build'}];
for(const test of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 raw.varve.activeDeployments=[{id:test.wrongVarve?'foreign':'new-varve',status:'SUCCESS'}];
 if(test.active)raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];
 const previous=Object.fromEntries(Object.entries(old).map(([role,id])=>[role,{id,status:'REMOVED',serviceId:roles[role],environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'}]));
 if(test.wrongOld)previous.driver.serviceId='foreign';
 const mutations=[],writes=[];
 const sandbox={process:{env:{}},console:{log(){}},existsSync(){return Boolean(test.intent);},readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/upload-varve.jsonl'))return JSON.stringify({deploymentId:'new-varve'});throw Error('Unexpected read');},writeFileSync(path,text){writes.push({path,value:JSON.parse(text)});},execFileSync(){throw Error('External execution forbidden');},api(query,variables){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{'))return previous;if(query.startsWith('mutation(')){assert(query.includes('usePreviousImageTag:true'));const role=Object.keys(old).find(role=>old[role]===variables.id);assert(role);mutations.push(role);return {deploymentRedeploy:{...previous[role],id:'new-'+role,status:'INITIALIZING',serviceId:test.wrongNew?'foreign':roles[role]}};}throw Error('Unexpected operation');}};
 let failure;try{vm.runInNewContext(redeploySource,sandbox,{timeout:1000});}catch(error){failure=error.message;}
 if(test.error){assert(failure?.includes(test.error),failure);assert.equal(mutations.length,test.wrongNew?1:0);}
 else{assert.equal(failure,undefined);assert.deepEqual(mutations,['timescale','driver']);assert.deepEqual(writes.at(-1).value.deployments,{timescale:'new-timescale',driver:'new-driver'});assert(!writes.some(write=>write.path.endsWith('/upload-varve.jsonl')));}
}
const report=json(repo+'/docs/evidence/frontier/v9/prefix001.json');
for(const id of ['scoped001','scoped002','prefix001','../x','scoped003','scoped001.extra']){
 const writes=[],logs=[];let failure;
 try{vm.runInNewContext(source('unpack.mjs'),{process:{argv:['node','unpack',id],exit(){throw Error('Unexpected exit');}},readFileSync(path){assert.equal(path,root+'/'+id+'-bundle.json');return JSON.stringify({files:{[id+'.json']:JSON.stringify(report)}});},writeFileSync(path,text){writes.push(path);},console:{log(text){logs.push(JSON.parse(text));}}},{timeout:1000});}catch(error){failure=error.message;}
 if(['scoped001','scoped002'].includes(id)){assert.equal(failure,undefined);assert.equal(writes.length,1);assert.equal(logs[0].state,report.state);assert.equal(logs[0].watermark,report.manifest.total_committed_watermark_rows);}
 else{assert(failure?.includes('Unknown run ID'));assert.equal(writes.length,0);}
}
for(const injectParseFailure of [false,true]){
 const disk=new Map(),events=[],calls=[],polls={};let restarted=false;
 const proc={argv:['node','campaign'],pid:42,env:{},exitCode:0};
 const result={state:'passed',finished_at:'done',mixed_workload:{failed_or_ambiguous_rows:0}};
 const execute=async(command,args)=>{
  assert.equal(command,'node');const name=args[0].split('/').pop();calls.push([name,...args.slice(1)]);
  if(name==='status.mjs')return {stdout:JSON.stringify({deployments:Object.fromEntries(['varve','timescale','driver'].map(role=>[role,{status:'SUCCESS'}]))})};
  if(name==='ssh.mjs'){
   const role=args[1],script=args[2].split('/').pop(),id=args[3];
   if(script==='launch.py')return {stdout:JSON.stringify({at:new Date().toISOString(),run_id:id,mode:args[4]})};
   if(script==='collect.py'){polls[id]=(polls[id]??0)+1;return {stdout:JSON.stringify({report_ready:true,files:{[id+'.json']:JSON.stringify(polls[id]===1?{state:'running',finished_at:null}:result)}})};}
   if(script==='resources.sh')return {stdout:'boot_id=same\nprocess_start_ticks='+(restarted?'2':'1')+'\n'};
   if(['metrics.py','probe-varve.py','probe-driver.py','recovery.py'].includes(script))return {stdout:'{}'};
   throw Error('Unexpected probe '+script);
  }
  if(name==='unpack.mjs'){
   const id=args[1],phase=id==='scoped001'?'baseline':'stress';
   assert(disk.has(root+'/metrics-after-'+phase+'.json'),'metrics must precede report parsing');
   assert.equal(polls[id],2,'running report must not be unpacked as final');
   if(injectParseFailure)throw Error('fixture parse failure');
   disk.set(root+'/'+id+'.json',JSON.stringify(result));return {stdout:'{}'};
  }
  if(name==='restart.mjs')restarted=true;
  assert(['redeploy.mjs','check-driver.mjs','restart.mjs','check-recovery.mjs','stop-attempt.mjs','verify-stop.mjs'].includes(name));
  return {stdout:'{}'};
 };
 const sandbox={assert,process:proc,console:{log(text){events.push(JSON.parse(text));}},Date,Promise,JSON,setTimeout(callback){callback();},promisify(){return execute;},execFile(){throw Error('External execution forbidden');},existsSync(path){return disk.has(path);},readFileSync(path){assert(disk.has(path),'Missing fixture '+path);return disk.get(path);},writeFileSync(path,text){disk.set(path,text);}};
 await vm.runInNewContext('(async()=>{'+source('run-campaign.mjs')+'})()',sandbox,{timeout:1000});
 assert.equal(proc.exitCode,injectParseFailure?1:0);
 assert(disk.has(root+'/scoped001.launch.json'));
 assert(calls.some(call=>call[0]==='stop-attempt.mjs'&&(!injectParseFailure||call[1]==='--abort')));
 assert(events.some(event=>event.event===(injectParseFailure?'abort_cleanup_verified':'campaign_complete')));
 if(!injectParseFailure){assert(disk.has(root+'/scoped002.launch.json'));assert(restarted);}
}
const helpers=['stage.mjs','configure.mjs','remote.mjs','redeploy.mjs','status.mjs','ssh.mjs','launch.py','unpack.mjs','stop-attempt.mjs','probe-varve.py','check-driver.mjs','probe-driver.py','collect.py','recovery.py','check-recovery.mjs','metrics.py','resources.sh','restart.mjs','verify-stop.mjs','run-campaign.mjs'];
const receipt={at:new Date().toISOString(),source_digest:qualified.source_digest,staged_files:staged.files.length,qualified_staged_files:qualifiedStageFiles,profile_identical_to_v9:true,redeploy_fixture_cases:cases.length,run_id_fixture_cases:6,campaign_fixture_cases:2,external_commands:0,infra_mutations:0,scope:'VM controller fixtures and source hashes; not live benchmark evidence',helpers:Object.fromEntries(helpers.map(name=>[name,sha(readFileSync(root+'/'+name))]))};
writeFileSync(root+'/helper-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});
console.log(JSON.stringify({...receipt,helpers:helpers.length}));
