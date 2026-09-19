import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import assert from 'node:assert/strict';
const execute=promisify(execFile),parent='/tmp/varve-prefix2.2Q0Icf',root=parent+'/resume-stress';
const read=name=>JSON.parse(readFileSync(root+'/'+name,'utf8'));
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const event=(name,data={})=>console.log(JSON.stringify({at:new Date().toISOString(),event:name,...data}));
async function helper(name,args=[],output){
 const result=await execute('node',[root+'/'+name,...args],{timeout:180000,maxBuffer:8*1024*1024});
 if(output)writeFileSync(root+'/'+output,result.stdout,{mode:0o600});
 return result.stdout;
}
const probe=(role,name,args=[],output)=>helper('ssh.mjs',[role,root+'/'+name,...args],output);
async function ready(seconds){
 const deadline=Date.now()+seconds*1000;let previous='';
 while(Date.now()<deadline){
  const value=JSON.parse(await helper('status.mjs'));
  const states=Object.fromEntries(['varve','timescale','driver'].map(role=>[role,value.deployments[role]?.status]));
  const description=JSON.stringify(states);
  if(description!==previous){event('deployment_states',states);previous=description;}
  if(Object.values(states).every(status=>status==='SUCCESS'))return;
  assert(Object.values(states).every(status=>['QUEUED','INITIALIZING','WAITING','BUILDING','DEPLOYING','SUCCESS'].includes(status)),'Unexpected deployment state');
  await sleep(10000);
 }
 throw Error('Exact-image readiness deadline exceeded');
}
function resource(name){return Object.fromEntries(readFileSync(root+'/'+name,'utf8').trim().split('\n').map(line=>{const i=line.indexOf('=');return [line.slice(0,i),line.slice(i+1)];}));}
async function verifyCleanup(){
 const deadline=Date.now()+90000;let failure;
 while(Date.now()<deadline){try{return JSON.parse(await helper('verify-stop.mjs'));}catch(error){failure=error;await sleep(10000);}}
 throw failure??Error('Cleanup verification deadline exceeded');
}
assert(!existsSync(root+'/campaign-started.json'),'Resume controller already started');
writeFileSync(root+'/campaign-started.json',JSON.stringify({at:new Date().toISOString(),pid:process.pid,baseline_rerun:false,intervening_restart:true,fresh_uninterrupted_trial:false})+'\n',{mode:0o600});
try{
 await execute('node',[parent+'/qualification.mjs','--check'],{timeout:60000});
 event('images_submitted',{result:(await helper('redeploy.mjs')).trim(),rebuild:false});
 await ready(300);
 await Promise.all([
  probe('varve','probe-varve.py',[],'runtime-varve.json'),
  probe('driver','probe-driver.py',[],'runtime-driver.json'),
  ...['varve','timescale','driver'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-before.txt')),
 ]);
 const original=JSON.parse(readFileSync(parent+'/runtime-varve.json','utf8')),current=read('runtime-varve.json');
 for(const key of ['source_manifest_sha256','config_sha256','binary_sha256','duckdb_binary_sha256','duckdb_version','region','cpu_max','memory_max','data_dir'])assert.deepEqual(current[key],original[key],'Runtime drift: '+key);
 assert.equal(current.status.database_id,original.status.database_id);
 event('runtime_verified',JSON.parse(await helper('check-driver.mjs')));
 event('baseline_reference_restored',JSON.parse(await probe('driver','restore-reference.py')));
 await probe('driver','recover-baseline.py',['before'],'baseline-recovered.json');
 assert.equal(read('baseline-recovered.json').fingerprints.prefix001.expected_watermark_rows,550000);
 event('baseline_recovered',{rows:550000,baseline_rerun:false});
 await probe('varve','metrics.py',[],'metrics-before-stress.json');
 const launch=JSON.parse(await probe('driver','launch.py',['prefix002','stress']));
 event('workload_started',launch);
 const deadline=Date.parse(launch.at)+630000;let finished=false;
 while(Date.now()<deadline){
  await probe('driver','collect.py',['prefix002'],'prefix002-bundle.json');
  const bundle=read('prefix002-bundle.json');
  if(bundle.report_ready&&JSON.parse(bundle.files['prefix002.json']).finished_at){
   // Preserve phase evidence before formatting or interpreting the local report.
   await probe('varve','metrics.py',[],'metrics-after-stress.json');
   event('workload_result',JSON.parse(await helper('unpack.mjs',['prefix002'])));
   const report=read('prefix002.json');
   assert(['passed','overloaded'].includes(report.state),'Stress correctness failed; preserve all evidence');
   assert.equal(report.mixed_workload.failed_or_ambiguous_rows,0);
   finished=true;break;
  }
  await sleep(10000);
 }
 assert(finished,'Stress hard deadline reached without finished report');
 await Promise.all([
  probe('driver','resources.sh',[],'resources-driver-after-workload.txt'),
  ...['varve','timescale'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-pre-restart.txt')),
  probe('driver','recovery.py',['before'],'recovery-before.json'),
 ]);
 assert.deepEqual(read('baseline-recovered.json').fingerprints.prefix001,read('recovery-before.json').fingerprints.prefix001,'Baseline changed during stress');
 event('restart_submitted',JSON.parse(await helper('restart.mjs')));
 const restartDeadline=Date.now()+240000;let restarted=false;
 while(Date.now()<restartDeadline){
  await sleep(10000);
  try{
   await Promise.all(['varve','timescale'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-after-restart.txt')));
   restarted=['varve','timescale'].every(role=>{const before=resource('resources-'+role+'-pre-restart.txt'),after=resource('resources-'+role+'-after-restart.txt');return before.boot_id!==after.boot_id||before.process_start_ticks!==after.process_start_ticks;});
   if(restarted)break;
  }catch{event('restart_probe_retry');}
 }
 assert(restarted,'Actual database process restarts not witnessed');
 await ready(180);
 await probe('driver','recovery.py',['after'],'recovery-after.json');
 await probe('varve','metrics.py',[],'metrics-after-restart.json');
 event('recovery_verified',JSON.parse(await helper('check-recovery.mjs')));
 event('cleanup_requested',JSON.parse(await helper('stop-attempt.mjs')));
 event('cleanup_verified',await verifyCleanup());
 writeFileSync(root+'/campaign-complete.json',JSON.stringify({at:new Date().toISOString(),performance_win_claimed:false,baseline_rerun:false,intervening_restart:true,fresh_uninterrupted_trial:false})+'\n',{mode:0o600});
 event('resumed_diagnostic_complete',{performance_win_claimed:false});
}catch(error){
 writeFileSync(root+'/campaign-error.json',JSON.stringify({at:new Date().toISOString(),message:error.message,code:error.code})+'\n',{mode:0o600});
 event('campaign_halted',{message:error.message});
 try{
  if(!existsSync(root+'/cleanup-request.json'))event('abort_cleanup_requested',JSON.parse(await helper('stop-attempt.mjs',['--abort'])));
  event('abort_cleanup_verified',await verifyCleanup());
 }catch(cleanupError){
  writeFileSync(root+'/cleanup-error.json',JSON.stringify({at:new Date().toISOString(),message:cleanupError.message,manual_reconciliation_required:true})+'\n',{mode:0o600});
  event('abort_cleanup_unverified',{manual_reconciliation_required:true});
 }
 process.exitCode=1;
}
