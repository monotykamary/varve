import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import assert from 'node:assert/strict';
const execute=promisify(execFile),root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor';
const read=name=>JSON.parse(readFileSync(root+'/'+name,'utf8'));
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const event=(name,data={})=>console.log(JSON.stringify({at:new Date().toISOString(),event:name,...data}));
async function helper(name,args=[],output){
  const result=await execute('node',[root+'/'+name,...args],{timeout:180000,maxBuffer:8*1024*1024});
  if(output)writeFileSync(root+'/'+output,result.stdout,{mode:0o600});
  return result.stdout;
}
async function probe(role,name,args=[],output){return helper('ssh.mjs',[role,root+'/'+name,...args],output);}
async function ready(roles,seconds){
  const deadline=Date.now()+seconds*1000;let previous='';
  while(Date.now()<deadline){
    const value=JSON.parse(await helper('status.mjs'));
    const states=Object.fromEntries(roles.map(role=>[role,value.deployments[role]?.status]));
    const description=JSON.stringify(states);
    if(description!==previous){event('deployment_states',states);previous=description;}
    if(roles.some(role=>!['READY','PENDING'].includes(value.readiness?.[role])))throw Error('Missing or invalid explicit readiness');
    if(Object.values(states).some(status=>!['QUEUED','INITIALIZING','WAITING','BUILDING','DEPLOYING','SUCCESS'].includes(status)))throw Error('Unexpected deployment state '+description);
    if(roles.every(role=>value.readiness?.[role]==='READY'&&states[role]==='SUCCESS'))return;
    await sleep(15000);
  }
  throw Error('Exact-deployment readiness deadline exceeded');
}
async function workload(id,mode,ceiling,phase){
  const launch=JSON.parse(await probe('driver','launch.py',[id,mode]));
  writeFileSync(root+'/'+id+'.launch.json',JSON.stringify(launch)+'\n',{mode:0o600});
  assert.deepEqual(launch.effective_driver_files,read('prepared.json').effective_driver_files,'Launched driver bytes differ from reviewed source');
  assert.equal(launch.driver_path,'/results/driver-accounting-s13-r1/benchmark.py');
  assert.equal(launch.uid,10001,'Launched driver must be nonroot');
  assert.equal(launch.gid,10001,'Launched driver group must be nonroot');
  event('workload_started',launch);
  const deadline=Date.parse(launch.at)+(ceiling+30)*1000;
  while(Date.now()<deadline){
    await probe('driver','collect.py',[id],id+'-bundle.json');
    const bundle=read(id+'-bundle.json');
    if(bundle.report_ready && JSON.parse(bundle.files[id+'.json']).finished_at){
      await probe('varve','metrics.py',[],'metrics-after-'+phase+'.json');
      event('workload_result',JSON.parse(await helper('unpack.mjs',[id])));
      const report=read(id+'.json');
      assert(['passed','overloaded'].includes(report.state),'Workload failed; retain evidence and investigate');
      assert.equal(report.mixed_workload.failed_or_ambiguous_rows,0);
      return;
    }
    await sleep(10000);
  }
  await helper('unpack.mjs',[id]);
  throw Error('Workload hard deadline reached without report');
}
function resource(name){return Object.fromEntries(readFileSync(root+'/'+name,'utf8').trim().split('\n').map(line=>{const i=line.indexOf('=');return [line.slice(0,i),line.slice(i+1)];}));}
assert(!existsSync(root+'/campaign-started.json'),'Campaign controller already started');
writeFileSync(root+'/campaign-started.json',JSON.stringify({at:new Date().toISOString(),pid:process.pid})+'\n',{mode:0o600,flag:'wx'});
try{
  {
  await ready(['varve'],1200);
  await probe('varve','probe-varve.py',[],'runtime-varve.json');
  event('varve_verified',{varve:JSON.parse(await helper('check-varve.mjs'))});
  event('redeploy_submitted',{result:(await helper('redeploy.mjs')).trim()});
  await ready(['varve','timescale','driver'],300);
  await Promise.all([
    probe('driver','probe-driver.py',[],'runtime-driver.json'),
    ...['varve','timescale','driver'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-before.txt')),
  ]);
  await probe('driver','install-driver.py',[],'runtime-driver-effective.json');
  event('runtime_verified',{driver:JSON.parse(await helper('check-driver.mjs'))});
  await probe('varve','metrics.py',[],'metrics-before.json');
  }
  await workload('acct101','baseline',1200,'baseline');
  await workload('acct102','stress',600,'stress');
  await Promise.all([
    probe('driver','resources.sh',[],'resources-driver-after-workload.txt'),
    ...['varve','timescale'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-pre-restart.txt')),
    probe('driver','recovery.py',['before'],'recovery-before.json'),
  ]);
  event('restart_submitted',{result:JSON.parse(await helper('restart.mjs'))});
  const deadline=Date.now()+240000;let restarted=false;
  while(Date.now()<deadline){
    await sleep(15000);
    try{
      await Promise.all(['varve','timescale'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-after-restart.txt')));
      restarted=['varve','timescale'].every(role=>{const before=resource('resources-'+role+'-pre-restart.txt'),after=resource('resources-'+role+'-after-restart.txt');return before.boot_id!==after.boot_id||before.process_start_ticks!==after.process_start_ticks;});
      if(restarted)break;
    }catch(error){event('restart_probe_retry',{message:'Container unavailable while restarting'});}
  }
  assert(restarted,'Actual process restart not witnessed');
  await ready(['varve','timescale','driver'],180);
  await probe('driver','recovery.py',['after'],'recovery-after.json');
  await probe('varve','metrics.py',[],'metrics-after-restart.json');
  event('recovery_verified',JSON.parse(await helper('check-recovery.mjs')));
  event('cleanup_requested',JSON.parse(await helper('stop-attempt.mjs')));
  await sleep(20000);
  event('cleanup_verified',JSON.parse(await helper('verify-stop.mjs')));
  writeFileSync(root+'/campaign-complete.json',JSON.stringify({at:new Date().toISOString(),performance_win_claimed:false})+'\n',{mode:0o600});
  event('campaign_complete',{performance_win_claimed:false});
}catch(error){
  writeFileSync(root+'/campaign-error.json',JSON.stringify({at:new Date().toISOString(),message:error.message,code:error.code})+'\n',{mode:0o600});
  event('campaign_halted',{message:error.message,inspect_owned_resources:true});
  try {
    if(!existsSync(root+'/cleanup-request.json'))event('abort_cleanup_requested',JSON.parse(await helper('stop-attempt.mjs',['--abort'])));
    await sleep(20000);
    event('abort_cleanup_verified',JSON.parse(await helper('verify-stop.mjs')));
  } catch(cleanupError) {
    writeFileSync(root+'/cleanup-error.json',JSON.stringify({at:new Date().toISOString(),message:cleanupError.message,manual_reconciliation_required:true})+'\n',{mode:0o600});
    event('abort_cleanup_unverified',{inspect_owned_resources:true});
  }
  process.exitCode=1;
}
