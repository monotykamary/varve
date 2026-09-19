import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import assert from 'node:assert/strict';
const execute=promisify(execFile),root='/tmp/varve-resident.xUy7GB';
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
    if(roles.every(role=>states[role]==='SUCCESS'))return;
    if(Object.values(states).some(status=>!['QUEUED','INITIALIZING','WAITING','BUILDING','DEPLOYING','SUCCESS'].includes(status)))throw Error('Unexpected deployment state '+description);
    await sleep(15000);
  }
  throw Error('Exact-deployment readiness deadline exceeded');
}
async function workload(id,mode,ceiling,phase,resume=false){
  const launch=resume?read(id+'.launch.json'):JSON.parse(await probe('driver','launch.py',[id,mode]));
  event(resume?'workload_monitor_resumed':'workload_started',launch);
  const deadline=Date.parse(launch.at)+(ceiling+30)*1000;
  while(Date.now()<deadline){
    await probe('driver','collect.py',[id],id+'-bundle.json');
    const bundle=read(id+'-bundle.json');
    if(bundle.report_ready && JSON.parse(bundle.files[id+'.json']).finished_at){
      event('workload_result',JSON.parse(await helper('unpack.mjs',[id])));
      await probe('varve','metrics.py',[],'metrics-after-'+phase+'.json');
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
const resuming=process.argv.includes('--resume');
if(resuming){
  assert(existsSync(root+'/campaign-error.json'));
  assert.equal(read('resident001.json').state,'running');
  assert(!existsSync(root+'/campaign-resumed.json'));
  writeFileSync(root+'/campaign-resumed.json',JSON.stringify({at:new Date().toISOString(),pid:process.pid,reason:'Initial controller mistook an in-progress report for a final report; workload was not interrupted or relaunched.'})+'\n',{mode:0o600});
}else{
  assert(!existsSync(root+'/campaign-started.json'),'Campaign controller already started');
  writeFileSync(root+'/campaign-started.json',JSON.stringify({at:new Date().toISOString(),pid:process.pid})+'\n',{mode:0o600});
}
try{
  if(!resuming){
  await ready(['varve'],1200);
  event('redeploy_submitted',{result:(await helper('redeploy.mjs')).trim()});
  await ready(['varve','timescale','driver'],300);
  await Promise.all([
    probe('varve','probe-varve.py',[],'runtime-varve.json'),
    probe('driver','probe-driver.py',[],'runtime-driver.json'),
    ...['varve','timescale','driver'].map(role=>probe(role,'resources.sh',[],'resources-'+role+'-before.txt')),
  ]);
  event('runtime_verified',{driver:JSON.parse(await helper('check-driver.mjs'))});
  await probe('varve','metrics.py',[],'metrics-before.json');
  }
  await workload('resident001','baseline',1200,'baseline',resuming);
  await workload('resident002','stress',600,'stress');
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
  process.exitCode=1;
}
