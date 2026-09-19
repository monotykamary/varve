import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const before=JSON.parse(readFileSync(root+'/before.json','utf8')),config=readFileSync(root+'/benchmark-config.json','utf8');
const source=readFileSync(root+'/configure.mjs','utf8').replace(/^import .*;\n/gm,'').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const cases=['valid','existing_intent','existing_receipt','active','caps','original','start_command','readback_mismatch','unexpected_activation'];
for(const name of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 if(name==='active')raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];
 if(name==='caps')raw.timescaleLimits.containers.cpu=3;
 if(name==='original')raw.original.latestDeployment.status='FAILED';
 if(name==='start_command')raw.varve.startCommand='foreign';
 const writes=[],mutations=[],values={},logs=[];let failure,snapshots=0;
 try{vm.runInNewContext(source,{Date:class extends Date{constructor(){super('2026-09-17T05:00:00Z');}},process:{env:{}},console:{log:text=>logs.push(JSON.parse(text))},existsSync:path=>(name==='existing_intent'&&path.endsWith('/configuration-intent.json'))||(name==='existing_receipt'&&path.endsWith('/configured.json')),readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/benchmark-config.json'))return config;assert.equal(path,repo+'/benchmarks/varve-service.json');return readFileSync(path,'utf8');},writeFileSync:(path,text)=>writes.push([path,JSON.parse(text)]),execFileSync(command,args){if(command==='node'){assert.equal(args[0],root+'/verify-ready.mjs');return '';}assert.equal(command,'railway');assert.deepEqual(Array.from(args.slice(0,2)),['variable','list']);const id=args[args.indexOf('--service')+1];return JSON.stringify(name==='readback_mismatch'?{}:values[id]);},api(query,variables){if(query.startsWith('query($p:')){snapshots++;if(snapshots===2&&name==='unexpected_activation')raw.driver.activeDeployments=[{id:'foreign',status:'SUCCESS'}];return raw;}assert(query.startsWith('mutation($i:VariableCollectionUpsertInput!)'));assert.equal(variables.i.skipDeploys,true);assert.equal(variables.i.replace,false);assert([roles.varve,roles.timescale].includes(variables.i.serviceId));assert(writes.some(([path])=>path.endsWith('/configuration-intent.json')));values[variables.i.serviceId]=variables.i.variables;mutations.push(variables.i);}},{timeout:1000});}catch(error){failure=error;}
 if(name==='valid'){assert.equal(failure,undefined);assert.equal(mutations.length,2);assert.equal(values[roles.varve].VARVE_DATA_DIR,'/data/probes/accounting-s13-r1');assert.equal(values[roles.varve].VARVE_BENCH_CONFIG,config.trim());assert.equal(values[roles.timescale].PGDATA,'/var/lib/postgresql/data/accounting-s13-r1');assert.equal(logs[0].compute_started,false);assert(writes.at(-1)[0].endsWith('/configured.json'));}
 else{assert(failure,name);assert.equal(logs.length,0);assert(!writes.some(([path])=>path.endsWith('/configured.json')));assert.equal(mutations.length,name==='readback_mismatch'?1:name==='unexpected_activation'?2:0);}
}
const receipt={cases:cases.length,case_names:cases,actual_source_exercised:true,external_commands:0,infra_mutations:0,helpers:{'configure.mjs':createHash('sha256').update(readFileSync(root+'/configure.mjs')).digest('hex')}};
writeFileSync(root+'/configure-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({cases:cases.length,external_commands:0,infra_mutations:0}));
