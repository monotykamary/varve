import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor',before=JSON.parse(readFileSync(root+'/before.json','utf8'));
const config=readFileSync(root+'/benchmark-config.json','utf8').trim();
const source=readFileSync(root+'/reuse-varve.mjs','utf8').replace(/^import .*;\n/gm,'').replace('function api(query,variables={})','function forbiddenOriginalApi(query,variables={})');
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const old='a88e2c35-596f-41d4-b383-06857bf43108';
const cases=['valid','intent','receipt','missing_configuration','wrong_directory','wrong_profile_bytes','wrong_pgdata','active_varve','active_reference','caps','original','old_id','old_status','old_service','old_environment','old_project','missing_old','new_service','new_environment','new_project','new_missing_id','new_same_id','new_terminal','missing_response','ambiguous_mutation'];
for(const name of cases){
 const raw={project:structuredClone(before.project),original:structuredClone(before.original)};
 for(const role of Object.keys(roles)){raw[role]=structuredClone(before[role].instance);raw[role+'Limits']=structuredClone(before[role].limits);}
 const previous={id:old,status:'REMOVED',serviceId:roles.varve,environmentId:'5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',projectId:'8caffa15-0158-4822-a6c2-cb405bddc62d'},next={...previous,id:'new-varve',status:'INITIALIZING'};
 const configured={variables:{varve:{VARVE_DATA_DIR:'/data/probes/accounting-s13-r1',VARVE_BENCH_CONFIG:config},timescale:{PGDATA:'/var/lib/postgresql/data/accounting-s13-r1'}}};
 if(name==='wrong_directory')configured.variables.varve.VARVE_DATA_DIR='/old';
 if(name==='wrong_profile_bytes')configured.variables.varve.VARVE_BENCH_CONFIG=JSON.stringify(JSON.parse(config));
 if(name==='wrong_pgdata')configured.variables.timescale.PGDATA='/old';
 if(name==='active_varve')raw.varve.activeDeployments=[{id:'other',status:'SUCCESS'}];
 if(name==='active_reference')raw.driver.activeDeployments=[{id:'other',status:'SUCCESS'}];
 if(name==='caps')raw.varveLimits.containers.cpu=3;
 if(name==='original')raw.original.latestDeployment.status='FAILED';
 for(const [test,key]of Object.entries({old_id:'id',old_status:'status',old_service:'serviceId',old_environment:'environmentId',old_project:'projectId'}))if(name===test)previous[key]='foreign';
 for(const [test,key]of Object.entries({new_service:'serviceId',new_environment:'environmentId',new_project:'projectId'}))if(name===test)next[key]='foreign';
 if(name==='new_missing_id')delete next.id;
 if(name==='new_same_id')next.id=old;
 if(name==='new_terminal')next.status='FAILED';
 const writes=[],mutations=[],commands=[],logs=[];let failure;
 try{vm.runInNewContext(source,{Date:class extends Date{constructor(){super('2026-09-17T05:00:00Z');}},process:{env:{}},console:{log:text=>logs.push(JSON.parse(text))},existsSync:path=>(name==='intent'&&path.endsWith('/activation-varve-intent.json'))||(name==='receipt'&&path.endsWith('/activation-varve.json')),readFileSync(path){if(path.endsWith('/before.json'))return JSON.stringify(before);if(path.endsWith('/benchmark-config.json'))return config;if(path.endsWith('/configured.json')){assert.notEqual(name,'missing_configuration');return JSON.stringify(configured);}throw Error('Unexpected read '+path);},writeFileSync(path,text,options){assert.equal(options.flag,'wx');assert(!path.endsWith('/upload-varve.jsonl'));writes.push([path,JSON.parse(text)]);},execFileSync(command,args){assert.equal(command,'node');commands.push(args);assert([root+'/freeze-helpers.mjs',root+'/verify-ready.mjs'].includes(args[0]));return '';},api(query,variables){if(query.startsWith('query($p:'))return raw;if(query.startsWith('query{')){assert(query.includes(old));return {varve:name==='missing_old'?null:previous};}assert(query.startsWith('mutation('));assert(query.includes('usePreviousImageTag:true'));assert.equal(variables.id,old);assert.equal(writes.length,1,'Durable intent precedes exactly one mutation');mutations.push(variables);if(name==='ambiguous_mutation')throw Error('Fixture ambiguous transport failure');return {deploymentRedeploy:name==='missing_response'?null:next};}},{timeout:1000});}catch(error){failure=error;}
 assert.equal(commands.length,2);
 const submitted=name==='valid'||name.startsWith('new_')||['missing_response','ambiguous_mutation'].includes(name);
 assert.equal(mutations.length,submitted?1:0,name);
 if(name==='valid'){assert.equal(failure,undefined);assert.equal(writes.length,3);assert.equal(writes[2][1].deploymentId,'new-varve');assert.deepEqual(writes[2][1].submitted,next);assert.equal(writes[2][1].previous.id,old);assert.equal(writes[2][1].usePreviousImageTag,true);assert.equal(logs.length,1);}
 else{assert(failure,name+' must reject');assert.equal(logs.length,0);assert.equal(writes.length,submitted?(name==='ambiguous_mutation'?1:2):0,name);assert(!writes.some(([path])=>path.endsWith('/activation-varve.json')));}
}
const sha=data=>createHash('sha256').update(data).digest('hex');
const receipt={cases:cases.length,case_names:cases,actual_source_exercised:true,external_commands:0,infra_mutations:0,helpers:{'reuse-varve.mjs':sha(readFileSync(root+'/reuse-varve.mjs'))}};
writeFileSync(root+'/reuse-tests.json',JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({cases:cases.length,external_commands:0,infra_mutations:0}));
