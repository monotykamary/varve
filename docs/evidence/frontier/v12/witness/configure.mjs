import {readFileSync,writeFileSync,unlinkSync,existsSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot';
const repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const projectId='8caffa15-0158-4822-a6c2-cb405bddc62d', environmentId='5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1';
const workspaceId='18857f84-0af6-4379-9217-e62e2d14b48f';
const original='64ddd834-6732-4582-bf4a-d86bbd0317fa';
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3'};
const env={...process.env,RAILWAY_CALLER:'skill:use-railway@1.4.0',RAILWAY_AGENT_SESSION:'varve-sediment-20260916'};
function api(query,variables={}){
  const file=root+'/'+randomUUID()+'.json';writeFileSync(file,JSON.stringify(variables),{mode:0o600});
  try{const r=JSON.parse(execFileSync('railway',['api',query,'--variables','@'+file],{env,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024,stdio:['ignore','pipe','pipe']}));if(r.errors)throw Error('GraphQL returned errors');return r.data;}
  catch(e){writeFileSync(root+'/api-error-'+randomUUID()+'.txt',String(e.stderr??e.message),{mode:0o600});throw Error('Railway API failed; inspect private diagnostics before retrying');}
  finally{unlinkSync(file);}
}
function snapshot(){
 const details='region startCommand latestDeployment{id status} activeDeployments{id status} domains{serviceDomains{domain} customDomains{domain}}';
 const query='query($p:String!,$e:String!,$o:String!,$v:String!,$t:String!,$d:String!){project(id:$p){id workspaceId services{edges{node{id name}}} volumes{edges{node{id name}}}} original:serviceInstance(serviceId:$o,environmentId:$e){region startCommand latestDeployment{id status} activeDeployments{id status}} varve:serviceInstance(serviceId:$v,environmentId:$e){'+details+'} timescale:serviceInstance(serviceId:$t,environmentId:$e){'+details+'} driver:serviceInstance(serviceId:$d,environmentId:$e){'+details+'} varveLimits:serviceInstanceLimits(serviceId:$v,environmentId:$e) timescaleLimits:serviceInstanceLimits(serviceId:$t,environmentId:$e) driverLimits:serviceInstanceLimits(serviceId:$d,environmentId:$e)}';
 const raw=api(query,{p:projectId,e:environmentId,o:original,v:roles.varve,t:roles.timescale,d:roles.driver});
 const state={project:raw.project,original:raw.original};
 if(state.project.workspaceId!==workspaceId)throw Error('Wrong workspace');
 const names={varve:'varve-benchmark',timescale:'timescale-benchmark',driver:'benchmark-driver'};
 for(const [role,id]of Object.entries(roles)){
   if(!state.project.services.edges.some(({node})=>node.id===id&&node.name===names[role]))throw Error('Owned service mismatch');
   state[role]={instance:raw[role],limits:raw[role+'Limits']};
   if(state[role].instance.domains.serviceDomains.length||state[role].instance.domains.customDomains.length)throw Error('Unexpected public benchmark domain');
   const limits=state[role].limits.containers;
   if(limits.cpu!==2||limits.memoryBytes!==(role==='driver'?1000000000:2000000000))throw Error('Resource caps changed');
 }
 for(const id of ['720d3de6-3ce6-4007-b2a1-c3f6d2ee5232','cdd445af-7c7d-42b8-ba10-5a2055e5abc8'])if(!state.project.volumes.edges.some(({node})=>node.id===id))throw Error('Owned volume missing');
 return state;
}
execFileSync('node',[root+'/verify-ready.mjs'],{stdio:'pipe'});
if(existsSync(root+'/configured.json'))throw Error('Configuration already recorded; inspect existing proof');
const before=JSON.parse(readFileSync(root+'/before.json','utf8'));
const now=snapshot();
if(JSON.stringify(now.original)!==JSON.stringify(before.original))throw Error('Original service changed');
for(const role of Object.keys(roles))if(now[role].instance.activeDeployments.length)throw Error('Unexpected active benchmark deployment');
const service=JSON.parse(readFileSync(repo+'/benchmarks/varve-service.json','utf8'));
if(now.varve.instance.startCommand!==service.startCommand)throw Error('Benchmark start command changed; inspect before mutation');
const variables={varve:{VARVE_DATA_DIR:'/data/probes/accounting-s13',VARVE_BENCH_CONFIG:readFileSync(root+'/benchmark-config.json','utf8').trim()},timescale:{PGDATA:'/var/lib/postgresql/data/accounting-s13'}};
writeFileSync(root+'/configuration-intent.json',JSON.stringify({at:new Date().toISOString(),variables,completed:[]},null,2)+'\n',{mode:0o600});
const completed=[];
for(const [role,values]of Object.entries(variables)){
 api('mutation($i:VariableCollectionUpsertInput!){variableCollectionUpsert(input:$i)}',{i:{projectId,environmentId,serviceId:roles[role],skipDeploys:true,replace:false,variables:values}});
 const raw=JSON.parse(execFileSync('railway',['variable','list','--project',projectId,'--environment',environmentId,'--service',roles[role],'--json'],{env,encoding:'utf8',timeout:60000,stdio:['ignore','pipe','pipe']}));
 const actual=Array.isArray(raw)?Object.fromEntries(raw.map(v=>[v.name,v.value])):(raw.variables??raw);
 for(const [key,value]of Object.entries(values))if(actual[key]!==value)throw Error('Variable readback differs: '+role+'/'+key);
 completed.push(role);writeFileSync(root+'/configuration-progress.json',JSON.stringify({completed})+'\n',{mode:0o600});
}
const after=snapshot();
if(JSON.stringify(after.original)!==JSON.stringify(before.original))throw Error('Original service changed');
for(const role of Object.keys(roles))if(after[role].instance.activeDeployments.length)throw Error('Unexpected deployment started');
writeFileSync(root+'/configured.json',JSON.stringify({at:new Date().toISOString(),variables,state:after},null,2)+'\n',{mode:0o600});
console.log(JSON.stringify({configured:completed,readback_verified:true,original_unchanged:true,compute_started:false,explicit_format:'v2 opt-in',derived_budget_bytes:536870912,fresh_directory_suffix:'accounting-s13'}));
