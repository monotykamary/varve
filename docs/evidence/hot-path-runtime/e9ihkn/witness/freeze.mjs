import {readdirSync,readFileSync,writeFileSync,mkdirSync,copyFileSync,lstatSync,existsSync} from 'node:fs';
import {join,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root='/Users/monotykamary/VCS/working-remote/open-source/varve';
const work='/tmp/varve-runtime.E9ihKN';
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
const ignored=new Set(['target','node_modules','__pycache__','.git','.DS_Store']);
function list(base,rel=''){
  return readdirSync(join(base,rel),{withFileTypes:true}).flatMap(entry=>{
    if(ignored.has(entry.name)||entry.name.endsWith('.pyc')||entry.name.startsWith('.env'))return [];
    const path=join(rel,entry.name);
    assert(!entry.isSymbolicLink(),'symlink outside explicit closure: '+path);
    return entry.isDirectory()?list(base,path):[path];
  }).sort();
}
function source(){
  const paths=['Cargo.toml','Cargo.lock','README.md','LICENSE','Dockerfile','.dockerignore','railway.json'];
  for(const prefix of ['src','tests','clients/rust','examples','scripts','config','benchmarks','licenses','.cargo']){
    paths.push(...list(join(root,prefix)).map(path=>join(prefix,path)));
  }
  const files=Object.fromEntries(paths.sort().map(path=>[path,digest(readFileSync(join(root,path)))]));
  return {files,sha256:digest(JSON.stringify(files))};
}
function save(path,value){writeFileSync(path,JSON.stringify(value,null,2)+'\n',{flag:'wx',mode:0o600});}
const [mode,name='final']=process.argv.slice(2);
assert(/^[a-z0-9_-]+$/.test(name),'invalid checkpoint name');
const checkpoint=join(work,'source-'+name+'.json');
if(mode==='capture'){
  const current=source();save(checkpoint,current);console.log(JSON.stringify({checkpoint,sha256:current.sha256,files:Object.keys(current.files).length}));
}else if(mode==='assert'){
  const frozen=JSON.parse(readFileSync(checkpoint));assert.deepEqual(source(),frozen);console.log(JSON.stringify({unchanged:true,sha256:frozen.sha256}));
}else if(mode==='stage'){
  const frozen=JSON.parse(readFileSync(checkpoint));assert.deepEqual(source(),frozen);
  const qualified=JSON.parse(readFileSync(join(work,'qualification.json')));
  assert(qualified.passed&&qualified.source_sha256===frozen.sha256,'runtime qualification missing/mismatched');
  const varve=join(work,'stage-varve'),driver=join(work,'stage-driver');
  assert(!existsSync(varve)&&!existsSync(driver),'stages are immutable; choose a new evidence root instead of overwriting');
  mkdirSync(varve);mkdirSync(driver);
  for(const path of Object.keys(frozen.files)){
    const dest=join(varve,path);mkdirSync(dirname(dest),{recursive:true});copyFileSync(join(root,path),dest);
  }
  const railway=JSON.parse(readFileSync(join(varve,'railway.json')));
  railway.deploy.restartPolicyType='NEVER';delete railway.deploy.restartPolicyMaxRetries;
  writeFileSync(join(varve,'railway.json'),JSON.stringify(railway,null,2)+'\n');
  writeFileSync(join(varve,'Dockerfile'),readFileSync(join(varve,'Dockerfile'),'utf8')+'\nCOPY source-manifest.json /usr/share/doc/varve/source-manifest.json\n');
  const overlays=['Dockerfile','railway.json'];
  save(join(varve,'source-manifest.json'),{source_sha256:frozen.sha256,qualified,files:frozen.files,overlays:Object.fromEntries(overlays.map(path=>[path,digest(readFileSync(join(varve,path)))]))});
  for(const name of ['benchmark.py','core.py','requirements.txt','Dockerfile'])copyFileSync(join(root,'benchmarks/timescale',name),join(driver,name));
  writeFileSync(join(driver,'railway.json'),JSON.stringify({build:{builder:'DOCKERFILE',dockerfilePath:'Dockerfile'},deploy:{numReplicas:1,restartPolicyType:'NEVER',sleepApplication:false,startCommand:'sleep infinity'}},null,2)+'\n');
  const profile=JSON.parse(readFileSync(join(work,'railway-readiness/varve-profile-sanitized.json'))).config;
  writeFileSync(join(work,'benchmark-profile.json'),JSON.stringify(profile),{flag:'wx',mode:0o600});
  const stages={};
  for(const [role,path]of [['varve',varve],['driver',driver]]){
    const files=Object.fromEntries(list(path).map(file=>[file,digest(readFileSync(join(path,file)))]));
    stages[role]={path,files,sha256:digest(JSON.stringify(files))};
  }
  save(join(work,'stages.json'),{source_sha256:frozen.sha256,profile_sha256:digest(readFileSync(join(work,'benchmark-profile.json'))),stages});
  assert.deepEqual(source(),frozen);console.log(JSON.stringify({staged:true,source_sha256:frozen.sha256,stages:Object.fromEntries(Object.entries(stages).map(([role,value])=>[role,{path:value.path,sha256:value.sha256,files:Object.keys(value.files).length}]))}));
}else throw new Error('usage: freeze.mjs capture|assert|stage [checkpoint]');
