import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
const repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const out='/tmp/varve-frontier-gate.JsZmOV';
const scopes=['.cargo/config.toml','Cargo.toml','Cargo.lock','README.md','LICENSE','AGENTS.md','build.rs','rust-toolchain','rust-toolchain.toml','src','tests','examples','clients/rust','clients/typescript/src','clients/typescript/test','clients/typescript/package.json','clients/typescript/package-lock.json','Dockerfile','.dockerignore','railway.json','scripts','benchmarks/timescale'];
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
function inventory(){
 const paths=[...new Set(execFileSync('git',['ls-files','-z','--cached','--others','--exclude-standard','--',...scopes],{cwd:repo,encoding:'utf8'}).split('\0').filter(Boolean))];
 for(const f of ['.cargo/config.toml','Cargo.lock'])if(fs.existsSync(path.join(repo,f))&&!paths.includes(f))paths.push(f);
 let total=0;
 return paths.sort().map(f=>{
  if(/[\r\n]/.test(f)||path.isAbsolute(f)||f.split('/').includes('..'))throw Error('unsafe source path');
  const absolute=path.join(repo,f), stat=fs.lstatSync(absolute);
  if(!stat.isFile()||stat.size>5*1024*1024)throw Error('nonregular or oversized source: '+f);
  total+=stat.size;if(total>20*1024*1024)throw Error('source inventory budget exceeded');
  return {path:f,bytes:stat.size,sha256:sha(fs.readFileSync(absolute))};
 });
}
const files=inventory(), manifest=path.join(out,'lifetime-source.json');
if(process.argv.includes('--verify')){
 const old=JSON.parse(fs.readFileSync(manifest));
 if(JSON.stringify(files)!==JSON.stringify(old.files))throw Error('source changed after freeze');
 console.log(JSON.stringify({verified:true,files:files.length,manifest_sha256:sha(fs.readFileSync(manifest))}));
}else{
 const body={kind:'local-source-input-inventory',base_commit:execFileSync('git',['rev-parse','HEAD'],{cwd:repo,encoding:'utf8'}).trim(),rustc:execFileSync('rustc',['-Vv'],{cwd:repo,encoding:'utf8'}).trim(),build_environment:Object.fromEntries(['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','CARGO_BUILD_TARGET','RUSTC_WRAPPER','CARGO_PROFILE_RELEASE_LTO','CARGO_PROFILE_RELEASE_CODEGEN_UNITS'].map(k=>[k,process.env[k]??null])),files};
 fs.writeFileSync(manifest,JSON.stringify(body,null,2)+'\n',{flag:'wx',mode:0o600});
 const list=path.join(out,'lifetime-source-files.txt'), archive=path.join(out,'lifetime-source.tar.gz');
 if(fs.existsSync(archive))throw Error('source archive already exists');
 fs.writeFileSync(list,files.map(f=>f.path).join('\n')+'\n',{flag:'wx',mode:0o600});
 execFileSync('tar',['-czf',archive,'-C',repo,'-T',list],{env:{...process.env,COPYFILE_DISABLE:'1'},stdio:'pipe'});
 fs.chmodSync(archive,0o600);
 console.log(JSON.stringify({frozen:true,files:files.length,bytes:files.reduce((n,f)=>n+f.bytes,0),manifest_sha256:sha(fs.readFileSync(manifest)),archive_sha256:sha(fs.readFileSync(archive))}));
}
