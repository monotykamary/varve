import {readFileSync,writeFileSync,mkdirSync,existsSync,readdirSync,lstatSync,renameSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {join,dirname} from 'node:path';
import assert from 'node:assert/strict';
const work='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-evidence',pilot='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot',qualification='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-qualification',repo='/Users/monotykamary/VCS/working-remote/open-source/varve',stage=work+'/stage',dest=repo+'/docs/evidence/frontier/v12';
assert(!existsSync(dest)&&!existsSync(stage),'Never overwrite existing or partial evidence');
for(const script of ['freeze-helpers.mjs','verify-ready.mjs'])execFileSync('node',[pilot+'/'+script,...(script.startsWith('freeze')?['--check']:[])],{stdio:'pipe'});
for(const n of ['acct001.launch.json','acct002.launch.json','acct001.json','acct002.json','runtime-driver-effective.json','campaign-complete.json'])assert(!existsSync(pilot+'/'+n),'Unexpected workload evidence');
const secret=JSON.parse(readFileSync('/tmp/varve-timescale.DU0Znc/secrets.json','utf8'));assert(secret.token&&secret.password);const sha=b=>createHash('sha256').update(b).digest('hex');
function checked(path){const b=readFileSync(path);for(const value of [secret.token,secret.password])assert(!b.includes(Buffer.from(value)),'Known credential in '+path);return b;}
const q=JSON.parse(checked(qualification+'/qualification.json'));for(const f of q.files){assert(!f.path.startsWith('/')&&!f.path.split('/').includes('..'));assert(lstatSync(repo+'/'+f.path).isFile());const b=checked(repo+'/'+f.path);assert.equal(b.length,f.bytes);assert.equal(sha(b),f.sha256);}
const mappings=[];for(const name of readdirSync(pilot).sort()){if(/\.(mjs|py|sh)$/.test(name)||name.endsWith('.template'))mappings.push(['witness/'+name,pilot+'/'+name]);else if(/\.json$/.test(name)||['campaign.log','upload-varve.jsonl','upload-varve.stderr','source.tar.gz','PREPARATION.md','CONTRACT.md','driver-diagnostic-tests.log','DIAGNOSTIC_REVIEW.md','REVIEW_RESOLUTION.md'].includes(name))mappings.push([name,pilot+'/'+name]);}
for(const name of ['qualification.json','qualification.log','source-before.json'])mappings.push([name,qualification+'/'+name]);
mappings.push(['typescript-unit.log',repo+'/docs/evidence/frontier/v11/typescript-unit.log'],['prior-v11-runtime.json',repo+'/docs/evidence/frontier/v11/runtime-varve.json'],['README.md',work+'/README.md'],['witness/verify.mjs',work+'/verify.mjs'],['witness/preserve.mjs',work+'/preserve.mjs']);
assert.equal(new Set(mappings.map(([n])=>n)).size,mappings.length);const prepared=mappings.map(([name,path])=>({name,bytes:checked(path)}));mkdirSync(stage);for(const {name,bytes}of prepared){mkdirSync(dirname(join(stage,name)),{recursive:true});writeFileSync(join(stage,name),bytes);}
execFileSync('tar',['-czf',stage+'/local-qualified-source.tar.gz','-C',repo,'--',...q.files.map(f=>f.path)],{env:{...process.env,COPYFILE_DISABLE:'1'},stdio:'pipe'});
const files=[];function walk(path,prefix=''){for(const n of readdirSync(path).sort()){const full=join(path,n),relative=prefix+n;if(lstatSync(full).isDirectory())walk(full,relative+'/');else{const bytes=checked(full);files.push({path:relative,bytes:bytes.length,sha256:sha(bytes)});}}}walk(stage);
writeFileSync(stage+'/MANIFEST.json',JSON.stringify({format_version:1,candidate:'v12',kind:'pre-workload-monitor-abort',comparative_runs:0,failed_poll_retained:false,performance_win_claimed:false,known_credential_values_absent:true,files},null,2)+'\n');
const result=JSON.parse(execFileSync('node',[stage+'/witness/verify.mjs'],{encoding:'utf8',timeout:60000}));renameSync(stage,dest);console.log(JSON.stringify({destination:dest,...result,known_credentials_absent:true}));
