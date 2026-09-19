import {readFileSync,writeFileSync,mkdirSync,existsSync,readdirSync,lstatSync,renameSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {join,dirname} from 'node:path';
import assert from 'node:assert/strict';
const base='/tmp/varve-diagnostic.KOUrtH/retry-config',root=base+'/s13-monitor',work=base+'/s13-failure',repo='/Users/monotykamary/VCS/working-remote/open-source/varve',stage=work+'/stage',dest=repo+'/docs/evidence/frontier/v13',sha=b=>createHash('sha256').update(b).digest('hex');
assert(!existsSync(stage)&&!existsSync(dest),'Never overwrite evidence');for(const n of ['acct102.json','acct102.launch.json','recovery-after.json','campaign-complete.json'])assert(!existsSync(root+'/'+n),'Unexpected later workload evidence');
execFileSync('node',[root+'/freeze-helpers.mjs','--check'],{stdio:'pipe'});execFileSync('node',[root+'/verify-ready.mjs'],{stdio:'pipe'});
const secret=JSON.parse(readFileSync('/tmp/varve-timescale.DU0Znc/secrets.json','utf8'));assert(secret.token&&secret.password);function checked(p){const b=readFileSync(p);for(const s of [secret.token,secret.password])assert(!b.includes(Buffer.from(s)),'Known credential in '+p);return b;}
const mappings=[];for(const n of readdirSync(root).sort()){if(!lstatSync(root+'/'+n).isFile()||n==='PROGRESS.md')continue;const target=/\.(mjs|py|sh)$/.test(n)||n.endsWith('.template')?'witness/'+n:n;mappings.push([target,root+'/'+n]);}
mappings.push(['stage/SOURCE_MANIFEST.json',root+'/stage/SOURCE_MANIFEST.json']);
function tree(from,to){for(const n of readdirSync(from).sort()){const p=join(from,n);assert(!lstatSync(p).isSymbolicLink());if(lstatSync(p).isDirectory())tree(p,to+'/'+n);else mappings.push([to+'/'+n,p]);}}
tree(base+'/s13-monitor-review-checkpoint','pre-correction');for(const n of ['REVIEW.md','REVIEW-CORRECTION.md','status-ready-probe.mjs'])mappings.push(['independent-review/'+n,base+'/s13-monitor-review/'+n]);
for(const n of ['qualification.mjs','qualification.json','qualification.log','source-before.json'])mappings.push(['qualification/'+n,base+'/s13-qualification/'+n]);
const previous=repo+'/docs/evidence/frontier/';mappings.push(['qualification/typescript-unit.log',previous+'v12/typescript-unit.log'],['local-qualified-source.tar.gz',previous+'v12/local-qualified-source.tar.gz'],['prior-v11-runtime.json',previous+'v12/prior-v11-runtime.json']);tree(previous+'v11/driver','driver');tree(previous+'v11/base-driver','base-driver');
mappings.push(['README.md',work+'/README.md'],['witness/verify.mjs',work+'/verify.mjs'],['witness/preserve.mjs',work+'/preserve.mjs']);assert.equal(new Set(mappings.map(([n])=>n)).size,mappings.length);
const prepared=mappings.map(([name,path])=>({name,bytes:checked(path)}));mkdirSync(stage);const files=[];for(const {name,bytes}of prepared){assert(!name.startsWith('/')&&!name.split('/').includes('..'));mkdirSync(dirname(stage+'/'+name),{recursive:true});writeFileSync(stage+'/'+name,bytes,{flag:'wx'});files.push({path:name,bytes:bytes.length,sha256:sha(bytes)});}
writeFileSync(stage+'/MANIFEST.json',JSON.stringify({format_version:1,candidate:'v13',kind:'failed-baseline-transport-disconnect',failed_workloads:1,completed_workloads:0,failed_or_ambiguous_rows:1000,performance_win_claimed:false,known_credential_values_absent:true,files},null,2)+'\n',{flag:'wx'});
const proof=JSON.parse(execFileSync('node',[stage+'/witness/verify.mjs'],{encoding:'utf8',timeout:60000}));renameSync(stage,dest);console.log(JSON.stringify({destination:dest,...proof,known_credential_values_absent:true}));
