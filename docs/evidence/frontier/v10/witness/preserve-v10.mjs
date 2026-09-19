import {readFileSync,writeFileSync,mkdirSync,existsSync,readdirSync,statSync,renameSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {join,dirname} from 'node:path';
import assert from 'node:assert/strict';
const root='/tmp/varve-scoped.0bj4bw',repo='/Users/monotykamary/VCS/working-remote/open-source/varve',destination=repo+'/docs/evidence/frontier/v10',stage=root+'/evidence-stage-v10';
assert(!existsSync(destination)&&!existsSync(stage),'Never overwrite existing evidence or a partial staging attempt');
execFileSync('node',[root+'/verify-ready.mjs'],{stdio:'pipe'});
const secrets=JSON.parse(readFileSync('/tmp/varve-timescale.DU0Znc/secrets.json','utf8'));assert(secrets.token&&secrets.password);
const hash=data=>createHash('sha256').update(data).digest('hex');
const checked=path=>{const bytes=readFileSync(path);for(const value of [secrets.token,secrets.password])assert(!bytes.includes(Buffer.from(value)),'Known credential found in '+path);return bytes;};
const json=path=>JSON.parse(checked(path));
const source=json(root+'/stage/SOURCE_MANIFEST.json'),qualification=json(root+'/qualification.json'),runtime=json(root+'/runtime-varve.json');
for(const [base,files]of [[root+'/stage',source.files],[repo,qualification.files]])for(const file of files){assert(!file.path.startsWith('/')&&!file.path.split('/').includes('..'));const bytes=checked(base+'/'+file.path);assert.equal(bytes.length,file.bytes);assert.equal(hash(bytes),file.sha256);}
assert.equal(hash(checked(root+'/stage/SOURCE_MANIFEST.json')),runtime.source_manifest_sha256);assert.equal(hash(checked(root+'/runtime-config.json')),runtime.config_sha256);
assert.deepEqual(json(root+'/runtime-config.json'),json(repo+'/docs/evidence/frontier/v9/varve-config.json'));
const clean=json(root+'/cleanup-verified.json');assert.equal(clean.active_benchmark_deployments,0);assert(clean.original_unchanged&&clean.benchmark_volumes_retained);
const report=json(root+'/scoped001.json');assert.equal(report.state,'failed');assert.equal(report.mixed_workload.failed_or_ambiguous_rows,1000);assert(!existsSync(root+'/scoped002.launch.json'));assert(!existsSync(root+'/campaign-complete.json'));
const mappings=[];
for(const suffix of ['.json','.log','.launch.json'])mappings.push(['scoped001'+suffix,root+'/scoped001'+suffix]);
for(const name of ['before.json','configured.json','runtime-varve.json','runtime-driver.json','redeploy-intent.json','redeployed.json','upload-varve.jsonl','campaign-started.json','campaign-error.json','campaign.log','metrics-before.json','metrics-after-baseline.json','analysis-baseline.json','failure-summary.json','cleanup-request.json','cleanup-verified.json','staged.json','helper-tests.json','integration-check.json','REVIEW_RESOLUTION.md','INDEPENDENT_REVIEW.md','CONTRACT.md','varve-failure-logs.jsonl','timescale-failure-logs.jsonl'])mappings.push([name,root+'/'+name]);
for(const name of ['first-monitor-started.json','duplicate-monitor-guard.log','upload-timeout-first-inference-superseded.json','upload-timeout-reconciled.json','monitor-resumed.json'])mappings.push(['monitor-incident/'+name,root+'/'+name]);
for(const role of ['varve','timescale','driver'])mappings.push(['resources-'+role+'-before.txt',root+'/resources-'+role+'-before.txt']);
for(const name of ['stage.mjs','configure.mjs','remote.mjs','redeploy.mjs','ssh.mjs','status.mjs','check-driver.mjs','check-recovery.mjs','restart.mjs','stop-attempt.mjs','verify-stop.mjs','unpack.mjs','launch.py','probe-varve.py','probe-driver.py','resources.sh','metrics.py','collect.py','recovery.py','run-campaign.mjs','test-cleanup.mjs','test-controls.mjs','analyze-phases.mjs','summarize-failure.mjs','resume-monitor.mjs','verify-ready.mjs','qualification.mjs','qualify.sh','check-integration.mjs','preserve-v10.mjs'])mappings.push(['witness/'+name,root+'/'+name]);
const driver=json(root+'/runtime-driver.json');for(const name of ['benchmark.py','core.py','Dockerfile','requirements.txt']){const path=repo+'/docs/evidence/frontier/v4/driver/'+name;assert.equal(hash(checked(path)),driver.files[name]);mappings.push(['driver/'+name,path]);}
mappings.push(['source-manifest.json',root+'/stage/SOURCE_MANIFEST.json'],['source.tar.gz',root+'/source.tar.gz'],['varve-config.json',root+'/runtime-config.json'],['previous-profile.json',repo+'/docs/evidence/frontier/v9/varve-config.json'],['README.md',root+'/evidence.md'],['witness/verify.mjs',root+'/verify-v10.mjs']);
qualification.logs=qualification.logs.map((log,index)=>{const name=['local-qualification.log','typescript-unit.log'][index];assert(name);assert.equal(hash(checked(log.path)),log.sha256);mappings.push([name,log.path]);return {...log,path:name};});
const prepared=mappings.map(([name,path])=>({name,bytes:checked(path)}));assert.equal(new Set(prepared.map(file=>file.name)).size,prepared.length);
mkdirSync(stage,{recursive:true});for(const {name,bytes}of prepared){mkdirSync(dirname(join(stage,name)),{recursive:true});writeFileSync(join(stage,name),bytes);}
execFileSync('tar',['-czf',stage+'/local-qualified-source.tar.gz','-C',repo,...qualification.files.map(file=>file.path)],{timeout:30000});
writeFileSync(stage+'/local-qualification.json',JSON.stringify(qualification,null,2)+'\n');
const files=[];function walk(path,prefix=''){for(const name of readdirSync(path).sort()){const full=join(path,name),relative=prefix+name;if(statSync(full).isDirectory())walk(full,relative+'/');else{const bytes=checked(full);files.push({path:relative,bytes:bytes.length,sha256:hash(bytes)});}}}walk(stage);
writeFileSync(stage+'/MANIFEST.json',JSON.stringify({format_version:1,candidate:'v10',outcomes:{baseline:'failed',stress:'not_run'},fresh_uninterrupted_trial:false,pre_workload_monitor_interrupted:true,intervening_database_restart:false,performance_win_claimed:false,recovery_verified:false,acknowledged_common_watermark_rows:487000,failed_or_ambiguous_rows:1000,known_credential_values_absent:true,files},null,2)+'\n');
const verified=JSON.parse(execFileSync('node',[stage+'/witness/verify.mjs'],{encoding:'utf8',timeout:30000}));
renameSync(stage,destination);
console.log(JSON.stringify({destination,...verified,known_credentials_absent:true,overall_win:false}));
