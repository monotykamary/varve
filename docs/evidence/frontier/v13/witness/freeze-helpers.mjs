import {readFileSync,writeFileSync,readdirSync,existsSync} from 'node:fs';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor';
const sha=data=>createHash('sha256').update(data).digest('hex');
const controls=readdirSync(root).filter(name=>/\.(mjs|py|sh)$/.test(name)||name.endsWith('.template')).sort();
const receipts=['before.json','prepared.json','PRIOR-s13-staged.json','PRIOR-s13-prepared.json','PRIOR-s13-runtime-varve.json','source-manifest.json','stage/SOURCE_MANIFEST.json','source.tar.gz','runtime-config.json','benchmark-config.json','driver-scope.json','driver-diagnostic-tests.log','DIAGNOSTIC_REVIEW.md','REVIEW_RESOLUTION.md','summary-tests.json','installer-tests.json','attestation-tests.json','cleanup-tests.json','control-tests.json','varve-tests.json','probe-varve-tests.json','ownership-tests.json','configuration-tests.json','analysis-tests.json','monitor-tests.json','reuse-tests.json','configure-tests.json','scope-tests.json','readiness-coupling-tests.json','MONITOR_REVIEW_RESOLUTION.md','PREPARATION.md'];
const qualification=['qualification.mjs','qualification.json','qualification.log','source-before.json'].map(name=>'../s13-qualification/'+name);
const files=Object.fromEntries([...controls,...receipts,...qualification].map(name=>[name,sha(readFileSync(root+'/'+name))]));
const path=root+'/helper-freeze.json';
if(process.argv.includes('--check')){
 const frozen=JSON.parse(readFileSync(path,'utf8'));assert.deepEqual(files,frozen.files,'Frozen offline helper, source, or receipt changed; review and requalify before activation');
 console.log(JSON.stringify({helper_freeze_verified:true,controls:controls.length,files:Object.keys(files).length,freeze_sha256:sha(readFileSync(path)),network_calls:0}));
}else{
 assert(!existsSync(path),'Helper freeze already exists; never silently replace it');
 for(const name of ['configured.json','configuration-intent.json','configuration-progress.json','activation-varve.json','activation-varve-intent.json','activation-varve-response.json','status-observations.jsonl','upload-varve.jsonl','redeploy-intent.json','redeployed.json','runtime-varve.json','runtime-driver.json','runtime-driver-effective.json','campaign-started.json','campaign-complete.json','campaign-error.json','cleanup-request.json','cleanup-response.json','cleanup-verified.json','status-latest.json','acct101.json','acct102.json'])assert(!existsSync(root+'/'+name),'Unexpected activation receipt: '+name);
 writeFileSync(path,JSON.stringify({at:new Date().toISOString(),scope:'Offline preparation only; existing before.json is Main actual preflight, test receipts are local fixtures, no runtime or deployment results fabricated',files,compute_started:false,performance_qualified:false},null,2)+'\n',{mode:0o600,flag:'wx'});
 console.log(JSON.stringify({helper_freeze_created:true,controls:controls.length,files:Object.keys(files).length,freeze_sha256:sha(readFileSync(path)),compute_started:false}));
}
