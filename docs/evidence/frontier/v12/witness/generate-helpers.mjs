import {readFileSync,writeFileSync,copyFileSync,existsSync} from 'node:fs';
import assert from 'node:assert/strict';
const parent='/tmp/varve-diagnostic.KOUrtH/retry-config',root=parent+'/s13-pilot',old='/tmp/varve-scoped.0bj4bw';
const adapt=s=>s.replaceAll(parent,root).replaceAll('diagnostic-kourth-2','accounting-s13').replaceAll('diag101','acct001').replaceAll('diag102','acct002').replaceAll('diag10[12]','acct00[12]').replaceAll('diag103','acct003');
function save(name,text){assert(!existsSync(root+'/'+name),name+' already exists');writeFileSync(root+'/'+name,text,{mode:0o600});}
const names=['configure.mjs','status.mjs','ssh.mjs','launch.py','unpack.mjs','stop-attempt.mjs','probe-varve.py','check-driver.mjs','probe-driver.py','collect.py','recovery.py','check-recovery.mjs','metrics.py','resources.sh','restart.mjs','verify-stop.mjs','run-campaign.mjs','install-driver.py.template','test-summary.mjs','test-installer.py','test-attestation.mjs','test-cleanup.mjs','test-controls.mjs','test-configuration.mjs','analyze-phases.mjs'];
for(const name of names)save(name,adapt(readFileSync(parent+'/'+name,'utf8')));
for(const name of ['driver-scope.json','driver-diagnostic-tests.log','DIAGNOSTIC_REVIEW.md','REVIEW_RESOLUTION.md']){assert(!existsSync(root+'/'+name));copyFileSync(parent+'/'+name,root+'/'+name);}
let stage=readFileSync(old+'/stage.mjs','utf8').replaceAll(old,root).replaceAll("root+'/qualification.mjs'","root+'/../s13-qualification/qualification.mjs'").replaceAll("root+'/qualification.json'","root+'/../s13-qualification/qualification.json'");
save('stage.mjs',stage);
let prepare=adapt(readFileSync(parent+'/prepare.mjs','utf8')).replace("base=repo+'/docs/evidence/frontier/v10'","base=repo+'/docs/evidence/frontier/v11'").replace(old+'/qualification.mjs',parent+'/s13-qualification/qualification.mjs').replace("readFileSync(base+'/driver/'+name)","readFileSync(repo+'/docs/evidence/frontier/v4/driver/'+name)").replace("readFileSync(base+'/source-manifest.json')","readFileSync(root+'/stage/SOURCE_MANIFEST.json')").replace("read(base+'/local-qualification.json').source_digest","read(root+'/../s13-qualification/qualification.json').source_digest").replace('database_source_changed:false','database_source_changed:true,effective_driver_unchanged_vs_v11:true,source_qualification_inputs:86');
save('prepare.mjs',prepare);
let redeploy=readFileSync(old+'/redeploy.mjs','utf8').replaceAll(old,root).replace('3bb14b95-0468-455c-9e3d-7f55d483ec45','87eb3575-ec4b-4c51-9c62-7eb5285ef41e').replace('b6849f53-4623-4a1d-b21f-6940fab9e235','fd3d1f84-8efc-4d27-8296-a67733599d89');
save('redeploy.mjs',redeploy);
console.log(JSON.stringify({helpers:names.length+3,verbatim_receipts:4,compute_started:false}));
