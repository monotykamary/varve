import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const prepared=JSON.parse(readFileSync(root+'/prepared.json','utf8'));
const original=JSON.parse(readFileSync(repo+'/docs/evidence/frontier/v10/runtime-driver.json','utf8'));
const source=readFileSync(root+'/check-driver.mjs','utf8');
const cases=['valid','effective_hash','base_hash','scope_hash','uid','path','fsync','existing_tables','python'];
for(const test of cases){
 const runtime=structuredClone(original),installed={base_files:prepared.base_driver_files,effective_files:structuredClone(prepared.effective_driver_files),driver_scope_sha256:prepared.driver_scope_sha256,uid:10001,directory:'/results/driver-accounting-s13'};
 if(test==='effective_hash')installed.effective_files['benchmark.py']='wrong';
 if(test==='base_hash')installed.base_files={};
 if(test==='scope_hash')installed.driver_scope_sha256='wrong';
 if(test==='uid')installed.uid=0;
 if(test==='path')installed.directory='/app';
 if(test==='fsync')runtime.postgres[5]='off';
 if(test==='existing_tables')runtime.postgres[4]=1;
 if(test==='python')runtime.python='other';
 const outputs=[];let failure;
 try{vm.runInNewContext(source.replace(/^import .*;\n/gm,''),{assert,createHash,console:{log(text){outputs.push(JSON.parse(text));}},readFileSync(path,encoding){if(path===root+'/runtime-driver.json')return JSON.stringify(runtime);if(path===root+'/runtime-driver-effective.json')return JSON.stringify(installed);if(path===root+'/prepared.json')return JSON.stringify(prepared);assert(path.startsWith(repo+'/docs/evidence/frontier/v4/'));return readFileSync(path,encoding);}},{timeout:1000});}catch(error){failure=error;}
 if(test==='valid'){assert.equal(failure,undefined);assert.equal(outputs[0].base_driver_files_identical,true);assert.equal(outputs[0].effective_driver_reporting_only,true);}
 else{assert(failure,test+' must reject');assert.equal(outputs.length,0);}
}
const result={cases:cases.length,external_commands:0,infra_mutations:0,check_driver_sha256:createHash('sha256').update(source).digest('hex')};
writeFileSync(root+'/attestation-tests.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
