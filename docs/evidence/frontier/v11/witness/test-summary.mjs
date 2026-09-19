import {readFileSync,writeFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {createHash} from 'node:crypto';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const source=readFileSync(root+'/unpack.mjs','utf8'),code=source.replace(/^import .*;\n/gm,'');
const complete=JSON.parse(readFileSync(repo+'/docs/evidence/frontier/v9/prefix001.json','utf8'));
const failed=JSON.parse(readFileSync(repo+'/docs/evidence/frontier/v10/scoped001.json','utf8'));
const withReads=structuredClone(failed);
withReads.mixed_workload.varve_concurrent_read_ms={raw:[1,2],summary:{samples:2,p95_ms:2}};
withReads.mixed_workload.timescale_concurrent_read_ms={raw:[3,4],summary:{samples:2,p95_ms:4}};
const absent={state:'failed',failure:'initial failure',finished_at:'done',initial_ingest:{varve:{state:'failed'}}};
let cases=0;
for(const report of [complete,failed,withReads,absent])for(const id of ['diag101','diag102']){
 const output=[],writes=[];
 vm.runInNewContext(code,{process:{argv:['node','unpack',id],exit(){throw Error('Unexpected exit');}},readFileSync(path){assert.equal(path,root+'/'+id+'-bundle.json');return JSON.stringify({files:{[id+'.json']:JSON.stringify(report)}});},writeFileSync(path,text){writes.push([path,JSON.parse(text)]);},console:{log(text){output.push(JSON.parse(text));}}},{timeout:1000});
 assert.equal(writes.length,1);assert.deepEqual(writes[0][1],report);assert.equal(output[0].state,report.state);assert.equal(output[0].failure,report.failure);
 if(report.mixed_workload){const m=output[0].mixed;assert.equal(m.failed_or_ambiguous,report.mixed_workload.failed_or_ambiguous_rows);assert.equal(m.varve_read_p95_ms,report.mixed_workload.varve_concurrent_read_ms?.summary.p95_ms??null);assert.equal(m.read_samples,report.mixed_workload.varve_concurrent_read_ms?.summary.samples??null);}
 else assert.equal(output[0].initial.varve.ack_p95_ms,null);
 cases++;
}
for(const id of ['scoped001','prefix001','../diag101','diag103','diag101.extra']){
 assert.throws(()=>vm.runInNewContext(code,{process:{argv:['node','unpack',id]},readFileSync(){throw Error('Unexpected read');},writeFileSync(){throw Error('Unexpected write');}},{timeout:1000}),/Unknown run ID/);cases++;
}
const result={cases,complete_and_failed_fixtures:true,raw_reports_unchanged:true,missing_read_metrics_are_null_not_zero:true,external_calls:0,unpacker_sha256:createHash('sha256').update(source).digest('hex')};
writeFileSync(root+'/summary-tests.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
