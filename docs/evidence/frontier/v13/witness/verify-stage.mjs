import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const sha=data=>createHash('sha256').update(data).digest('hex'),read=path=>JSON.parse(readFileSync(path,'utf8'));
const qualification=read(root+'/../s13-qualification/qualification.json'),manifest=read(root+'/stage/SOURCE_MANIFEST.json'),staged=read(root+'/PRIOR-s13-staged.json');
assert.equal(qualification.source_digest,'58f405adf7d472fdccf0011bc0bcc3f041201f9792ec1a7ab372002b1803944a');
assert.equal(qualification.files.length,86);assert.equal(qualification.rust_passed,410);assert.equal(qualification.typescript_unit+qualification.typescript_real_service,19);
assert.equal(manifest.local_qualified_source,qualification.source_digest);assert.equal(manifest.files.length,56);
const expected=[...new Set([...execFileSync('tar',['-tzf',repo+'/docs/evidence/frontier/v4/source.tar.gz'],{encoding:'utf8'}).trim().split('\n').filter(p=>p!=='SOURCE_MANIFEST.json'),...qualification.files.filter(f=>f.path.endsWith('.rs')&&(f.path.startsWith('src/')||f.path.startsWith('clients/rust/'))).map(f=>f.path),'.dockerignore'])].sort();
assert.deepEqual(manifest.files.map(f=>f.path),expected);
for(const file of manifest.files){
 let bytes=readFileSync(repo+'/'+file.path);
 if(file.path==='Dockerfile')bytes=Buffer.from(bytes.toString().replace('WORKDIR /app\n','COPY SOURCE_MANIFEST.json /usr/share/doc/varve/benchmark-source.json\nWORKDIR /app\n'));
 assert.equal(sha(bytes),file.sha256,file.path+' current source');assert.equal(bytes.length,file.bytes);
 assert.equal(sha(readFileSync(root+'/stage/'+file.path)),file.sha256,file.path+' stage');
 assert.equal(sha(execFileSync('tar',['-xOf',root+'/source.tar.gz',file.path])),file.sha256,file.path+' archive');
}
const bytes=readFileSync(root+'/stage/SOURCE_MANIFEST.json');
assert.equal(sha(bytes),staged.manifest_sha256);assert.equal(sha(readFileSync(root+'/source-manifest.json')),sha(bytes));assert.equal(sha(execFileSync('tar',['-xOf',root+'/source.tar.gz','SOURCE_MANIFEST.json'])),sha(bytes));
assert.equal(sha(readFileSync(root+'/source.tar.gz')),staged.archive_sha256);
assert.equal(sha(readFileSync(root+'/runtime-config.json')),'215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089');
console.log(JSON.stringify({build_files:56,qualified_inputs:86,source_digest:qualification.source_digest,manifest_sha256:sha(bytes),archive_sha256:staged.archive_sha256,stage_matches_current_qualified_source:true,docker_receipt_injection_only:true,compute_started:false}));
