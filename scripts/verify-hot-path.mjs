#!/usr/bin/env node
import {spawnSync} from 'node:child_process';
import {mkdtempSync,writeFileSync,readFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

// This is an explicit low-level test selection, not a sandbox or benchmark.
// Keep every prefix database-free when adding tests to these modules.
const prefixes = [
  'engine::hot_path_tests::',
  'engine::write_input::tests::',
  'wal::tests::',
  'wal::dependency_publication::tests::',
  'query::workers::resident::descriptor_delta_tests::',
];
const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
function sourceInputs(){
  const files=['Cargo.toml','Cargo.lock','scripts/verify-hot-path.mjs'];
  function visit(relative){
    for(const entry of readdirSync(join(root,relative),{withFileTypes:true})){
      const file=join(relative,entry.name);
      if(entry.isDirectory())visit(file);
      else if(entry.isFile())files.push(file);
    }
  }
  visit('src');
  return Object.fromEntries(files.sort().map(file=>[file,createHash('sha256').update(readFileSync(join(root,file))).digest('hex')]));
}
const source=sourceInputs();
const evidence=mkdtempSync(join(tmpdir(),'varve-hot-path-'));
const environment={...process.env,CARGO_NET_OFFLINE:'true'};
delete environment.VARVE_FAILPOINT;
delete environment.VARVE_IO_FAILPOINT;
function run(command,args,name,timeout=240000){
  const result=spawnSync(command,args,{cwd:root,env:environment,encoding:'utf8',timeout,maxBuffer:16*1024*1024,stdio:['ignore','pipe','pipe']});
  writeFileSync(join(evidence,name+'.stdout'),result.stdout??'');
  writeFileSync(join(evidence,name+'.stderr'),result.stderr??'');
  if(result.error||result.status!==0){
    console.error((result.stderr??'').slice(-10000));
    console.error((result.stdout??'').slice(-16000));
    throw Error(`${name} failed: ${result.error?.message??result.status}; evidence ${evidence}`);
  }
  return result.stdout;
}
const build=run('cargo',['test','--offline','--locked','--lib','--features','fault-injection','--no-run','--message-format=json'],'compile');
const artifacts=build.split('\n').filter(Boolean).map(line=>JSON.parse(line));
const executables=artifacts.filter(x=>x.reason==='compiler-artifact'&&x.target?.name==='varve'&&x.profile?.test&&x.executable).map(x=>x.executable);
if(executables.length!==1)throw Error('Expected exactly one current library test binary');
const executable=executables[0];
const listing=run(executable,['--list','--format','terse'],'list');
const tests=listing.split('\n').filter(line=>line.endsWith(': test')).map(line=>line.slice(0,-6));
const summaries=[];
for(const [index,prefix]of prefixes.entries()){
  const names=tests.filter(name=>name.startsWith(prefix));
  if(!names.length)throw Error(`Offline prefix selected zero tests: ${prefix}`);
  const output=run(executable,[prefix,'--test-threads=1','--nocapture'],`suite-${index}`,120000);
  const summary=output.match(/test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;/);
  if(!summary||Number(summary[1])!==names.length||Number(summary[2])||Number(summary[3]))throw Error(`Incomplete offline suite ${prefix}`);
  summaries.push({prefix,passed:names.length,tests:names});
  console.log(`${prefix} ${names.length} passed`);
}
assert.deepEqual(sourceInputs(),source,'Source changed during offline verification');
const source_sha256=createHash('sha256').update(JSON.stringify(source)).digest('hex');
const receipt={passed:true,source_sha256,source_inputs:source,scope:'Explicit pure state/input/codec and low-level tempfile tests; no database runtime qualification or performance claim',total:summaries.reduce((n,s)=>n+s.passed,0),suites:summaries,executable,evidence};
writeFileSync(join(evidence,'receipt.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify({passed:true,total:receipt.total,source_sha256,evidence}));
