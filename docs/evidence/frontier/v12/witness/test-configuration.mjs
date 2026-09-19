import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot',source=readFileSync(root+'/verify-ready.mjs','utf8');
const original=readFileSync(root+'/benchmark-config.json','utf8'),compact=JSON.stringify(JSON.parse(original))+'\n';
assert.deepEqual(JSON.parse(original),JSON.parse(compact));assert.notEqual(original.trim(),compact.trim());
for(const changed of [false,true]){
 const logs=[];let failure,qualifications=0;
 try{vm.runInNewContext(source.replace(/^import .*;\n/gm,''),{assert,createHash,console:{log(text){logs.push(JSON.parse(text));}},execFileSync(command,args){assert.equal(command,'node');assert([root+'/verify-stage.mjs','/tmp/varve-diagnostic.KOUrtH/retry-config/s13-qualification/qualification.mjs'].includes(args[0]));if(args[1]==='--check')qualifications++;return '';},readFileSync(path,encoding){if(path===root+'/configuration-tests.json')return JSON.stringify({cases:2,actual_readiness_source_exercised:true,semantically_equal_but_different_payload_rejected:true,verify_ready_sha256:createHash('sha256').update(source).digest('hex')});if(changed&&path===root+'/benchmark-config.json')return encoding?compact:Buffer.from(compact);return readFileSync(path,encoding);}},{timeout:1000});}catch(error){failure=error;}
 assert.equal(qualifications,1);
 if(changed){assert(failure?.message.includes('Actual environment payload bytes differ'));assert.equal(logs.length,0);}
 else{assert.equal(failure,undefined);assert.equal(logs.length,1);}
}
const result={cases:2,actual_readiness_source_exercised:true,semantically_equal_but_different_payload_rejected:true,external_commands:0,infra_mutations:0,verify_ready_sha256:createHash('sha256').update(source).digest('hex')};
writeFileSync(root+'/configuration-tests.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
