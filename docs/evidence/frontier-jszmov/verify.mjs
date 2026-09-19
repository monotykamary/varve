import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=fs.realpathSync(path.dirname(fileURLToPath(import.meta.url)));
const manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json')));
const seen=new Set();
for(const entry of manifest.files){
 if(path.isAbsolute(entry.path)||entry.path.split('/').includes('..')||seen.has(entry.path))throw Error('invalid inventory path');
 seen.add(entry.path);
 const file=path.join(root,entry.path);
 if(!fs.realpathSync(file).startsWith(root+path.sep)||!fs.lstatSync(file).isFile())throw Error('artifact outside evidence directory');
 const bytes=fs.readFileSync(file);
 if(bytes.length!==entry.bytes||crypto.createHash('sha256').update(bytes).digest('hex')!==entry.sha256)throw Error('artifact mismatch: '+entry.path);
}
console.log(JSON.stringify({verified:true,artifacts:seen.size,claims:manifest.claims}));
