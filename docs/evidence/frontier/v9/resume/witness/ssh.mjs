import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
const roles={varve:'0aff95bb-3f5a-45fe-bda0-195aa742938a',driver:'76fdbde0-4296-46a4-a6f2-4026432fd4a3',timescale:'acc76115-5c5f-4be4-901d-fde4adbcbacf'};
const [role,path,...args]=process.argv.slice(2);
if(!roles[role]||!path)throw Error('Expected owned role and local script');
const encoded=readFileSync(path).toString('base64');
const expr="exec(__import__('base64').b64decode('"+encoded+"'))";
const command=path.endsWith('.sh')?['sh','-c',readFileSync(path,'utf8'),'varve-owned-probe']:[role==='driver'?'python':'python3','-c',expr];
let output;
try {
output=execFileSync('railway',['ssh','--project','8caffa15-0158-4822-a6c2-cb405bddc62d','--environment','5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1','--service',roles[role],'--',...command,...args],{encoding:'utf8',timeout:120000,maxBuffer:8*1024*1024,env:{...process.env,RAILWAY_CALLER:'skill:use-railway@1.4.0',RAILWAY_AGENT_SESSION:'varve-sediment-20260916'},stdio:['ignore','pipe','pipe']});
} catch(error) {
  if(error.stdout)process.stdout.write(error.stdout);
  process.stderr.write('Owned SSH probe failed (status '+String(error.status)+'); '+String(error.stderr??'').split('\n').filter(line=>!line.startsWith('Using SSH key')).slice(-6).join('\n')+'\n');
  process.exit(1);
}
if(!output.trim())throw Error('SSH returned no proof output');
process.stdout.write(output);
