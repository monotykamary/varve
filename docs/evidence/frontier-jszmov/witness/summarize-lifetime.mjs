import fs from 'node:fs';
import crypto from 'node:crypto';
const root='/tmp/varve-frontier-gate.JsZmOV';
const names=['lifetime-release-fresh-large-01','lifetime-release-fresh-load-01'];
const summaries=names.map(name=>{
 const report=JSON.parse(fs.readFileSync(`${root}/${name}/report.json`));
 const metrics=report.load_phase_deltas;
 const phases=['snapshot','query_wait','query_build','query_run','query_spawn','query_reset','checkpoint_prepare','checkpoint_reclaim','commit_lock_wait','wal_file_sync','wal_directory_sync'].map(phase=>({phase,seconds:metrics[`varve_phase_duration_seconds_sum{phase="${phase}"}`],count:metrics[`varve_phase_duration_seconds_count{phase="${phase}"}`]}));
 const inventory=JSON.parse(fs.readFileSync(`${root}/${name}/sha256.json`));
 for(const [file,expected]of Object.entries(inventory)){
  if(file.includes('/')||file.includes('..'))throw Error('invalid artifact entry');
  const actual=crypto.createHash('sha256').update(fs.readFileSync(`${root}/${name}/${file}`)).digest('hex');
  if(actual!==expected)throw Error('artifact hash mismatch: '+file);
 }
 return {name,status:report.status,binary_sha256:report.identity.binary_sha256,profile_sha256:report.identity.profile_sha256,rows:report.oracles.n,offered:report.load.offered,acknowledged:report.load.acknowledged,dropped:report.load.dropped,failed:report.load.failed,pending:report.load.pending,owned_server_reaped:report.owned_server_reaped,write_samples:report.load.ack_latency.samples,write_p95_ms:report.load.ack_latency.p95_ms,arrival_p95_ms:report.load.arrival_latency.p95_ms,read_samples:report.load.read_latency.samples,read_p95_ms:report.load.read_latency.p95_ms,measured_rows_per_s:report.load.acknowledged_rows_per_s,phases};
});
fs.writeFileSync(`${root}/lifetime-release-summary.json`,JSON.stringify({claims:{timescale_win:false,cloud_qualification:false,p99_qualified:false,independent_read_arrivals:false},summaries},null,2)+'\n');
console.log(JSON.stringify(summaries,null,2));
