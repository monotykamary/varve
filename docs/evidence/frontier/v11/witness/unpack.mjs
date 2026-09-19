import {readFileSync,writeFileSync} from 'node:fs';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config',id=process.argv[2];
if(!/^diag10[12]$/.test(id))throw Error('Unknown run ID');
const bundle=JSON.parse(readFileSync(root+'/'+id+'-bundle.json','utf8'));
for(const[name,content]of Object.entries(bundle.files)){if(!['.json','.log','.launch.json'].some(suffix=>name===id+suffix))throw Error('Unexpected evidence name');writeFileSync(root+'/'+name,content,{mode:0o600});}
const report=bundle.files[id+'.json']?JSON.parse(bundle.files[id+'.json']):null;
if(!report){console.log(JSON.stringify({state:'no_report',log_tail:bundle.files[id+'.log']?.slice(-800)}));process.exit(0);}
const summary={state:report.state,started:report.started_at,finished:report.finished_at,failure:report.failure,watermark:report.manifest?.total_committed_watermark_rows};
summary.initial=Object.fromEntries(Object.entries(report.initial_ingest??{}).map(([backend,data])=>[backend,{rows:data.rows,rows_per_second:data.rows_per_second,seconds:data.seconds,ack_p95_ms:data.ack_latency_ms?.summary?.p95_ms ?? null}]));
if(report.mixed_workload){const m=report.mixed_workload;summary.mixed={offered:m.offered_rows,acknowledged:m.acknowledged_rows,dropped:m.dropped_rows,failed_or_ambiguous:m.failed_or_ambiguous_rows,seconds:m.seconds,varve_ack_p95_ms:m.varve_ack_latency_ms?.summary?.p95_ms ?? null,timescale_ack_p95_ms:m.timescale_ack_latency_ms?.summary?.p95_ms ?? null,varve_read_p95_ms:m.varve_concurrent_read_ms?.summary?.p95_ms ?? null,timescale_read_p95_ms:m.timescale_concurrent_read_ms?.summary?.p95_ms ?? null,read_samples:m.varve_concurrent_read_ms?.summary?.samples ?? null};}
summary.query_p95_ms=Object.fromEntries(Object.entries(report.query_stages??{}).map(([stage,data])=>[stage,Object.fromEntries(Object.entries(data.backends).map(([backend,queries])=>[backend,Object.fromEntries(Object.entries(queries).map(([name,q])=>[name,q.latency_ms?.summary?.p95_ms ?? null]))]))]));
console.log(JSON.stringify(summary));
