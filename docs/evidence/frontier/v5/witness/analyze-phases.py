import json, re, sys
from pathlib import Path
root=Path('/tmp/varve-reuse.k2eXIP')
before,after=(json.loads((root/name).read_text()) for name in sys.argv[1:])
assert before['status']['database_id']==after['status']['database_id']
def parse(text):
    phases={}; workers={}
    for line in text.splitlines():
        match=re.fullmatch(r'varve_phase_duration_seconds_(count|sum)\{phase="([^"]+)"\} (.+)',line)
        if match:
            kind,phase,value=match.groups()
            phases.setdefault(phase,{})[kind]=float(value)
        if line.startswith('varve_query_workers_'):
            key,value=line.rsplit(' ',1)
            workers[key]=float(value)
    return phases,workers
bp,bw=parse(before['metrics']); ap,aw=parse(after['metrics'])
assert len(ap)==18 and ap.keys()==bp.keys()
rows=[]
for name in ap:
    count=ap[name]['count']-bp[name]['count']; seconds=ap[name]['sum']-bp[name]['sum']
    assert count>=0 and seconds>=0
    rows.append({'phase':name,'observations':int(count),'cumulative_seconds':round(seconds,6),'mean_ms':round(1000*seconds/count,4) if count else None})
workers={key:(value-bw[key] if key.endswith('_total') else value) for key,value in aw.items()}
print(json.dumps({'scope':'phase intervals overlap; sums are not exclusive CPU/wall time; working-byte gauge is not a peak','phases':sorted(rows,key=lambda row:row['cumulative_seconds'],reverse=True),'workers':workers,'sizes':{key:after['status'][key] for key in ('metadata_bytes','control_root_bytes','derived_encoded_bytes','derived_resident_bytes','derived_working_bytes')},'memory_peak':after['memory_peak'],'memory_events':after['memory_events']}))
