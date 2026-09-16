import json, sys
from pathlib import Path
run_id=sys.argv[1]
assert run_id in ('reuse003','reuse004','reuse003.verify','reuse004.verify')
files={}
for suffix in ('.json','.log','.launch.json'):
    path=Path('/results')/(run_id+suffix)
    if path.exists():
        files[path.name]=path.read_text()
print(json.dumps({'files':files,'report_ready':run_id+'.json' in files}),flush=True)
