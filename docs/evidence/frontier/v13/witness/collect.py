import json, sys
from pathlib import Path
run_id=sys.argv[1]
assert run_id in ('acct101','acct102','acct101.verify','acct102.verify')
files={}
for suffix in ('.json','.log','.launch.json'):
    path=Path('/results')/(run_id+suffix)
    if path.exists():
        files[path.name]=path.read_text()
print(json.dumps({'files':files,'report_ready':run_id+'.json' in files}),flush=True)
