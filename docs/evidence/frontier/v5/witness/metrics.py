import http.client, json, os, pwd
from datetime import datetime, timezone
from pathlib import Path
user=pwd.getpwnam('varve')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001
connection=http.client.HTTPConnection('::1',8080,timeout=30)
headers={'Authorization':'Bearer '+os.environ['VARVE_API_TOKEN']}
connection.request('GET','/metrics',headers=headers)
response=connection.getresponse()
text=response.read().decode()
assert response.status==200
connection.request('GET','/v1/status',headers=headers)
response=connection.getresponse()
status=json.loads(response.read())
assert response.status==200
connection.close()
assert 'varve_phase_duration_seconds_count' in text
assert all(key in status for key in ('control_root_bytes','derived_encoded_bytes','derived_resident_bytes','derived_working_bytes'))
print(json.dumps({'at':datetime.now(timezone.utc).isoformat(),'scope':'untimed phase-boundary observation; phase timers overlap and are not exclusive CPU totals','status':status,'metrics':text,'memory_peak':int(Path('/sys/fs/cgroup/memory.peak').read_text()),'memory_events':Path('/sys/fs/cgroup/memory.events').read_text()}),flush=True)
