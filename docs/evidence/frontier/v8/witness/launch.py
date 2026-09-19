import json, os, pwd, subprocess, sys
from pathlib import Path
from datetime import datetime, timezone
run_id, mode=sys.argv[1:]
assert run_id in ('resident001','resident002') and mode in ('baseline','stress')
user=pwd.getpwnam('benchmark')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001
os.umask(0o077)
output=Path('/results')/(run_id+'.json')
log=output.with_suffix('.log')
proof=output.with_suffix('.launch.json')
assert not any(path.exists() for path in (output,log,proof)), 'run ID already used'
args=['--run-id',run_id,'--output',str(output),'--batch','1000','--writers','4']
if mode=='baseline':
    seconds=1200
    args+=['--rows','250000','--query-samples','50','--mixed-seconds','60','--rate','5000','--max-seconds',str(seconds)]
else:
    seconds=600
    args+=['--rows','1000000','--query-samples','30','--mixed-seconds','30','--rate','20000','--max-seconds',str(seconds)]
with log.open('xb') as stream:
    process=subprocess.Popen(['timeout','--kill-after=10s',str(seconds)+'s',sys.executable,'-u','/app/benchmark.py',*args],stdin=subprocess.DEVNULL,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,cwd='/app')
record={'event':'benchmark_launch','run_id':run_id,'mode':mode,'pid':process.pid,'uid':os.getuid(),'gid':os.getgid(),'hard_ceiling_seconds':seconds,'at':datetime.now(timezone.utc).isoformat(),'arguments':args}
proof.write_text(json.dumps(record)+'\n')
print(json.dumps(record),flush=True)
