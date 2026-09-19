import hashlib, json, os, pwd, subprocess, sys
from pathlib import Path
from datetime import datetime, timezone
run_id, mode=sys.argv[1:]
assert run_id in ('acct101','acct102') and mode in ('baseline','stress')
user=pwd.getpwnam('benchmark')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001
os.umask(0o077)
driver_dir=Path('/results/driver-accounting-s13-r1')
installed=json.loads((driver_dir/'INSTALL_MANIFEST.json').read_text())
effective_files={name:hashlib.sha256((driver_dir/name).read_bytes()).hexdigest() for name in installed['effective_files']}
assert effective_files==installed['effective_files']
assert effective_files['benchmark.py']=='23e05928cf287a4f8b59433f2079cf63610a256dabbb75138a2ea5081442df23'
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
    process=subprocess.Popen(['timeout','--kill-after=10s',str(seconds)+'s',sys.executable,'-u',str(driver_dir/'benchmark.py'),*args],stdin=subprocess.DEVNULL,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,cwd=str(driver_dir))
record={'event':'benchmark_launch','run_id':run_id,'mode':mode,'pid':process.pid,'uid':os.getuid(),'gid':os.getgid(),'hard_ceiling_seconds':seconds,'at':datetime.now(timezone.utc).isoformat(),'arguments':args,'effective_driver_files':effective_files,'driver_path':str(driver_dir/'benchmark.py')}
proof.write_text(json.dumps(record)+'\n')
print(json.dumps(record),flush=True)
