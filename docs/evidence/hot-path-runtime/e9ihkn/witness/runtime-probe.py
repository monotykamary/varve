import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

role = sys.argv[1]
manifest_bytes = Path("/usr/share/doc/varve/source-manifest.json").read_bytes() if role == "varve" else None
if os.geteuid() == 0:
    os.setgid(10001)
    os.setuid(10001)
assert os.geteuid() == 10001
role = sys.argv[1]
result = {'uid': os.geteuid(), 'identity': {key: os.environ.get(key) for key in ('RAILWAY_PROJECT_ID', 'RAILWAY_ENVIRONMENT_ID', 'RAILWAY_SERVICE_ID', 'RAILWAY_DEPLOYMENT_ID')}}
result['cgroups'] = {name: (Path('/sys/fs/cgroup') / name).read_text().strip() for name in ('cpu.max', 'memory.max', 'memory.current', 'cpu.stat')}
if role == 'varve':
    processes = []
    for directory in Path('/proc').iterdir():
        if not directory.name.isdecimal():
            continue
        try:
            if (directory / 'comm').read_text().strip() == 'varve':
                status = (directory / 'status').read_text().splitlines()
                uid = next(line for line in status if line.startswith('Uid:')).split()[1:]
                argv = [item.decode() for item in (directory / 'cmdline').read_bytes().split(b'\0') if item]
                processes.append({'pid': int(directory.name), 'uid': list(map(int, uid)), 'argv': argv, 'start_ticks': (directory / 'stat').read_text().split()[21], 'executable': os.readlink(directory / 'exe')})
        except FileNotFoundError:
            continue
    assert len(processes) == 1 and all(uid == 10001 for uid in processes[0]['uid'])
    result['process'] = processes[0]
    result['data_directory'] = os.environ['VARVE_DATA_DIR']
    result['volume_mounted'] = os.path.ismount('/data')
    result['root_exists'] = Path(result['data_directory']).is_dir()
    result['sha256'] = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in {'binary': '/usr/local/bin/varve', 'duckdb': '/usr/local/bin/duckdb', 'config': '/data/probes/benchmark.json', 'entrypoint': '/usr/local/bin/varve-entrypoint'}.items()}
    result['sha256']['source_manifest'] = hashlib.sha256(manifest_bytes).hexdigest()
    manifest = json.loads(manifest_bytes)
    result['source_sha256'] = manifest['source_sha256']
    result['duckdb_version'] = subprocess.check_output(['/usr/local/bin/duckdb', '--version'], text=True, timeout=10).strip()
    result['free_bytes'] = os.statvfs('/data').f_bavail * os.statvfs('/data').f_frsize
elif role == 'driver':
    sys.path.insert(0, '/app')
    import benchmark
    result['driver'] = benchmark.artifact_metadata()
    result['results_writable'] = os.access('/results', os.W_OK)
else:
    raise ValueError('unknown role')
print(json.dumps(result))
