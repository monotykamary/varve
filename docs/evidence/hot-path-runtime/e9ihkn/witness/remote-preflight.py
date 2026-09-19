import asyncio
import json
import os
import sys
from pathlib import Path

if os.geteuid() == 0:
    os.setgid(10001)
    os.setuid(10001)
assert os.geteuid() == 10001
sys.path.insert(0, '/app')
import benchmark
from core import Deadline

credentials = benchmark.load_credentials()

async def run():
    deadline = Deadline(60)
    varve = benchmark.VarveClient(credentials['VARVE_URL'], credentials['VARVE_API_TOKEN'], deadline, 1)
    pg = benchmark.TimescaleClient(credentials, deadline)
    try:
        await pg.open(0)
        checks = await benchmark.preflight(varve, pg, benchmark.names_for(sys.argv[1]))
        ready = await varve.request('GET', '/ready')
        status = await varve.request('GET', '/v1/status')
        postgres = await pg.query("SELECT pg_postmaster_start_time() AS started, current_setting('data_directory') AS data_directory")
        active = await pg.query("SELECT count(*) AS active_other_clients FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type = 'client backend' AND state = 'active'")
        settings = await pg.query("SELECT name, setting, unit FROM pg_settings WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'max_parallel_workers_per_gather', 'max_parallel_workers', 'max_worker_processes', 'timescaledb.max_background_workers', 'timescaledb.telemetry_level') ORDER BY name")
        resources = {}
        for name in ('cpu.max', 'memory.max', 'memory.current', 'cpu.stat'):
            path = Path('/sys/fs/cgroup') / name
            if path.is_file():
                resources[name] = path.read_text().strip()
        print(json.dumps({'passed': True, 'uid': os.geteuid(), 'identity': {key: os.environ.get(key) for key in ('RAILWAY_PROJECT_ID', 'RAILWAY_ENVIRONMENT_ID', 'RAILWAY_SERVICE_ID', 'RAILWAY_DEPLOYMENT_ID')}, 'driver': benchmark.artifact_metadata(), 'checks': checks, 'ready': ready, 'status': status, 'postgres': postgres, 'settings': settings, 'activity': active, 'generator_cgroups': resources}, default=str))
    finally:
        await varve.close()
        await pg.close()

try:
    asyncio.run(asyncio.wait_for(run(), timeout=65))
except BaseException as error:
    print(json.dumps({'passed': False, 'error': benchmark.redactor(credentials)(error)}))
    raise SystemExit(1)
