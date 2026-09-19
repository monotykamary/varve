import asyncio, json, os, pwd, sys, hashlib
from pathlib import Path
import psycopg

async def main():
    result = {'event':'frontier_driver_preflight','ssh_uid':os.getuid(),'benchmark_uid':pwd.getpwnam('benchmark').pw_uid,'python':sys.version,'region':os.environ.get('RAILWAY_REPLICA_REGION'),'cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text().strip(),'memory_max':Path('/sys/fs/cgroup/memory.max').read_text().strip(),'files':{name:hashlib.sha256(Path('/app',name).read_bytes()).hexdigest() for name in ('benchmark.py','core.py','Dockerfile','requirements.txt')}}
    async with await psycopg.AsyncConnection.connect(host=os.environ['PGHOST'],port=os.environ['PGPORT'],user=os.environ['PGUSER'],password=os.environ['PGPASSWORD'],dbname=os.environ['PGDATABASE'],connect_timeout=10,autocommit=True) as connection:
        cursor=await connection.execute("SELECT current_setting('server_version'), current_setting('data_directory'), pg_postmaster_start_time()::text, (SELECT extversion FROM pg_extension WHERE extname='timescaledb'), (SELECT count(*) FROM pg_class WHERE relname LIKE 'varve_%'), current_setting('fsync'), current_setting('synchronous_commit'), current_setting('full_page_writes')")
        result['postgres']=await cursor.fetchone()
    assert result['region']=='asia-southeast1-eqsg3a'
    assert result['cpu_max']=='200000 100000'
    assert int(result['memory_max'])==999997440
    assert result['postgres'][1]=='/var/lib/postgresql/data/accounting-s13-r1'
    assert result['postgres'][3]=='2.30.0'
    assert result['postgres'][4]==0
    assert tuple(result['postgres'][5:])==('on','on','on')
    print(json.dumps(result),flush=True)
asyncio.run(main())
