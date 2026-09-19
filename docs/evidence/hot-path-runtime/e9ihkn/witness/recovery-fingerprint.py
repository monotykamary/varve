import asyncio
import json
import os
from pathlib import Path
import sys
if os.geteuid() == 0:
    os.setgid(10001)
    os.setuid(10001)
assert os.geteuid() == 10001
sys.path.insert(0, '/app')
import benchmark
from core import Deadline, checked_identifier
report = json.loads(Path('/results/runtime_e9ihkn_s1.json').read_text())
assert report['state'] == 'passed'
manifest = report['manifest']
base = manifest['base_timestamp_us']
initial = manifest['initial_rows']
mixed = manifest['mixed_acknowledged_rows']
assert initial == 250000 and mixed == 300000
expected = {'n': initial + mixed, 'ts_sum': 0, 'ts_square_sum': 0, 'ts_value_sum': 0}
for late, count in ((False, initial), (True, mixed)):
    for index in range(count):
        timestamp = base - 60000000 - (index % 16) * 1000000 + (index * 73) % 1024 if late else base + (index // 1024) * 1000000 + index % 1024
        value_q = -4000 + (index * 29) % 8000 if late else (index * 17) % 10000
        expected['ts_sum'] += timestamp
        expected['ts_square_sum'] += timestamp * timestamp
        expected['ts_value_sum'] += timestamp * value_q
expected = {key: str(value) for key, value in expected.items()}
credentials = benchmark.load_credentials()
async def run():
    deadline = Deadline(90)
    varve = benchmark.VarveClient(credentials['VARVE_URL'], credentials['VARVE_API_TOKEN'], deadline, 1)
    pg = benchmark.TimescaleClient(credentials, deadline)
    try:
        await pg.open(0)
        table = checked_identifier(report['namespaces']['varve_table'])
        schema = checked_identifier(report['namespaces']['pg_schema'])
        varve_sql = f"SELECT count(*)::VARCHAR AS n, sum(t)::VARCHAR AS ts_sum, sum(t*t)::VARCHAR AS ts_square_sum, sum(t*q)::VARCHAR AS ts_value_sum FROM (SELECT timestamp_us::HUGEINT AS t, CAST(value*4 AS HUGEINT) AS q FROM {table})"
        pg_sql = f'SELECT count(*)::text AS n, sum(t)::numeric(38,0)::text AS ts_sum, sum(t*t)::numeric(38,0)::text AS ts_square_sum, sum(t*q)::numeric(38,0)::text AS ts_value_sum FROM (SELECT (extract(epoch FROM ts)*1000000)::numeric(38,0) AS t, (value*4)::numeric(38,0) AS q FROM "{schema}"."measurements") s'
        vrows, prows = await asyncio.gather(varve.sql(varve_sql), pg.query(pg_sql))
        assert vrows == [expected] and prows == [expected], 'independent timestamp/value fingerprint mismatch'
        status = await varve.request('GET', '/v1/status')
        pgstate = await pg.query("SELECT pg_postmaster_start_time() AS started, (SELECT extversion FROM pg_extension WHERE extname='timescaledb') AS timescaledb_version, current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes")
        receipts = await pg.query(f'SELECT count(*) AS n FROM "{schema}"."batch_receipts"')
        assert status['idempotency_keys'] == 550 and receipts[0]['n'] == 550
        assert status['fenced'] is None and status['last_maintenance_error'] is None
        assert pgstate[0]['timescaledb_version'] == '2.30.0'
        assert all(pgstate[0][name] == 'on' for name in ('fsync','synchronous_commit','full_page_writes'))
        print(json.dumps({'passed': True, 'expected': expected, 'varve': vrows[0], 'timescale': prows[0], 'status': status, 'postgres': pgstate[0], 'receipts_per_backend': 550, 'driver_deployment': os.environ['RAILWAY_DEPLOYMENT_ID']}, default=str))
    finally:
        await varve.close()
        await pg.close()
try:
    asyncio.run(asyncio.wait_for(run(), timeout=95))
except BaseException as error:
    print(json.dumps({'passed': False, 'error': benchmark.redactor(credentials)(error)}))
    raise SystemExit(1)
