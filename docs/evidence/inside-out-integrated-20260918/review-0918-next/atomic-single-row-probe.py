import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path

from psycopg import errors, sql
from benchmark import TimescaleClient, artifact_metadata, copy_batch, load_credentials, names_for, qualified, redactor, setup_timescale
from core import Deadline, measurement
from verify_exact import _close_clients

output = Path('/results/ior6_atomic_02.json')
with output.open('x'):
    pass
credentials = load_credentials()
report = {'state': 'running', 'run_id': 'ior6_atomic_02', 'driver_artifact': artifact_metadata(), 'probe_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}

async def run():
    deadline = Deadline(45)
    pg = TimescaleClient(credentials, deadline)
    names = names_for(report['run_id'])
    base = json.loads(Path('/results/ior6_smoke_04.json').read_text())['manifest']['base_timestamp_us']
    report['namespace'] = names.pg_schema
    table, receipts = qualified(names.pg_schema, names.pg_table), qualified(names.pg_schema, names.pg_receipts)
    try:
        await pg.open(2)
        settings = await pg.query("SELECT current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS synchronous_commit, current_setting('full_page_writes') AS full_page_writes")
        assert all(value == 'on' for value in settings[0].values()), settings
        report['durability'] = settings[0]
        await setup_timescale(pg, names)
        await pg.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT probe_reject_42 CHECK (value <> 42)').format(table))
        rejected = replace(measurement(0, base), value_q=168)
        try:
            await copy_batch(pg.writers[0], names, 'rollback', [rejected], deadline)
            raise AssertionError('event constraint failure was not observed')
        except errors.CheckViolation:
            pass
        for relation in (table, receipts):
            count = await pg.query(sql.SQL('SELECT count(*) AS n FROM {}').format(relation))
            assert count == [{'n': 0}], count
        assert await copy_batch(pg.writers[0], names, 'rollback', [measurement(0, base)], deadline) is True
        report['failed_event_rolls_back_receipt'] = True
        same = measurement(1, base)
        results = await asyncio.gather(*(copy_batch(connection, names, 'same', [same], deadline) for connection in pg.writers))
        assert sorted(results) == [False, True], results
        report['concurrent_identical_one_fresh_one_duplicate'] = True
        candidates = [measurement(2, base), measurement(3, base)]
        results = await asyncio.gather(*(copy_batch(connection, names, 'conflict', [row], deadline) for connection, row in zip(pg.writers, candidates)), return_exceptions=True)
        winners = [index for index, result in enumerate(results) if result is True]
        conflicts = [result for result in results if isinstance(result, RuntimeError) and 'canonical payload digest' in str(result)]
        assert len(winners) == 1 and len(conflicts) == 1, [str(result) for result in results]
        expected = sorted([measurement(0, base).value, same.value, candidates[winners[0]].value])
        actual = await pg.query(sql.SQL('SELECT value FROM {} ORDER BY value').format(table))
        assert [row['value'] for row in actual] == expected, actual
        counts = await pg.query(sql.SQL('SELECT count(*) AS n, sum(row_count) AS rows FROM {}').format(receipts))
        assert counts == [{'n': 3, 'rows': 3}], counts
        report['concurrent_conflict_exactly_one_event'] = True
        report['raw_rows'] = 3
        report['receipts'] = 3
        report['state'] = 'passed'
    finally:
        await _close_clients(pg)

try:
    asyncio.run(asyncio.wait_for(run(), 50))
except BaseException as error:
    report['state'] = 'failed'
    report['failure'] = redactor(credentials)(error)
output.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + '\n')
print(json.dumps({'state': report['state'], 'output': str(output)}))
raise SystemExit(0 if report['state'] == 'passed' else 1)
