import asyncio, json, os, pwd, sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import benchmark
from core import Deadline

user=pwd.getpwnam('benchmark')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001

async def run():
    mode=sys.argv[1]
    assert mode in ('before','after')
    destination=Path('/results/scoped-recovery-'+mode+'.json')
    assert not destination.exists(), 'recovery proof already exists'
    credentials=benchmark.load_credentials()
    varve=benchmark.VarveClient(credentials['VARVE_URL'],credentials['VARVE_API_TOKEN'],Deadline(100),1)
    pg=benchmark.TimescaleClient(credentials,Deadline(100))
    result={'at':datetime.now(timezone.utc).isoformat(),'mode':mode,'uid':os.getuid(),'fingerprints':{}}
    try:
        await pg.open(1)
        result['postgres_start']=await pg.query('SELECT pg_postmaster_start_time()::text AS started_at')
        for run_id in ('acct101','acct102'):
            source=json.loads(Path('/results/'+run_id+'.json').read_text())
            assert source['state'] in ('passed','overloaded')
            assert source['mixed_workload']['failed_or_ambiguous_rows']==0
            expected_count=source['manifest']['total_committed_watermark_rows']
            names=benchmark.Names(**source['namespaces'])
            for name in names.json().values():
                benchmark.checked_identifier(name)
            raw_projection='count(*) AS count, sum(value) AS sum, min(value) AS min, max(value) AS max'
            vraw=(await varve.sql('SELECT '+raw_projection+', CAST(sum(CAST(timestamp_us AS HUGEINT)) AS VARCHAR) AS timestamp_sum FROM '+names.varve_table))[0]
            vagg=(await varve.sql('SELECT CAST(sum(count) AS BIGINT) AS count, sum(sum) AS sum, min(min) AS min, max(max) AS max FROM '+names.varve_aggregate))[0]
            praw=(await pg.query(benchmark.sql.SQL('SELECT count(*) AS count, sum(value)::double precision AS sum, min(value)::double precision AS min, max(value)::double precision AS max, sum((extract(epoch FROM ts)*1000000)::numeric)::text AS timestamp_sum FROM {}').format(benchmark.qualified(names.pg_schema,names.pg_table))))[0]
            pagg=(await pg.query(benchmark.sql.SQL('SELECT sum(count)::bigint AS count, sum(sum)::double precision AS sum, min(min)::double precision AS min, max(max)::double precision AS max FROM {}').format(benchmark.qualified(names.pg_schema,names.pg_aggregate))))[0]
            for label,raw,aggregate in [('varve',vraw,vagg),('timescale',praw,pagg)]:
                assert raw['count']==expected_count, label+' common watermark changed'
                expected=benchmark.expected_stats({key:raw[key] for key in ('count','sum','min','max')})
                benchmark.assert_stats(aggregate,expected,label+' retained aggregate')
            for raw in (vraw,praw):
                exact=Decimal(raw['timestamp_sum'])
                assert exact.is_finite() and exact==int(exact), 'fractional timestamp sum'
                raw['timestamp_sum']=str(int(exact))
            if vraw!=praw or vagg!=pagg:
                print(json.dumps({'event':'fingerprint_difference','run_id':run_id,'varve_raw':vraw,'timescale_raw':praw,'varve_aggregate':vagg,'timescale_aggregate':pagg}),flush=True)
                raise AssertionError('backend fingerprints disagree')
            result['fingerprints'][run_id]={'varve_raw':vraw,'varve_aggregate':vagg,'timescale_raw':praw,'timescale_aggregate':pagg,'expected_watermark_rows':expected_count}
        if mode=='after':
            before=json.loads(Path('/results/scoped-recovery-before.json').read_text())
            assert before['fingerprints']==result['fingerprints'], 'recovery fingerprint changed'
            assert before['postgres_start']!=result['postgres_start'], 'PostgreSQL did not restart'
            result['verified_unchanged']=True
        destination.write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result),flush=True)
    except Exception as error:
        print(json.dumps({'failure':benchmark.redactor(credentials)(error)}),flush=True)
        raise SystemExit(1)
    finally:
        await varve.close()
        await pg.close()
asyncio.run(run())
