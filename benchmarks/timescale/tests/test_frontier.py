import argparse
import unittest
from frontier import conservation, metrics_delta, parser, summary, validate, validate_fresh_read


class FrontierTests(unittest.TestCase):
    def test_nearest_rank_and_sample_gate(self):
        self.assertEqual(summary([]), {"samples": 0})
        result = summary(list(range(1, 101)))
        self.assertEqual((result["p50_ms"], result["p95_ms"], result["p99_ms"]), (50, 95, 99))
        self.assertFalse(result["p99_minimum_sample_gate"])
        self.assertTrue(summary(list(range(1000)))["p99_minimum_sample_gate"])

    def test_no_hidden_drops_failures_or_backlog(self):
        self.assertTrue(conservation(100, 100, 0, 0, 0))
        for accounting in [(100, 90, 10, 0, 0), (100, 90, 0, 10, 0), (100, 90, 0, 0, 10)]:
            self.assertFalse(conservation(*accounting))
        with self.assertRaises(ValueError):
            conservation(100, 90, 0, 0, 0)
        with self.assertRaises(ValueError):
            conservation(100, 101, -1, 0, 0)

    def test_phase_deltas_do_not_subtract_gauges_or_mix_nested_timers(self):
        before = 'varve_phase_duration_seconds_sum{phase="wal_write"} 2\nvarve_phase_duration_seconds_sum{phase="wal_sync"} 1\nvarve_hot_rows 20\n'
        after = 'varve_phase_duration_seconds_sum{phase="wal_write"} 5\nvarve_phase_duration_seconds_sum{phase="wal_sync"} 3\nvarve_hot_rows 0\n'
        self.assertEqual(list(metrics_delta(before, after).values()), [3, 2])
        with self.assertRaises(ValueError):
            metrics_delta(after, before)

    def test_fresh_snapshot_requires_acknowledged_floor_and_raw_rollup_agreement(self):
        row = dict(n=1200, total=12.25, lo=-4.5, hi=7.5,
                   rollup_n=1200, rollup_total=12.25, rollup_lo=-4.5, rollup_hi=7.5)
        self.assertEqual(validate_fresh_read([row], 1100, 1300), 1200)
        for lower, upper in ((1201, 1300), (1100, 1199)):
            with self.assertRaises(RuntimeError):
                validate_fresh_read([row], lower, upper)
        for key, value in (("rollup_total", 12.5), ("n", True), ("total", float("nan"))):
            changed = {**row, key: value}
            with self.assertRaises(RuntimeError):
                validate_fresh_read([changed], 1100, 1300)
        for malformed in ([], [row, row], [None]):
            with self.assertRaises(RuntimeError):
                validate_fresh_read(malformed, 1100, 1300)

    def test_bounds_reject_nonfinite_and_oversized_profiles(self):
        args = parser().parse_args(['--binary','x','--profile','x','--output','x','--expected-binary-sha256','x','--rows','102400'])
        validate(args)
        for key, value in [('rate',float('nan')), ('seconds',61), ('trace_capacity',513), ('rows',100001), ('read_interval',.001), ('readers',3), ('drain_seconds',0), ('drain_seconds',float('inf'))]:
            clone = argparse.Namespace(**vars(args)); setattr(clone, key, value)
            with self.assertRaises(ValueError):
                validate(clone)


class ScheduledLoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_each_receipt_is_timed_and_full_offered_interval_is_charged(self):
        import io
        from frontier import scheduled_load
        class Client:
            async def request(self, method, path, body):
                return {'durability': 'local_fsync', 'duplicate': False, 'rows': len(body['rows']), 'sequence': 1}
        args = argparse.Namespace(rows=1024, rate=1000, seconds=.02, batch=10, writers=2, read_interval=0)
        events = io.StringIO()
        load, offsets = await scheduled_load(Client(), args, 0, events)
        self.assertEqual((load['offered'], load['acknowledged'], load['dropped']), (20, 20, 0))
        self.assertTrue(load['clean'])
        self.assertGreaterEqual(load['elapsed_with_drain_s'], .02)
        self.assertEqual(load['arrival_latency']['samples'], 2)
        self.assertEqual(len(offsets), 2)

    async def test_fresh_reader_observes_changing_inputs_without_hiding_write_failures(self):
        import asyncio
        import io
        import json
        from frontier import FRESH_SQL, scheduled_load
        class Client:
            rows = 1024
            async def request(self, method, path, body):
                await asyncio.sleep(.001)
                self.rows += len(body['rows'])
                return {'durability': 'local_fsync', 'duplicate': False, 'rows': len(body['rows']), 'sequence': 1}
            async def sql(self, sql):
                assert sql == FRESH_SQL
                return [dict(n=self.rows, total=0, lo=-1, hi=1, rollup_n=self.rows, rollup_total=0, rollup_lo=-1, rollup_hi=1)]
        args = argparse.Namespace(rows=1024, rate=1000, seconds=.04, batch=10, writers=2,
                                  read_interval=.01, read_mode='fresh')
        events = io.StringIO()
        load, _ = await scheduled_load(Client(), args, 0, events)
        self.assertTrue(load['clean'])
        reads = [json.loads(line) for line in events.getvalue().splitlines() if json.loads(line)['kind'] == 'read']
        self.assertTrue(reads)
        self.assertTrue(all(r['mode'] == 'fresh' and r['rows'] >= r['acknowledged_floor_rows'] for r in reads))
        self.assertGreater(reads[-1]['rows'], 1024)

    async def test_failed_read_retains_its_nested_diagnostic_and_stops_load(self):
        import asyncio
        import io
        from frontier import redactor, scheduled_load
        class Client:
            describe_error = staticmethod(redactor({'token': 'fixture-secret'}))
            async def request(self, method, path, body):
                return {'durability': 'local_fsync', 'duplicate': False, 'rows': len(body['rows']), 'sequence': 1}
            async def sql(self, sql):
                raise RuntimeError('fixture fresh read failed fixture-secret')
        args = argparse.Namespace(rows=1024, rate=1000, seconds=.1, batch=10, writers=2,
                                  read_interval=.01, read_mode='fresh')
        events, progress = io.StringIO(), {}
        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(scheduled_load(Client(), args, 0, events, progress=progress), 1)
        self.assertIn('fixture fresh read failed <redacted>', progress['failed_read']['failed'])
        self.assertNotIn('fixture-secret', events.getvalue())
        counts = progress['partial_load']
        self.assertEqual(counts['offered'], counts['acknowledged'] + counts['failed'] + counts['pending'])
        self.assertGreater(progress['scheduling']['write_unsubmitted_rows'], 0)

    async def test_slow_backend_discloses_overload_instead_of_hiding_arrivals(self):
        import asyncio
        import io
        from frontier import scheduled_load
        class Client:
            async def request(self, method, path, body):
                await asyncio.sleep(.02)
                return {'durability': 'local_fsync', 'duplicate': False, 'rows': len(body['rows']), 'sequence': 1}
        args = argparse.Namespace(rows=1024, rate=100000, seconds=.02, batch=10, writers=1, read_interval=0)
        load, offsets = await scheduled_load(Client(), args, 0, io.StringIO())
        self.assertFalse(load['clean'])
        self.assertEqual(load['dropped'], 0)
        self.assertEqual(load['offered'], load['acknowledged'])
        self.assertTrue(load['data_complete'])
        self.assertFalse(load['schedule_met'])
        self.assertLessEqual(load['scheduling']['peak_write_queue_requests'], 2)
        self.assertEqual(len(offsets), 200)
        self.assertEqual(load['pending'], 0)

    async def test_failed_or_ambiguous_write_is_not_retried_and_does_not_deadlock(self):
        import asyncio
        import io
        from frontier import scheduled_load
        attempts = []
        class Client:
            async def request(self, method, path, body):
                attempts.append(body['request_id'])
                raise RuntimeError('ambiguous fixture')
        args = argparse.Namespace(rows=1024, rate=100000, seconds=.1, batch=10, writers=1, read_interval=0)
        events = io.StringIO()
        progress = {}
        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(scheduled_load(Client(), args, 0, events, progress=progress), timeout=1)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(progress['partial_load']['failed'], 10)
        self.assertIn('ambiguous fixture', events.getvalue())


class LosslessScheduleTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_reads_keep_every_independent_arrival_and_bounded_queue(self):
        import asyncio
        import io
        import json
        from frontier import scheduled_load
        class Client:
            async def request(self, method, path, body):
                return dict(durability='local_fsync', duplicate=False, rows=len(body['rows']))
            async def sql(self, sql):
                await asyncio.sleep(.025)
                return [dict(n=1024, total=0)]
        args = argparse.Namespace(rows=1024, rate=1000, seconds=.08, batch=10,
                                  writers=1, readers=1, read_interval=.01, drain_seconds=1)
        events = io.StringIO()
        load, _ = await scheduled_load(Client(), args, 0, events, prefix_sum=0)
        self.assertEqual(load['reads'], dict(offered=7, completed=7, failed=0, pending=0))
        self.assertTrue(load['data_complete'])
        self.assertFalse(load['clean'])
        self.assertLessEqual(load['scheduling']['peak_read_queue_requests'], 2)
        self.assertEqual(load['scheduling']['read_unsubmitted'], 0)
        observations = [json.loads(x) for x in events.getvalue().splitlines() if json.loads(x)['kind'] == 'read']
        for i, observation in enumerate(observations, 1):
            self.assertAlmostEqual(observation['intended_s'], i * .01, places=6)
        self.assertGreater(load['read_arrival_latency']['max_ms'], load['read_latency']['max_ms'])

    async def test_drain_deadline_accounts_for_never_enqueued_and_ambiguous_work(self):
        import asyncio
        import io
        from frontier import scheduled_load
        attempts = []
        class Client:
            async def request(self, method, path, body):
                attempts.append(body['request_id'])
                await asyncio.Event().wait()
        args = argparse.Namespace(rows=1024, rate=10000, seconds=.02, batch=10,
                                  writers=1, read_interval=0, drain_seconds=.03)
        progress = {}
        with self.assertRaises(TimeoutError):
            await scheduled_load(Client(), args, 0, io.StringIO(), progress=progress)
        counts, flow = progress['partial_load'], progress['scheduling']
        self.assertEqual(counts, dict(offered=200, acknowledged=0, dropped=0, failed=10, pending=190))
        self.assertEqual(flow['write_enqueued_rows'], 30)
        self.assertEqual(flow['write_unsubmitted_rows'], 170)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(flow['peak_write_queue_requests'], 2)

    async def test_external_cancellation_conserves_independent_reads_and_writes(self):
        import asyncio
        import io
        from frontier import scheduled_load
        started = asyncio.Event()
        class Client:
            async def request(self, method, path, body):
                return dict(durability='local_fsync', duplicate=False, rows=len(body['rows']))
            async def sql(self, sql):
                started.set()
                await asyncio.Event().wait()
        args = argparse.Namespace(rows=1024, rate=1000, seconds=.5, batch=10,
                                  writers=1, read_interval=.01)
        progress = {}
        task = asyncio.create_task(scheduled_load(Client(), args, 0, io.StringIO(), prefix_sum=0, progress=progress))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        counts, reads = progress['partial_load'], progress['partial_reads']
        self.assertEqual(counts['offered'], counts['acknowledged'] + counts['failed'] + counts['pending'])
        self.assertEqual(reads['offered'], reads['completed'] + reads['failed'] + reads['pending'])
        self.assertEqual(reads['failed'], 1)
        self.assertGreater(progress['scheduling']['read_unsubmitted'], 0)


if __name__ == '__main__':
    unittest.main()
