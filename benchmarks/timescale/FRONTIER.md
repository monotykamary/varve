# Local service-level frontier probe

`frontier.py` launches one fresh owned Varve HTTP process, preloads deterministic quarter-valued data, then schedules a bounded offered write rate with explicitly labeled `prefix` or `fresh` concurrent reads. It reuses `core.py`'s generator and checks independent raw and continuous-rollup count/sum/min/max after drain. It does not start Timescale, claim CPU/storage parity, publish packages, contact Railway or authorize cloud operations.

## Run

Use an isolated virtualenv with `requirements.txt`. Supply an explicitly reviewed release binary and its SHA256, a Config JSON with the real DuckDB path, and a never-existing output directory. The profile should match the intended deployment except documented local path substitutions. Never use a retained database root: the probe creates its own `output/data`.

```sh
python frontier.py --binary /absolute/varve --expected-binary-sha256 SHA256 \
  --profile /absolute/config.json --output /absolute/fresh-run \
  --rows 102400 --batch 1000 --writers 4 --rate 5000 --seconds 20 \
  --read-interval .1 --read-mode fresh --max-seconds 180
```

Run a separate diagnosis with `--trace-capacity 512` on a trace-capable server. Do not compare tracing-on candidate timings against tracing-off baseline as a performance win. A baseline server without the diagnostic route is supported only with trace capacity zero.

## Measurement contract

- Initial rows must be divisible by 1024. Default `--read-mode prefix` queries only immutable preload timestamps and isolates resident pruning; it does **not** exercise newly written aggregate inputs. `--read-mode fresh` queries changing raw and rollup count/sum/min/max together in one SQL snapshot, requires their agreement, and bounds the count by previously acknowledged rows and rows offered by completion. Concurrent counts need not equal client receipt counts because writes can commit while the query is in flight. These are snapshot-consistency/bounds checks, not an independent per-read value oracle; the independent exact all-acknowledged quarter oracle still runs after drain. Every read event labels its mode and acknowledgment bounds. Timing starts after independent oracle setup, not during generation of its expected result.
- Writes are scheduled from an independent intended-arrival clock. Queuing cannot slow that clock to hide overload. Each worker timestamps its own completion. Row generation is charged; arrival latency additionally includes driver scheduling/queue delay. Ack latency includes aiohttp JSON encoding and transport; it is not pure server time.
- The write queue holds at most twice the writer count. Full queues now propagate lossless backpressure: one cursor holds the rest of the finite trace and payloads are generated only by workers. `offered` is the complete predeclared intended trace, including future/unsubmitted work after early cancellation, not merely work successfully enqueued. `scheduling.write_unsubmitted_rows` discloses the difference. No write is silently dropped or automatically retried. Failed/ambiguous requests, queued pending work and never-enqueued work reconcile against the full trace on every exit.
- `data_complete` means every intended write and read completed without errors; `clean` additionally requires `schedule_met`. Starting any operation more than one full arrival interval late fails this conservative scheduling diagnostic, even if drain eventually succeeds. The gate is explicit and identical for baseline/candidate, not a universal service SLO or a statistical proof of sustainable capacity. An overloaded report is not a pass.
- Since the lossless campaign, reads also have an independent finite arrival trace, rather than sleeping after the preceding query completes. `--readers` is 1 (default) or 2. Read events retain intended arrival, queue wait and service time; `read_arrival_latency` includes all waiting. Slow reads accumulate visible logical debt behind a bounded metadata queue, not fewer silently omitted query arrivals. `--drain-seconds` defaults to 30 and caps the extra drain interval; timeout preserves explicit pending/unsubmitted work. Historical evidence used the earlier closed-loop reader and is not retroactively comparable.
- Throughput includes the entire offered interval and final drain. At an under-capacity fixed offered rate, equal throughput is expected; compare latency there, and sweep rates to find the sustainable boundary. This is not a closed-loop saturation number.
- Raw request/read observations, before/after metrics, phase deltas, input hashes, profile, UUID, process cleanup and a file-hash inventory are retained. Nested phase totals are never summed into exclusive CPU time. Counter regressions fail rather than silently subtracting across a restart.
- Trace history can be evicted. Join successful fresh receipt sequences to recorded groups; disclose unmatched sequences and never infer zero checkpoint time from absent records.
- Sample percentiles are nearest-rank observations. `p99_minimum_sample_gate` requires 1000 observations but is only a minimum, not a confidence interval or production SLO. Short pilots are diagnostics, not p99 qualification.
- One million total rows, 20,000 requests, 60 seconds offered load and 600 seconds whole-run deadline are hard maxima. Binary/profile bytes are checked before and after. The process group is owned and killed/reaped at exit, including on catchable errors. Hard-killing the probe or host loss remains outside its cleanup guarantee. Shutdown here is not a recovery test.

## Progression

1. Qualify accounting/cancellation and exact HTTP oracles before measuring.
2. Capture traced diagnoses across pressure checkpoints, tier changes and warm/miss reads.
3. Run untraced baseline/candidate in counterbalanced order, no concurrent builds. Keep data/profile/rate/concurrency identical and retain failed attempts.
4. Collect enough independent runs and samples, report uncertainty and backlog, then perform fresh Linux/Railway qualification and a separately approved matched Timescale comparison. A local pass is never relabeled a Timescale win.
