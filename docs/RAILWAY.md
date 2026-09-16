# Railway evaluation deployment

This is an isolated evaluation, not a production-certification claim. Do not place irreplaceable data here.

## Scope

- Workspace: **Tom** (`18857f84-0af6-4379-9217-e62e2d14b48f`).
- Project: **varve-evaluation** (`8caffa15-0158-4822-a6c2-cb405bddc62d`).
- Environment: `production` (`5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1`), Railway's default name inside this evaluation project—not an existing production application.
- Service: **varve** (`64ddd834-6732-4582-bf4a-d86bbd0317fa`).
- Persistent volume: `a4bbfeed-de4b-4e97-ab15-94f39ba8b557`, mounted at `/data`.
- Dedicated bucket: **varve-objects** (`21c711b0-518d-4865-bec6-7a4890c68e06`), Singapore.
- Placement: `asia-southeast1-eqsg3a`, one replica; hard service limits read back as one vCPU and 1,000,000,000 memory bytes.
- Application disk admission: 512 MiB; Railway's volume capacity is 50 GB. Application admission is not a filesystem quota. Initial volume metadata reported approximately 1.06 GB used.

The workspace had no spending limit configured; none was changed. Compute limits and bounded tests constrain the evaluation but do not impose a monetary cap. A retained running service/bucket/volume can continue incurring charges; inspect Railway usage and explicitly stop/delete evaluation resources when no longer wanted.

## Deployment contract

`Dockerfile` builds release Rust remotely without bundling DuckDB source, installs the checksum-pinned DuckDB CLI, and runs the application as UID/GID 10001. The entrypoint refuses ephemeral storage unless deliberately overridden, requires an API token for its public bind, and forwards signals through `exec`. It creates only `/data/varve` and `/data/probes` and does not recursively change unrelated volume ownership.

Runtime credentials are Railway variables, not source files, Docker arguments or command-line token arguments. `VARVE_API_TOKEN` is a randomly generated 64-character token. All operator APIs require it; readiness/liveness disclose only minimal state. TLS terminates at Railway's HTTPS ingress. Do not bypass this with public plaintext clients.

The selected S3 credential response uses **virtual-host addressing** and region `auto`. With object_store 0.14.2, configure `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true` and put the bucket name into `VARVE_S3_ENDPOINT` (the bucket credential's returned base endpoint does not include it). `VARVE_S3_PREFIX=evaluation/main` isolates the example's database. Never copy credentials into documentation or logs.

## Bounded workload

`scripts/stress.py` uses standard Python libraries, a token from the environment, synthetic unique table/aggregate/job names, exact count/sum oracles, out-of-order events, idempotent retries, concurrent SQL, scheduling controls and optional independent raw expiration. Defaults offer at most 100,000 rows at 4,000 rows/s with two writers for up to 60 seconds. Network attempts and response bytes are separately bounded. `--seconds` limits new offered work; use an outer timeout (for example 180 seconds) to also cap drain/control phases.

```sh
# VARVE_API_TOKEN must be set securely in the current environment.
python3 scripts/stress.py --url https://YOUR-EVALUATION-DOMAIN \
  --rows 100000 --batch 256 --concurrency 2 --seconds 60 --rate 4000 --expire
```

Use `--expire` only on this tool's newly created synthetic table; the script refuses to continue if unique table creation fails. Paused jobs are manually exercised, then removed. Raw expiration leaves independently retained rollups and idempotency metadata intentionally intact.

The image also includes `varve-cloud-probe`, a Rust protocol/restore drill. `VARVE_CLOUD_PROBE=true varve-cloud-probe --rows=4000` creates random child prefixes under the configured base, tests immutable collisions/bounded reads/CAS/delete/list, verifies real ingestion/checkpoint/archive/SQL/named rollups, stops its writer, restores to a new temporary directory, expires raw rows while preserving derived history, and vacuums unreachable objects. It does **not** transfer ownership of the running example's main namespace. Small remote checkpoint/control objects remain as evidence; local temporary data is removed.

## State

The final release artifact passed a 4,000-row real-bucket protocol/archive/restore drill, a 100,000-row HTTPS workload, and a separate 20,000-row raw-expiration/retained-aggregate check. The token-protected endpoint is **https://varve-production.up.railway.app**. Current deployment `dbd59af2-5c4c-4e8e-a452-72914ca69528` was observed `SUCCESS` and checked over HTTPS/SSH; it recreates verified build `e57b9f08-4852-4db8-9b0e-da6ef49e7452` with identical executable hashes and the same volume.

A direct CLI restart timed out and left an exited container despite its stale `SUCCESS` status. Existing-artifact redeployment recovered service and exact 100,000-row raw/aggregate results; the cause of the restart incident remains unresolved. Do not treat deployment status alone as liveness or this recovery as automatic failover. See [EVALUATION.md](EVALUATION.md) for measured latency/resources, artifact fingerprints, retained-data policies and remaining release gates.

`/ready` may return a conservative 503 during a busy state-lock interval; use bounded readiness polling and inspect persistent failures. For an exited evaluation container, inspect logs/instance state before recreating the existing artifact with `railway redeploy --yes` and explicit project/environment/service scope. Never delete the volume or recovery files to force startup.
