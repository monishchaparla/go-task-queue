# Plan: week-by-week checklist

Status: Planned — implementation not started yet. Every box below is unchecked on purpose. Tick a box only when its done-criterion is true and demonstrable. Milestone numbers (M0–M8) refer to [README.md](README.md#milestones-and-done-criteria).

Eight weeks, part-time. Weeks are a budget, not a promise; if a week slips, cut from week 8 (the stretch) first, never from chaos tests or benchmarks.

## Week 1 — Foundations and core store (M0, start of M1)

- [ ] Go module, repo layout, linter config, GitHub Actions running `go vet` and `go test`
- [ ] `docker-compose.yml` with Postgres; one command brings it up
- [ ] Schema + migration: `jobs` table (id, queue, payload, state, run_at, attempts, max_attempts, lease_token, lease_expires_at, idempotency_key, last_error, timestamps)
- [ ] Indexes: partial index for ready jobs by `(queue, run_at)`, unique `(queue, idempotency_key)`
- [ ] `Store` interface: `Enqueue`, `Lease`, `Ack`, `Nack`, `Heartbeat`, `Reap`, `Stats`
- [ ] Postgres `Enqueue` and `Lease` using `FOR UPDATE SKIP LOCKED`
- [ ] Integration test (real Postgres): two concurrent consumers never get the same live lease
- [ ] Design note in `docs/`: lease/ack state machine and the five safety invariants

Done when: CI is green and the competing-consumer test passes.

## Week 2 — Network API and worker SDK (M1 finish, M2)

- [ ] `Ack` and `Nack` with lease-token check; stale token returns a distinct error
- [ ] Protobuf definitions and generated gRPC server: Enqueue, Lease, Ack, Nack, Heartbeat
- [ ] Minimal HTTP/JSON endpoints for enqueue, stats, health
- [ ] Worker SDK: register handler, concurrency limit, poll loop with backoff when idle, graceful shutdown
- [ ] Demo producer and demo worker
- [ ] Compose file runs server + Postgres + 2 workers end-to-end

Done when: enqueue 1,000 no-op jobs through the real API and every one ends `done` (count verified in a test, not by eye).

## Week 3 — Reliability core (M3, part 1)

- [ ] Visibility timeout enforced by the DB clock
- [ ] Reaper loop: expired leases go back to `ready` (or `dead` when attempts exhausted)
- [ ] Heartbeat RPC and automatic heartbeat in the SDK
- [ ] Exponential backoff with full jitter; per-queue `base`, `cap`, `max_attempts`
- [ ] Tests: handler outlives timeout without heartbeat → re-leased, late Ack rejected
- [ ] Tests: always-failing handler → retried with growing delays → `dead`

Done when: each worker-failure row in the README failure table has a passing test or is explicitly marked "chaos suite only".

## Week 4 — DLQ, idempotency, scheduling (M3 finish, M4)

- [ ] Dead-letter listing and replay (API + simple CLI)
- [ ] Idempotency keys: duplicate enqueue returns the existing job; concurrency test with many producers using one key
- [ ] Scheduled/delayed jobs via `run_at`; test that nothing runs early
- [ ] Retention job: delete finished jobs and expired idempotency keys after a configurable window
- [ ] Graceful server shutdown: drain in-flight RPCs
- [ ] Reaper safe on multiple replicas (test with N replicas)

Done when: M3 and M4 done-criteria in the README are all demonstrably true.

## Week 5 — Observability (M5)

- [ ] Prometheus metrics: counters, gauges, histograms listed in the README
- [ ] Prometheus container in Compose, scraping the server
- [ ] Structured logs with job id, queue, attempt, lease token prefix
- [ ] Built-in dashboard page: depth by state, oldest-job age, throughput, retries, DLQ with last error
- [ ] Check metric values against ground truth from the DB after a test run

Done when: a demo run shows the dashboard and Prometheus agreeing with database counts.

## Week 6 — Chaos tests (M6)

- [ ] Chaos driver: enqueue a known set of jobs with unique ids, record them durably
- [ ] Fault: `kill -9` workers mid-job at random times
- [ ] Fault: stop and restart the queue server under load
- [ ] Fault: restart Postgres under load
- [ ] Assertions: all five safety invariants (no lost job, one terminal state, no `done` without a valid Ack, `max_attempts` respected, duplicates counted)
- [ ] Randomized seeds, seed printed on failure so a failure can be replayed
- [ ] Run many seeds; record the number of runs and any violations found
- [ ] Fix every violation found, add a regression test for each

Done when: the suite passes repeatedly across many seeds, and the write-up lists the real bugs it caught (or says none were caught).

## Week 7 — Benchmarks (M7)

- [ ] Load generator `cmd/bench`: open-loop and closed-loop modes, HDR histogram latency
- [ ] Scenarios from the README: enqueue-only, end-to-end no-op, backlog drain, recovery after worker kill
- [ ] Parameter sweeps one variable at a time (workers, batch size, visibility timeout, payload size, `synchronous_commit`, polling vs `LISTEN/NOTIFY`)
- [ ] Warm-up, repeated runs, median and spread
- [ ] Capture hardware, OS, Go and Postgres versions, configs
- [ ] `docs/benchmarks.md` with raw output and scripts; one command reproduces
- [ ] Profile (pprof and `EXPLAIN ANALYZE`) the slowest path and fix the biggest bottleneck, if any

Done when: results are published with their configuration and methodology, whatever the numbers are.

## Week 8 — Stretch, write-up, polish (M8)

- [ ] Spike: append-only-log `Store` behind the same interface (segments, checksums, fsync policy, recovery on start)
- [ ] Run the same integration and chaos suites against it; run the same benchmarks
- [ ] Write-up: Postgres vs log store with measurements, including what did not work
- [ ] Decision record: why Postgres stays the default (or why not)
- [ ] README: replace the status line and any "planned" wording with what actually exists
- [ ] Quickstart in the README that works from a clean clone
- [ ] Tag `v0.1.0`

Done when: a stranger can clone, run `docker compose up`, enqueue a job, watch it on the dashboard, and run the chaos suite.

## Risks to watch

- Postgres bloat from high update churn on the jobs table → measure early in week 7; consider time partitioning if it hurts.
- Chaos tests that are flaky for the wrong reason → fix the test harness before trusting any pass.
- Scope creep (priorities, cron, workflows) → out of scope for v1.
- Stretch log store eating the schedule → it is cut first.
