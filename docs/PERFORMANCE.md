# Performance

Measured on 2026-10-20 with `scripts/load_test.py` (real partner calls: HTTP → Kafka → transformer → result, a
JSON order with 10 lines mapped to an invoice), everything on one laptop: MacBook (Apple M1 Pro, 10 cores) running
PostgreSQL 17, Kafka 3.9, the backend(s) and the load generator together. A dedicated server will do better.

## Results

| Instances | Callers | Messages/s | p50 | p95 | p99 | Errors |
|---|---|---|---|---|---|---|
| 1 | 10 | 348 | 28 ms | 41 ms | 47 ms | 0 |
| 1 | 50 | 352 | 138 ms | 231 ms | 264 ms | 0 |
| 1 | 100 | 344 | 284 ms | 404 ms | 444 ms | 0 |
| 1 | 200 | 340 | 579 ms | 733 ms | 781 ms | 0 |
| 2 | 50 | 554 | 74 ms | 161 ms | 210 ms | 0 |
| 2 | 100 | 549 | 174 ms | 326 ms | 356 ms | 0 |
| 2 | 200 | 549 | 354 ms | 574 ms | 656 ms | 0 |

**Soak test** - 1 instance, 50 callers, 5 minutes: 103,700 calls, 0 errors, steady 346 messages/s, memory flat
(136 MB → 130 MB), every transaction completed.

## What limits it

- One instance is one Python process: it tops out at about **350 messages/s** and beyond that callers queue
  (latency grows, throughput stays flat, nothing fails). Inside the server a transaction takes about 6 ms (median)
  from arrival to completion; the rest of the time is waiting for that one process.
- **More instances add capacity**: two gave +60% on this laptop, where they also share the CPU with Kafka,
  PostgreSQL and the load generator. On separate machines it scales further - put a load balancer in front and
  give the Kafka topics enough partitions (`KAFKA_TOPIC_PARTITIONS`, at least the number of instances).
- Callers wait at most 15 s for an answer; past that they get `202` with a link to poll - so overload shows up as
  latency and 202s, not lost messages.

## Run it yourself

Use a test environment - every call is a real transaction.

```bash
python scripts/load_test.py http://localhost:8000 <admin password> --concurrency 10,50,100,200 --seconds 20
python scripts/load_test.py http://host-a:8000 <pw> --urls http://host-b:8000 --concurrency 100   # two instances
python scripts/load_test.py http://localhost:8000 <pw> --concurrency 50 --seconds 600              # soak test
```

`server p50/p95` in the output is the time the server spent (the `X-Processing-Ms` header of every partner call);
if callers see much more than that, the load generator is the bottleneck - add `--processes`.

## Bugs the load tests found (fixed)

- A partner call's `QUEUED` status was written after publishing to Kafka, so with several instances it could
  overwrite `COMPLETED` and leave the caller waiting 15 s. Status is now written first, and a finished transaction
  can never be moved back to an in-progress status.
- PostgreSQL `NOTIFY` (first used to tell instances a transaction finished) serialises commits across the database;
  replaced by one small query per instance every 50 ms.
- The database pool (10 connections) made one instance collapse at 200 callers (p95 5 s); now 20 by default
  (`PAYLOAD_HUB_DB_POOL_MAX`).
- An unexpected error could stop the result consumer; it now logs the error and carries on.
