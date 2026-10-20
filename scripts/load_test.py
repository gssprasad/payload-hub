"""Load test: real partner calls (HTTP -> Kafka -> transformer -> result) at a fixed concurrency.

    python scripts/load_test.py http://localhost:8000 <admin password> --concurrency 50 --seconds 30
    python scripts/load_test.py URL PASSWORD --concurrency 10,50,100 --seconds 20      # several steps
    python scripts/load_test.py URL PASSWORD --concurrency 20 --seconds 600            # soak test
    python scripts/load_test.py URL PASSWORD --urls http://host-a:8000,http://host-b:8000  # spread over instances

It creates a throw-away partner, sends the same order to it as fast as the concurrency allows, and reports
throughput, latency percentiles and errors per step; then deletes the partner. Run it against a test environment -
every call is a real transaction in that environment's database.
"""
import argparse
import asyncio
import json
import statistics
import time
import uuid

import httpx

ORDER = {"order": {"id": "L-1", "currency": "EUR", "customer": {"firstName": "Asha", "lastName": "Rao",
                                                                "email": "ASHA@EXAMPLE.COM"},
                   "items": [{"sku": f"S{i}", "name": f"Item {i}", "qty": i + 1, "price": 9.99} for i in range(10)]}}
MAPPING = ("{invoice: order.id, customer: concat(order.customer.firstName, ' ', order.customer.lastName), "
           "email: lower(order.customer.email), lines: order.items[].{sku: sku, total: multiply(qty, price)}}")


def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


async def step(urls, code, key, concurrency, seconds):
    body = json.dumps(ORDER).encode()
    latencies, server, errors, statuses = [], [], [], {}
    deadline = time.perf_counter() + seconds
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency * 2)
    async with httpx.AsyncClient(timeout=60, limits=limits) as client:
        async def worker(n):
            i = n
            while time.perf_counter() < deadline:
                url = f"{urls[i % len(urls)]}/inbound/{code}"
                i += 1
                t0 = time.perf_counter()
                try:
                    r = await client.post(url, content=body, headers={"X-API-Key": key,
                                                                       "Content-Type": "application/json"})
                    statuses[r.status_code] = statuses.get(r.status_code, 0) + 1
                    if r.status_code == 200:
                        latencies.append((time.perf_counter() - t0) * 1000)
                        if "x-processing-ms" in r.headers:
                            server.append(float(r.headers["x-processing-ms"]))
                    else:
                        errors.append(f"HTTP {r.status_code}")
                except httpx.HTTPError as e:
                    errors.append(type(e).__name__)

        started = time.perf_counter()
        await asyncio.gather(*(worker(n) for n in range(concurrency)))
        elapsed = time.perf_counter() - started
    return {"latencies": latencies, "server": server, "errors": len(errors), "statuses": statuses, "elapsed": elapsed}


def _run_step(args):
    return asyncio.run(step(*args))


def measure(urls, code, key, concurrency, seconds, processes):
    """Several load-generator processes (one Python process can't drive a fast server on its own)."""
    from concurrent.futures import ProcessPoolExecutor

    shares = [concurrency // processes + (1 if i < concurrency % processes else 0) for i in range(processes)]
    with ProcessPoolExecutor(processes) as pool:
        parts = list(pool.map(_run_step, [(urls, code, key, n, seconds) for n in shares if n]))
    latencies = [x for p in parts for x in p["latencies"]]
    server = [x for p in parts for x in p["server"]]
    statuses = {}
    for p in parts:
        for k, v in p["statuses"].items():
            statuses[k] = statuses.get(k, 0) + v
    elapsed = max(p["elapsed"] for p in parts)
    return {"concurrency": concurrency, "seconds": round(elapsed, 1), "ok": len(latencies),
            "errors": sum(p["errors"] for p in parts), "per_second": round(len(latencies) / elapsed, 1),
            "p50": pct(latencies, 50), "p95": pct(latencies, 95), "p99": pct(latencies, 99),
            "max": max(latencies, default=float("nan")),
            "mean": statistics.fmean(latencies) if latencies else float("nan"), "statuses": statuses,
            "server_p50": pct(server, 50), "server_p95": pct(server, 95)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url")
    ap.add_argument("password")
    ap.add_argument("--concurrency", default="50", help="one number, or several separated by commas")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--urls", help="extra instances to spread the calls over (comma-separated)")
    ap.add_argument("--processes", type=int, default=4, help="load-generator processes (default 4)")
    args = ap.parse_args()
    admin = httpx.Client(base_url=args.url, timeout=60)
    admin.headers["Authorization"] = "Bearer " + admin.post("/api/auth/login", json={
        "username": "Administrator", "password": args.password}).raise_for_status().json()["token"]
    code = admin.post("/api/partners", json={"name": f"Load test {uuid.uuid4().hex[:6]}", "input_format": "json",
                                             "output_format": "json", "expression": MAPPING}).raise_for_status().json()["code"]
    key = admin.get(f"/api/partners/{code}/api-key").json()["api_key"]
    urls = [args.url] + ([u.strip() for u in args.urls.split(",") if u.strip()] if args.urls else [])
    try:
        measure(urls, code, key, 5, 3, 1)  # warm-up (connections, Kafka consumers)
        print(f"{'conc':>5} {'ok':>7} {'err':>5} {'msg/s':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'max ms':>8}"
              f"  {'server p50':>10} {'server p95':>10}")
        for c in [int(x) for x in args.concurrency.split(",")]:
            r = measure(urls, code, key, c, args.seconds, min(args.processes, c))
            print(f"{r['concurrency']:>5} {r['ok']:>7} {r['errors']:>5} {r['per_second']:>8} {r['p50']:>8.1f} "
                  f"{r['p95']:>8.1f} {r['p99']:>8.1f} {r['max']:>8.1f}  {r['server_p50']:>10.1f} {r['server_p95']:>10.1f}"
                  + (f"   statuses {r['statuses']}" if r["errors"] else ""))
    finally:
        admin.delete(f"/api/partners/{code}")


if __name__ == "__main__":
    main()
