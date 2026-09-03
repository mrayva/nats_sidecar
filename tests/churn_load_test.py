#!/usr/bin/env python3
"""High-churn-under-load characterization tool.

Every performance investigation on this project so far (matching cost, dedup arrays, the
backpressure-adaptive count-then-emit dispatch - see README) assumed a STATIC subscription set:
K fixed, no subscribes/unsubscribes happening while the data plane is under load. That's a real,
unexamined gap: subscription_manager::rebuild_tree_locked() (subscription_manager.cpp) is
O(current subscription count) and runs on every fully-removed subscription WHILE HOLDING m_mutex's
EXCLUSIVE lock - the same lock every worker thread's acquire_tree() needs (as a shared lock)
before it can match a single row. This tool exists to characterize whether that shows up as a real
worker-thread match-latency spike when subscription churn overlaps with sustained matching load -
not to fix anything (see the README's own "high-churn-under-load test plan" - the fix, if this
finds one is needed, is deliberately a separate, later step).

Built on top of integration_sidecar.py's own NatsClient/process-management functions (imported,
not duplicated) - that script already proves the process-lifecycle plumbing (temp dirs, log-file
waiting, clean shutdown) works against a real nats-server + a real sidecar binary; this tool adds
the load-generation and metrics-collection layer on top.

Two concurrent load generators, each its own NatsClient (its own TCP connection, its own Python
thread - simplest concurrency model that matches this project's existing synchronous-socket style,
no asyncio needed):
  1. Data-plane load: a raw msgpack publisher pushing rows at a configurable rate.
  2. Churn driver: a subscribe/unsubscribe cycle loop holding a target steady-state K, in "warm"
     (small fixed expression pool, no registry KV round trips after the first use of each) or
     "cold" (every cycle a brand-new expression, exercises subscription_registry's real KV round
     trip every time) mode.

Hypotheses this is actually trying to falsify (stated up front, not assumed true):
  1. Worker-thread match latency (P99+, not just mean - a lock-hold stall is a tail event) degrades
     measurably when churn is concurrent with load.
  2. That degradation scales with K (larger rebuild -> longer stall), not just churn rate alone.
  3. Cold churn adds meaningfully more latency/contention than warm churn at the same nominal rate.

Usage:
  python3 tests/churn_load_test.py \\
    --nats-server /path/to/nats-server --sidecar build/bin/nats_sidecar \\
    --config config/example.yaml \\
    --duration 30 --target-k 200 --churn-rate 20 --mode warm \\
    --data-rate 1000 --selectivity 10
"""

import argparse
import random
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import msgpack

sys.path.insert(0, str(Path(__file__).parent))
from integration_sidecar import NatsClient, free_port, wait_for_log, stop_process  # noqa: E402

TEMP_MAX = 100.0  # published "temperature" values are uniform in [0, TEMP_MAX)


def selectivity_threshold(s: float, rng: random.Random) -> float:
    """Same jittered-threshold model sidecar_pipeline_bench.cpp's own price-threshold generator
    uses (README's K-scaling investigation) - a +-5% jitter around the target selectivity so
    thresholds aren't all textually identical (important for "cold" mode's uniqueness need) while
    still averaging out to the requested selectivity across many subscriptions."""
    lo = 1.0 / s * 0.95
    hi = 1.0 / s * 1.05
    return TEMP_MAX * (1.0 - rng.uniform(lo, hi))


def make_expression(s: float, rng: random.Random) -> str:
    threshold = selectivity_threshold(s, rng)
    return f"temperature > {threshold:.8f}"


class Percentiles:
    """Records (op -> [latency_seconds]) samples from one thread, read back after stop() - a
    plain list under a lock is enough at this tool's scale (thousands of samples, not millions)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: dict[str, list[float]] = {}

    def record(self, op: str, latency: float) -> None:
        with self._lock:
            self._samples.setdefault(op, []).append(latency)

    def report(self) -> dict[str, dict[str, float]]:
        with self._lock:
            out = {}
            for op, samples in self._samples.items():
                if not samples:
                    continue
                s = sorted(samples)
                out[op] = {
                    "count": len(s),
                    "mean_ms": statistics.mean(s) * 1000,
                    "p50_ms": s[len(s) // 2] * 1000,
                    "p99_ms": s[min(len(s) - 1, int(len(s) * 0.99))] * 1000,
                    "p999_ms": s[min(len(s) - 1, int(len(s) * 0.999))] * 1000,
                    "max_ms": s[-1] * 1000,
                }
            return out


def data_plane_worker(
    nats_host: str, nats_port: int, subject: str, rate: float, stop_event: threading.Event
) -> None:
    """Publishes msgpack-encoded {"temperature": v} rows at (approximately - see the module's own
    docstring: no claim of precise real-time scheduling here, this is a load generator, not a
    timing benchmark) `rate` messages/sec until stop_event is set. Its own dedicated NatsClient/
    TCP connection - never shared with the churn thread's."""
    client = NatsClient(nats_host, nats_port)
    rng = random.Random()
    interval = 1.0 / rate if rate > 0 else 0
    try:
        while not stop_event.is_set():
            t0 = time.monotonic()
            payload = msgpack.packb({"temperature": rng.uniform(0, TEMP_MAX)}, use_bin_type=True)
            client.publish(subject, payload)
            if interval > 0:
                remaining = interval - (time.monotonic() - t0)
                if remaining > 0:
                    time.sleep(remaining)
    finally:
        client.close()


def churn_worker(
    nats_host: str,
    nats_port: int,
    mode: str,
    target_k: int,
    rate: float,
    selectivity: float,
    ramp_seconds: float,
    stop_event: threading.Event,
    percentiles: Percentiles,
    live_out: list,
) -> None:
    """Subscribe/unsubscribe cycle loop holding a steady-state K (DISTINCT active subscription
    count - what subscription_manager::active_count()/list_subscriptions actually count, not
    lease-pair count) around `target_k`, recording per-op latency into `percentiles` (skipping the
    first `ramp_seconds` - the initial ramp to target_k is a one-time cost, not steady-state
    churn, and shouldn't pollute the percentiles this tool reports). `live_out` is filled with the
    final (id, client_id, expr) list on exit, so the caller can cross-check against
    sidecar.list_subscriptions afterward. Its own dedicated NatsClient, separate from the
    data-plane worker's.

    NOTE on an earlier version of this function (found while first using this tool - worth
    keeping as a documented lesson, not just silently fixed): the add/remove balance decision
    used to compare len(live) (lease-PAIR count) against target_k, conflated with warm mode's own
    pool_size = target_k // 4. Since warm-mode leases accumulate toward target_k far faster than
    distinct subscriptions do (most adds after the initial pool population are REUSES, not new
    ids), len(live) could take far longer to reach target_k than the test's own duration allowed
    at any reasonable churn_rate - meaning warm mode could run for its entire duration in pure
    "add" bias, never actually reaching steady-state churn (never triggering a single remove).
    Fixed by keying the balance decision - and the warm-mode pool size - on DISTINCT id count
    directly, consistently across both modes."""
    client = NatsClient(nats_host, nats_port)
    rng = random.Random()
    interval = 1.0 / rate if rate > 0 else 0
    start = time.monotonic()
    next_client_id = 0
    live: list[tuple[int, str, str]] = []  # (sub_id, client_id, expression)

    # warm mode: a fixed pool of exactly target_k expressions, reused across many churn
    # cycles/client_ids - after each expression's first-ever subscribe, subscription_manager
    # already knows it locally (find_by_expression hits), so subsequent cycles never touch
    # subscription_registry's KV round trip UNLESS a pool entry gets fully removed (all its lease
    # holders gone) and later re-chosen - a real, expected occurrence, not a bug: in steady state
    # most pool entries have multiple simultaneous lease holders, so full removal is comparatively
    # rare, but not impossible. That's an intentional, honest part of what "warm" churn measures.
    pool = [make_expression(selectivity, rng) for _ in range(target_k)] if mode == "warm" else None
    # Round-robin through the pool while covering it for the first time, THEN fall back to random
    # reuse. Plain rng.choice(pool) from the start is a coupon-collector process - covering all
    # target_k entries at least once takes an EXPECTED target_k*ln(target_k) draws (~6900 for
    # target_k=1000), not target_k - found by actually running this tool: a 60s/50-cycles-per-sec
    # run never got anywhere near full pool coverage under pure random sampling. Round-robin
    # guarantees full coverage in exactly target_k add-cycles instead.
    pool_cursor = 0

    try:
        while not stop_event.is_set():
            t0 = time.monotonic()
            in_ramp = (time.monotonic() - start) < ramp_seconds

            distinct_k = len({sub_id for sub_id, _client_id, _expr in live})
            do_add = distinct_k < target_k or (distinct_k == target_k and rng.random() < 0.5) or not live
            if do_add:
                if mode == "warm":
                    if pool_cursor < len(pool):
                        expr = pool[pool_cursor]
                        pool_cursor += 1
                    else:
                        expr = rng.choice(pool)
                else:
                    expr = make_expression(selectivity, rng)
                client_id = f"churn-{next_client_id}"
                next_client_id += 1
                t_op = time.monotonic()
                reply = client.request("sidecar.subscribe", {"expression": expr, "client_id": client_id})
                latency = time.monotonic() - t_op
                if "error" in reply:
                    raise RuntimeError(f"subscribe failed mid-churn: {reply}")
                live.append((reply["id"], client_id, expr))
                if not in_ramp:
                    percentiles.record("subscribe", latency)
            else:
                idx = rng.randrange(len(live))
                sub_id, client_id, expr = live.pop(idx)
                t_op = time.monotonic()
                reply = client.request("sidecar.unsubscribe", {"id": sub_id, "client_id": client_id})
                latency = time.monotonic() - t_op
                if "error" in reply:
                    raise RuntimeError(f"unsubscribe failed mid-churn: {reply}")
                if not in_ramp:
                    percentiles.record("unsubscribe", latency)

            if interval > 0:
                remaining = interval - (time.monotonic() - t0)
                if remaining > 0:
                    time.sleep(remaining)
    finally:
        live_out.extend(live)
        client.close()


def fetch_all_subscriptions(client: NatsClient) -> list:
    """Pages through sidecar.list_subscriptions to completion - used only for the post-run
    correctness check, so a real request/reply round trip per page is fine here."""
    all_subs: list = []
    offset = 0
    while True:
        reply = client.request("sidecar.list_subscriptions", {"offset": offset, "limit": 1000})
        all_subs.extend(reply["subscriptions"])
        offset += reply["returned"]
        if reply["returned"] == 0 or offset >= reply["total_matching"]:
            break
    return all_subs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nats-server", required=True, type=Path)
    parser.add_argument("--sidecar", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--duration", type=float, default=30.0, help="test duration in seconds")
    parser.add_argument("--target-k", type=int, default=200, help="steady-state active subscription count")
    parser.add_argument("--churn-rate", type=float, default=20.0, help="subscribe+unsubscribe cycles/sec")
    parser.add_argument("--mode", choices=["warm", "cold"], default="warm")
    parser.add_argument("--data-rate", type=float, default=1000.0, help="data-plane messages/sec")
    parser.add_argument("--selectivity", type=float, default=10.0, help="target match fraction is 1/s")
    parser.add_argument("--stats-interval", type=float, default=2.0, help="sidecar.stats poll interval")
    parser.add_argument(
        "--ramp-seconds", type=float, default=3.0,
        help="initial period excluded from churn-latency percentiles (warm-mode pool population)")
    parser.add_argument("--input-subject", default="sensor.data")
    args = parser.parse_args()

    port = free_port()
    server = None
    sidecar = None

    with tempfile.TemporaryDirectory(prefix="nats-sidecar-churn-") as temp:
        temp_path = Path(temp)
        server_log = temp_path / "nats-server.log"
        sidecar_log = temp_path / "sidecar.log"

        try:
            with server_log.open("w") as output:
                server = subprocess.Popen(
                    [str(args.nats_server), "-js", "-p", str(port), "-sd", str(temp_path / "store")],
                    stdout=output, stderr=subprocess.STDOUT,
                )
            wait_for_log(server, server_log, "Server is ready")

            with sidecar_log.open("w") as output:
                sidecar = subprocess.Popen(
                    [
                        str(args.sidecar), "-c", str(args.config), "-p", str(port),
                        "--lease-bucket", "churn-test-leases",
                        "--lease-ttl", "3600",
                        "--lease-check-interval", "3600",
                        "--workers", "0",
                        "--stats-interval", "3600",
                    ],
                    stdout=output, stderr=subprocess.STDOUT,
                )
            wait_for_log(sidecar, sidecar_log, "Sidecar engine started")

            stop_event = threading.Event()
            percentiles = Percentiles()
            live_out: list = []

            data_thread = threading.Thread(
                target=data_plane_worker,
                args=("127.0.0.1", port, args.input_subject, args.data_rate, stop_event),
                daemon=True,
            )
            churn_thread = threading.Thread(
                target=churn_worker,
                args=("127.0.0.1", port, args.mode, args.target_k, args.churn_rate,
                      args.selectivity, args.ramp_seconds, stop_event, percentiles, live_out),
                daemon=True,
            )

            print(f"Starting: mode={args.mode} target_k={args.target_k} churn_rate={args.churn_rate}/s "
                  f"data_rate={args.data_rate}/s selectivity=1/{args.selectivity} duration={args.duration}s")

            stats_client = NatsClient("127.0.0.1", port)
            data_thread.start()
            churn_thread.start()

            deadline = time.monotonic() + args.duration
            while time.monotonic() < deadline:
                time.sleep(min(args.stats_interval, max(0, deadline - time.monotonic())))
                stats = stats_client.request("sidecar.stats", {})
                print(f"  t={args.duration - (deadline - time.monotonic()):5.1f}s  "
                      f"processed={stats.get('processed')} matched={stats.get('matched')} "
                      f"published={stats.get('published')} subscriptions={stats.get('subscriptions')} "
                      f"avg_match_us={stats.get('avg_match_us'):.2f} "
                      f"avg_fanout_us={stats.get('avg_fanout_us'):.2f}")

            stop_event.set()
            data_thread.join(timeout=10)
            churn_thread.join(timeout=10)

            final_stats = stats_client.request("sidecar.stats", {})
            actual_subs = fetch_all_subscriptions(stats_client)
            stats_client.close()

            # live_out holds one entry per LEASE (sub_id, client_id pair) - in warm mode many
            # leases legitimately share the same sub_id (that's the whole point of the pool), so
            # the correctness check needs the number of DISTINCT subscription ids, not the number
            # of leases, to match what sidecar.list_subscriptions itself counts (one row per
            # subscription, with its own lease_holder_count - not one row per lease).
            expected_k = len({sub_id for sub_id, _client_id, _expr in live_out})
            actual_k = len(actual_subs)
            correctness_ok = expected_k == actual_k

            print("\n=== Churn latency (steady-state, ramp excluded) ===")
            for op, p in percentiles.report().items():
                print(f"  {op}: n={p['count']} mean={p['mean_ms']:.2f}ms p50={p['p50_ms']:.2f}ms "
                      f"p99={p['p99_ms']:.2f}ms p999={p['p999_ms']:.2f}ms max={p['max_ms']:.2f}ms")

            print("\n=== Final data-plane stats ===")
            print(f"  {final_stats}")

            print("\n=== Correctness check (churn driver's own bookkeeping vs. list_subscriptions) ===")
            print(f"  expected active subscriptions: {expected_k}")
            print(f"  actual active subscriptions (via sidecar.list_subscriptions): {actual_k}")
            print(f"  {'PASS' if correctness_ok else 'FAIL'}")

            if not correctness_ok:
                raise AssertionError(
                    f"subscription count mismatch after churn: expected {expected_k}, got {actual_k} "
                    "- subscriptions were silently lost or leaked under concurrent churn+load")

        except Exception:
            for path in (server_log, sidecar_log):
                if path.exists():
                    print(f"\n--- {path.name} ---\n{path.read_text(errors='replace')}")
            raise
        finally:
            if sidecar is not None:
                stop_process(sidecar, "sidecar")
            if server is not None:
                stop_process(server, "nats-server")


if __name__ == "__main__":
    main()
