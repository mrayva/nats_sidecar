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
            # percentiles_of() is defined later in this file (module-level function-call
            # resolution happens at call time, not definition time - fine regardless of source
            # order) - shared with analyze_data_plane_latency() so there's exactly one percentile
            # computation in this tool, not two copies that could quietly drift apart.
            return {op: percentiles_of(samples) for op, samples in self._samples.items() if samples}


def data_plane_worker(
    nats_host: str, nats_port: int, subject: str, rate: float, stop_event: threading.Event,
    sent_seqs: set,
) -> None:
    """Publishes msgpack-encoded {"temperature": v, "seq": n, "send_ts": t} rows at
    (approximately - see the module's own docstring: no claim of precise real-time scheduling
    here, this is a load generator, not a timing benchmark) `rate` messages/sec until stop_event
    is set. Its own dedicated NatsClient/TCP connection - never shared with the churn thread's.

    `seq`/`send_ts` ride along verbatim in the republished output for any row-mode message that
    matches (worker_pool.cpp forwards qm.payload - the client's own original raw bytes - not a
    re-serialized copy, confirmed by reading worker_loop() directly), and populate_event() silently
    skips any key outside the configured schema (event_bridge.hpp: `if (!type_opt) continue;`) -
    so these two extra fields never affect matching, but do let output_listener_worker() correlate
    a received message back to when it was actually sent, using this SAME process's own
    time.monotonic() clock (both threads live in this one Python process - no cross-machine clock
    sync concern). `send_ts` is what the latency measurement is built on; `seq` is only used for
    an approximate drop-count sanity check (`sent_seqs` collects every seq actually sent)."""
    client = NatsClient(nats_host, nats_port)
    rng = random.Random()
    interval = 1.0 / rate if rate > 0 else 0
    seq = 0
    try:
        while not stop_event.is_set():
            t0 = time.monotonic()
            row = {"temperature": rng.uniform(0, TEMP_MAX), "seq": seq, "send_ts": t0}
            payload = msgpack.packb(row, use_bin_type=True)
            client.publish(subject, payload)
            sent_seqs.add(seq)
            seq += 1
            if interval > 0:
                remaining = interval - (time.monotonic() - t0)
                if remaining > 0:
                    time.sleep(remaining)
    finally:
        client.close()


def output_listener_worker(
    nats_host: str, nats_port: int, topic: str, stop_event: threading.Event, samples: list,
) -> None:
    """Subscribes directly to the matched-output topic (via subscribe_raw()/read_message() - a
    standing subscription, not request()'s one-shot reply pattern) and records
    (send_ts, recv_ts, latency_seconds) for every delivered message - the actual end-to-end
    publish -> filtered-output latency this tool exists to measure, not a proxy for it. Its own
    dedicated NatsClient with a short socket_timeout (0.5s, vs. the other connections' default 5s)
    so its read loop can check stop_event responsively instead of blocking for seconds at
    shutdown."""
    client = NatsClient(nats_host, nats_port, socket_timeout=0.5)
    client.subscribe_raw(topic)
    try:
        while not stop_event.is_set():
            event = client.read_message()
            if event is None:
                continue
            _subject, payload = event
            recv_ts = time.monotonic()
            try:
                row = msgpack.unpackb(payload, raw=False)
            except Exception:
                continue  # not one of ours, or truncated - skip rather than crash the listener
            send_ts = row.get("send_ts")
            if send_ts is None:
                continue
            samples.append((send_ts, recv_ts, recv_ts - send_ts, row.get("seq")))
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
    rebuild_windows: list,
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
                t_op_end = time.monotonic()
                latency = t_op_end - t_op
                if "error" in reply:
                    raise RuntimeError(f"unsubscribe failed mid-churn: {reply}")
                # Only a FULLY removed subscription (no lease holders left) triggers
                # subscription_manager::rebuild_tree_locked() - a partial removal (other clients
                # still hold it) is cheap and never touches the tree at all. Bucketing these
                # separately (instead of one blended "unsubscribe" percentile, an earlier version
                # of this function's own mistake) is what makes the rebuild cost itself directly
                # visible, not diluted by however many cheap partial removals happen alongside it.
                if not in_ramp:
                    if reply.get("removed"):
                        percentiles.record("unsubscribe_full_removal", latency)
                        rebuild_windows.append((t_op, t_op_end))
                    else:
                        percentiles.record("unsubscribe_partial", latency)

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


def percentiles_of(values: list) -> dict:
    if not values:
        return {}
    s = sorted(values)
    return {
        "count": len(s),
        "mean_ms": statistics.mean(s) * 1000,
        "p50_ms": s[len(s) // 2] * 1000,
        "p99_ms": s[min(len(s) - 1, int(len(s) * 0.99))] * 1000,
        "p999_ms": s[min(len(s) - 1, int(len(s) * 0.999))] * 1000,
        "max_ms": s[-1] * 1000,
    }


def analyze_data_plane_latency(
    latency_samples: list, rebuild_windows: list, run_start: float, ramp_seconds: float
) -> dict:
    """Splits data-plane (send_ts, recv_ts, latency, seq) samples into two groups: a message's
    own processing interval [send_ts, recv_ts] overlaps at least one recorded full-removal
    rebuild_tree_locked() window, or it doesn't - then reports percentiles for BOTH groups
    separately. This is the actual causal test for whether the rebuild's exclusive-lock hold
    stalls worker-thread matching: if it does, "during rebuild" latency should be measurably
    worse than "isolated" latency, not just a slightly noisier version of the same distribution.
    O(samples * rebuild_windows) overlap check - fine at this tool's scale (thousands of samples,
    tens to low hundreds of rebuild windows)."""
    steady_state = [s for s in latency_samples if s[0] >= run_start + ramp_seconds]

    def overlaps_any_window(send_ts: float, recv_ts: float) -> bool:
        return any(send_ts <= end and recv_ts >= start for start, end in rebuild_windows)

    during = [s[2] for s in steady_state if overlaps_any_window(s[0], s[1])]
    isolated = [s[2] for s in steady_state if not overlaps_any_window(s[0], s[1])]
    return {
        "overall": percentiles_of([s[2] for s in steady_state]),
        "during_rebuild_window": percentiles_of(during),
        "isolated": percentiles_of(isolated),
    }


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
    parser.add_argument(
        "--engine", choices=["atree", "betree", "pstree"], default=None,
        help="override the matching engine (default: whatever --config itself specifies). "
             "A real gap found while first using this tool for its own stated purpose: without "
             "this flag, every run silently used --config's own engine setting (config/example.yaml "
             "defaults to atree) regardless of which engine the caller actually meant to "
             "characterize - rebuild_tree_locked()'s cost is real for EVERY engine that doesn't "
             "support matching_engine::remove() (today: only pstree does), so a caller testing "
             "pstree specifically must pass --engine pstree explicitly, not assume the config "
             "file's own default matches their intent.")
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

            sidecar_args = [
                str(args.sidecar), "-c", str(args.config), "-p", str(port),
                "--lease-bucket", "churn-test-leases",
                "--lease-ttl", "3600",
                "--lease-check-interval", "3600",
                "--workers", "0",
                "--stats-interval", "3600",
            ]
            if args.engine is not None:
                sidecar_args += ["--engine", args.engine]

            with sidecar_log.open("w") as output:
                sidecar = subprocess.Popen(
                    sidecar_args,
                    stdout=output, stderr=subprocess.STDOUT,
                )
            wait_for_log(sidecar, sidecar_log, "Sidecar engine started")

            stop_event = threading.Event()
            percentiles = Percentiles()
            live_out: list = []
            rebuild_windows: list = []
            latency_samples: list = []
            sent_seqs: set = set()

            stats_client = NatsClient("127.0.0.1", port)

            # Latency probe: one dedicated subscription matching every published row
            # ("temperature >= 0.0" - values are uniform in [0, TEMP_MAX)), so every data-plane
            # message gets republished to its own topic - that's what output_listener_worker()
            # measures real end-to-end latency against. A real client_id of its own, entirely
            # separate from the churn driver's own churn-N client ids, so this subscription is
            # never touched by churn (it must survive the whole run to keep measuring).
            probe_reply = stats_client.request(
                "sidecar.subscribe", {"expression": "temperature >= 0.0", "client_id": "latency-probe"})
            if "error" in probe_reply:
                raise RuntimeError(f"failed to set up latency-probe subscription: {probe_reply}")
            probe_topic = probe_reply["topic"]

            data_thread = threading.Thread(
                target=data_plane_worker,
                args=("127.0.0.1", port, args.input_subject, args.data_rate, stop_event, sent_seqs),
                daemon=True,
            )
            churn_thread = threading.Thread(
                target=churn_worker,
                args=("127.0.0.1", port, args.mode, args.target_k, args.churn_rate,
                      args.selectivity, args.ramp_seconds, stop_event, percentiles, live_out,
                      rebuild_windows),
                daemon=True,
            )
            listener_thread = threading.Thread(
                target=output_listener_worker,
                args=("127.0.0.1", port, probe_topic, stop_event, latency_samples),
                daemon=True,
            )

            print(f"Starting: engine={args.engine or '(from --config)'} mode={args.mode} "
                  f"target_k={args.target_k} churn_rate={args.churn_rate}/s "
                  f"data_rate={args.data_rate}/s selectivity=1/{args.selectivity} duration={args.duration}s")

            # Listener first, so it's already receiving before any data-plane traffic exists -
            # otherwise the very first messages would be measured as "lost" rather than just
            # "sent before we were listening".
            listener_thread.start()
            time.sleep(0.2)
            run_start = time.monotonic()
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
            listener_thread.join(timeout=10)

            final_stats = stats_client.request("sidecar.stats", {})
            actual_subs = fetch_all_subscriptions(stats_client)
            stats_client.close()

            # live_out holds one entry per LEASE (sub_id, client_id pair) - in warm mode many
            # leases legitimately share the same sub_id (that's the whole point of the pool), so
            # the correctness check needs the number of DISTINCT subscription ids, not the number
            # of leases, to match what sidecar.list_subscriptions itself counts (one row per
            # subscription, with its own lease_holder_count - not one row per lease). The
            # latency-probe subscription itself is real, active, and correctly shows up in
            # list_subscriptions too, but it's never part of the churn driver's own bookkeeping -
            # exclude its id from both sides rather than let it look like an off-by-one leak.
            expected_k = len({sub_id for sub_id, _client_id, _expr in live_out})
            actual_k = len([s for s in actual_subs if s["id"] != probe_reply["id"]])
            correctness_ok = expected_k == actual_k

            print("\n=== Churn latency (steady-state, ramp excluded) ===")
            for op, p in percentiles.report().items():
                print(f"  {op}: n={p['count']} mean={p['mean_ms']:.2f}ms p50={p['p50_ms']:.2f}ms "
                      f"p99={p['p99_ms']:.2f}ms p999={p['p999_ms']:.2f}ms max={p['max_ms']:.2f}ms")

            print("\n=== Data-plane latency (publish -> filtered-output delivery, steady-state) ===")
            dp = analyze_data_plane_latency(latency_samples, rebuild_windows, run_start, args.ramp_seconds)
            for group in ("overall", "isolated", "during_rebuild_window"):
                p = dp[group]
                if not p:
                    print(f"  {group}: (no samples)")
                    continue
                print(f"  {group}: n={p['count']} mean={p['mean_ms']:.2f}ms p50={p['p50_ms']:.2f}ms "
                      f"p99={p['p99_ms']:.2f}ms p999={p['p999_ms']:.2f}ms max={p['max_ms']:.2f}ms")
            print(f"  {len(rebuild_windows)} full-removal rebuild window(s) recorded "
                  f"(cumulative {sum(end - start for start, end in rebuild_windows) * 1000:.1f}ms "
                  "of exclusive-lock hold time)")
            approx_dropped = len(sent_seqs) - len({s[3] for s in latency_samples if s[3] is not None})
            print(f"  approx. dropped (sent but never observed on output, incl. any still in "
                  f"flight at shutdown): {approx_dropped} of {len(sent_seqs)} sent")

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
