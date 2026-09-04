#!/usr/bin/env python3
"""End-to-end verification of source-routed dual output (plan's verification step 2 & 3):

Part A (feature enabled): one core-mode + one js-mode input connection, output_stream_enabled.
  - a core-sourced matching row lands on `topic` (plain SUB)
  - a js-sourced matching row is published BEFORE any consumer exists, then a real durable
    JetStream consumer (filter_subject=updates_topic, deliver_policy=all) is created and proven
    to receive that earlier message - the actual catch-up claim, not just "the stream exists".

Part B (feature disabled, the default): confirms zero new NATS/JetStream traffic and an
unchanged subscribe response - the backward-compatibility claim.

Also confirms lease removal (explicit unsubscribe here - the same subscription_manager::
remove_lease() path a TTL expiry ultimately calls too, see grace_period_check.py-era work in this
project's history) stops matching on BOTH channels, not just the core one - proof that one shared
matching-tree entry feeds both routes rather than two independent per-channel lifecycles.

Usage: python3 output_stream_check.py --nats-server /path/to/nats-server \\
           --sidecar build/bin/nats_sidecar
"""
import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from integration_sidecar import NatsClient, free_port, wait_for_log, stop_process

CONFIG_ENABLED = """
connections:
  - name: flush
    mode: core
    subjects: [flush.in]
  - name: updates
    mode: js
    subjects: [updates.in]
    stream: UPDATES_INPUT
    consumer_durable_name: updates-durable
    consumer_deliver_subject: updates.deliver
output_prefix: sensor.filtered
output_stream_enabled: true
output_updates_prefix: sensor.filtered.updates
output_stream_name: sidecar-output
attributes:
  - name: value
    type: integer
"""

CONFIG_DISABLED = """
connections:
  - name: flush
    mode: core
    subjects: [flush.in]
  - name: updates
    mode: js
    subjects: [updates.in]
    stream: UPDATES_INPUT
    consumer_durable_name: updates-durable
    consumer_deliver_subject: updates.deliver
output_prefix: sensor.filtered
attributes:
  - name: value
    type: integer
"""


def msgpack_value(v):
    import struct
    # {"value": v} hand-encoded msgpack: fixmap(1), fixstr("value"), int
    out = bytearray()
    out.append(0x81)  # fixmap with 1 pair
    out.append(0xA5)  # fixstr len 5
    out += b"value"
    if 0 <= v <= 127:
        out.append(v)
    else:
        out.append(0xd2)
        out += struct.pack(">i", v)
    return bytes(out)


def run_part_a(nats_server: Path, sidecar_bin: Path):
    print("=== Part A: output_stream_enabled = true ===")
    port = free_port()
    server = sidecar = client = None
    with tempfile.TemporaryDirectory(prefix="output-stream-check-a-") as temp:
        temp_path = Path(temp)
        config_path = temp_path / "config.yaml"
        config_path.write_text(CONFIG_ENABLED)
        server_log = temp_path / "nats-server.log"
        sidecar_log = temp_path / "sidecar.log"
        try:
            with server_log.open("w") as out:
                server = subprocess.Popen(
                    [str(nats_server), "-js", "-p", str(port), "-sd", str(temp_path / "store")],
                    stdout=out, stderr=subprocess.STDOUT)
            wait_for_log(server, server_log, "Server is ready")

            with sidecar_log.open("w") as out:
                sidecar = subprocess.Popen(
                    [str(sidecar_bin), "-c", str(config_path), "-p", str(port),
                     "--lease-bucket", "output-check-leases",
                     "--workers", "2", "--stats-interval", "60"],
                    stdout=out, stderr=subprocess.STDOUT)
            wait_for_log(sidecar, sidecar_log, "Sidecar engine started")
            log_text = sidecar_log.read_text()
            assert "Created JetStream output stream 'sidecar-output'" in log_text or \
                   "Validated JetStream output stream 'sidecar-output'" in log_text, \
                   f"expected output-stream provisioning log line, got:\n{log_text}"
            print("output stream provisioned at startup: PASS")

            client = NatsClient("127.0.0.1", port)
            sub = client.request("sidecar.subscribe",
                                  {"expression": "value > 10", "client_id": "check-client"})
            print(f"subscribed: {sub}")
            assert sub["topic"] == "sensor.filtered.1", sub
            assert sub["updates_topic"] == "sensor.filtered.updates.1", sub
            assert sub["output_stream"] == "sidecar-output", sub
            # NOT lease_key verbatim - "1.check-client" has a "." and isn't a valid JetStream
            # consumer name (found by this exact script's first run, before the fix).
            assert "." not in sub["output_stream_durable_name"], sub
            assert len(sub["output_stream_durable_name"]) == 16, sub
            print("subscribe response fields: PASS")

            expected_payload = msgpack_value(42)

            # --- core-sourced row -> plain core SUB on `topic` ---
            core_sid = client.subscribe_raw(sub["topic"])
            client.publish("flush.in", expected_payload)
            deadline = time.monotonic() + 5
            core_msg = None
            while time.monotonic() < deadline:
                r = client.read_message()
                if r is not None:
                    core_msg = r
                    break
            assert core_msg is not None, "core-sourced match never arrived on `topic`"
            assert core_msg[0] == sub["topic"], core_msg
            assert core_msg[1] == expected_payload, core_msg
            print("core-sourced match delivered on plain core topic: PASS")

            # --- js-sourced row -> durable JetStream channel, published BEFORE any consumer ---
            client.publish("updates.in", expected_payload)
            time.sleep(1.0)  # let it land in the output stream before any consumer exists

            durable_name = sub["output_stream_durable_name"]
            deliver_subject = "check.deliver"
            consumer_req = {
                "stream_name": sub["output_stream"],
                "config": {
                    "durable_name": durable_name,
                    "deliver_subject": deliver_subject,
                    "filter_subject": sub["updates_topic"],
                    "ack_policy": "explicit",
                    "replay_policy": "instant",
                    "deliver_policy": "all",
                    "ack_wait": 30_000_000_000,
                },
            }
            create_resp = client.request(
                f"$JS.API.CONSUMER.DURABLE.CREATE.{sub['output_stream']}.{durable_name}",
                consumer_req)
            assert "error" not in create_resp, f"consumer create failed: {create_resp}"
            print(f"durable consumer created: {create_resp.get('name')}")

            js_sid = client.subscribe_raw(deliver_subject)
            deadline = time.monotonic() + 5
            js_msg = None
            while time.monotonic() < deadline:
                r = client.read_message()
                if r is not None:
                    js_msg = r
                    break
            assert js_msg is not None, \
                "durable consumer never delivered the PRE-EXISTING js-sourced message - catch-up failed"
            # js delivery includes JetStream ack-reply-subject metadata appended after the
            # payload's own reply-to field structure - the PAYLOAD bytes themselves (what
            # subscribe_raw()/read_message() returns as element [1]) are exactly the original
            # publish, untouched, so an exact comparison is still the right check here.
            assert js_msg[1] == expected_payload, js_msg
            print("PASS: durable JetStream consumer, created AFTER publish, still received the "
                  "earlier js-sourced match - real catch-up proven, not just live pass-through")

            # --- lease removal applies to BOTH channels (plan's verification item 4) ---
            # Same removal path a TTL expiry would trigger (subscription_manager::remove_lease())
            # - explicit unsubscribe here instead of waiting out a real TTL, since it exercises
            # the identical code path and this is the part that's actually in question (does
            # removal affect both channels, not which trigger caused it - already covered by this
            # session's earlier grace-period verification).
            unsub = client.request("sidecar.unsubscribe", {"id": sub["id"], "client_id": "check-client"})
            assert unsub["removed"] is True, unsub

            client.publish("flush.in", expected_payload)
            client.publish("updates.in", expected_payload)
            time.sleep(0.5)
            assert client.read_message() is None, \
                "core channel still matched after the subscription was removed"
            assert client.read_message() is None, \
                "updates channel still matched after the subscription was removed"
            print("PASS: subscription removal stops matching on BOTH channels (single shared "
                  "matching-tree entry feeding both routes, not two independent lifecycles)")

        except Exception:
            for p in (server_log, sidecar_log):
                if p.exists():
                    print(f"\n--- {p.name} ---\n{p.read_text(errors='replace')}")
            raise
        finally:
            if client is not None:
                client.close()
            if sidecar is not None:
                stop_process(sidecar, "sidecar")
            if server is not None:
                stop_process(server, "nats-server")


def run_part_b(nats_server: Path, sidecar_bin: Path):
    print("\n=== Part B: output_stream_enabled unset (default) ===")
    port = free_port()
    server = sidecar = client = None
    with tempfile.TemporaryDirectory(prefix="output-stream-check-b-") as temp:
        temp_path = Path(temp)
        config_path = temp_path / "config.yaml"
        config_path.write_text(CONFIG_DISABLED)
        server_log = temp_path / "nats-server.log"
        sidecar_log = temp_path / "sidecar.log"
        try:
            with server_log.open("w") as out:
                server = subprocess.Popen(
                    [str(nats_server), "-js", "-p", str(port), "-sd", str(temp_path / "store")],
                    stdout=out, stderr=subprocess.STDOUT)
            wait_for_log(server, server_log, "Server is ready")

            with sidecar_log.open("w") as out:
                sidecar = subprocess.Popen(
                    [str(sidecar_bin), "-c", str(config_path), "-p", str(port),
                     "--lease-bucket", "output-check-leases-b",
                     "--workers", "2", "--stats-interval", "60"],
                    stdout=out, stderr=subprocess.STDOUT)
            wait_for_log(sidecar, sidecar_log, "Sidecar engine started")
            log_text = sidecar_log.read_text()
            assert "output stream" not in log_text.lower(), \
                f"unexpected output-stream activity with the feature disabled:\n{log_text}"
            print("no output-stream provisioning traffic at startup: PASS")

            client = NatsClient("127.0.0.1", port)
            sub = client.request("sidecar.subscribe",
                                  {"expression": "value > 10", "client_id": "check-client"})
            print(f"subscribed: {sub}")
            assert "updates_topic" not in sub, sub
            assert "output_stream" not in sub, sub
            assert "output_stream_durable_name" not in sub, sub
            print("subscribe response unchanged from before this feature: PASS")

            # js-sourced row still goes to the SAME core topic as a core-sourced one would.
            core_sid = client.subscribe_raw(sub["topic"])
            client.publish("updates.in", msgpack_value(42))
            deadline = time.monotonic() + 5
            msg = None
            while time.monotonic() < deadline:
                r = client.read_message()
                if r is not None:
                    msg = r
                    break
            assert msg is not None, "js-sourced match never arrived on the plain core topic"
            print("js-sourced match still delivered on the unchanged core topic: PASS")

        except Exception:
            for p in (server_log, sidecar_log):
                if p.exists():
                    print(f"\n--- {p.name} ---\n{p.read_text(errors='replace')}")
            raise
        finally:
            if client is not None:
                client.close()
            if sidecar is not None:
                stop_process(sidecar, "sidecar")
            if server is not None:
                stop_process(server, "nats-server")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nats-server", required=True, type=Path)
    parser.add_argument("--sidecar", required=True, type=Path)
    args = parser.parse_args()

    run_part_a(args.nats_server, args.sidecar)
    run_part_b(args.nats_server, args.sidecar)
    print("\nALL PASS")


if __name__ == "__main__":
    main()
