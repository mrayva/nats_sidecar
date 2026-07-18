#!/usr/bin/env python3

import argparse
import json
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path


class NatsClient:
    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.create_connection((host, port), timeout=5)
        self._socket.settimeout(5)
        self._stream = self._socket.makefile("rb")
        self._next_sid = 1
        self._next_inbox = 1

        info = self._readline()
        if not info.startswith(b"INFO "):
            raise RuntimeError(f"expected INFO, received {info!r}")
        self._send(b'CONNECT {"verbose":false,"pedantic":false}\r\nPING\r\n')
        self._wait_for_pong()

    def close(self) -> None:
        self._stream.close()
        self._socket.close()

    def request(self, subject: str, body: dict, timeout: float = 10) -> dict:
        sid = self._next_sid
        self._next_sid += 1
        inbox = f"_INBOX.sidecar.integration.{self._next_inbox}"
        self._next_inbox += 1
        payload = json.dumps(body, separators=(",", ":")).encode()

        self._send(
            f"SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\nPING\r\n".encode()
        )
        self._wait_for_pong()
        self._send(
            f"PUB {subject} {inbox} {len(payload)}\r\n".encode()
            + payload
            + b"\r\n"
        )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            self._socket.settimeout(remaining)
            event = self._read_event()
            if event is None:
                continue
            reply_subject, reply_payload = event
            if reply_subject == inbox:
                return json.loads(reply_payload)
        raise TimeoutError(f"request to {subject!r} timed out")

    def _send(self, data: bytes) -> None:
        self._socket.sendall(data)

    def _readline(self) -> bytes:
        line = self._stream.readline()
        if not line:
            raise ConnectionError("NATS connection closed")
        if not line.endswith(b"\r\n"):
            raise RuntimeError(f"invalid NATS line: {line!r}")
        return line[:-2]

    def _wait_for_pong(self) -> None:
        while True:
            line = self._readline()
            if line == b"PONG":
                return
            if line == b"PING":
                self._send(b"PONG\r\n")
            elif line.startswith(b"-ERR"):
                raise RuntimeError(line.decode(errors="replace"))

    def _read_event(self):
        line = self._readline()
        if line == b"PING":
            self._send(b"PONG\r\n")
            return None
        if line == b"PONG" or line == b"+OK" or line.startswith(b"INFO "):
            return None
        if line.startswith(b"-ERR"):
            raise RuntimeError(line.decode(errors="replace"))
        if not line.startswith(b"MSG "):
            raise RuntimeError(f"unexpected NATS protocol line: {line!r}")

        fields = line.split()
        if len(fields) not in (4, 5):
            raise RuntimeError(f"invalid MSG line: {line!r}")
        size = int(fields[-1])
        payload = self._stream.read(size)
        if len(payload) != size or self._stream.read(2) != b"\r\n":
            raise RuntimeError("truncated NATS message")
        return fields[1].decode(), payload.decode()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_log(process: subprocess.Popen, log_path: Path, text: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited with status {process.returncode}")
        if log_path.exists() and text in log_path.read_text(errors="replace"):
            return
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for {text!r}")


def stop_process(process: subprocess.Popen, name: str) -> None:
    if process.poll() is not None:
        if process.returncode != 0:
            raise RuntimeError(f"{name} exited with status {process.returncode}")
        return
    process.send_signal(signal.SIGINT)
    try:
        returncode = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise TimeoutError(f"{name} did not stop after SIGINT")
    if returncode != 0:
        raise RuntimeError(f"{name} exited with status {returncode}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats-server", required=True, type=Path)
    parser.add_argument("--sidecar", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()

    port = free_port()
    server = None
    sidecar = None
    client = None

    with tempfile.TemporaryDirectory(prefix="nats-sidecar-integration-") as temp:
        temp_path = Path(temp)
        server_log = temp_path / "nats-server.log"
        sidecar_logs = [temp_path / "sidecar-1.log", temp_path / "sidecar-2.log"]

        try:
            with server_log.open("w") as output:
                server = subprocess.Popen(
                    [
                        str(args.nats_server),
                        "-js",
                        "-p",
                        str(port),
                        "-sd",
                        str(temp_path / "store"),
                    ],
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
            wait_for_log(server, server_log, "Server is ready")

            def start_sidecar(log_path: Path) -> subprocess.Popen:
                output = log_path.open("w")
                process = subprocess.Popen(
                    [
                        str(args.sidecar),
                        "-c",
                        str(args.config),
                        "-p",
                        str(port),
                        "--lease-bucket",
                        "sidecar-integration-leases",
                        "--lease-ttl",
                        "30",
                        "--lease-check-interval",
                        "1",
                        "--workers",
                        "2",
                        "--stats-interval",
                        "60",
                    ],
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
                output.close()
                wait_for_log(process, log_path, "Sidecar engine started")
                return process

            sidecar = start_sidecar(sidecar_logs[0])
            client = NatsClient("127.0.0.1", port)

            first = client.request(
                "sidecar.subscribe",
                {
                    "expression": "temperature > 30.0",
                    "client_id": "integration-client-1",
                },
            )
            if first.get("id") != 1 or first.get("lease_key") != "1.integration-client-1":
                raise AssertionError(f"unexpected first subscription reply: {first}")

            stop_process(sidecar, "first sidecar")
            sidecar = None
            client.close()
            client = None

            sidecar = start_sidecar(sidecar_logs[1])
            wait_for_log(sidecar, sidecar_logs[1], "restored 1 active lease(s)")
            client = NatsClient("127.0.0.1", port)

            second = client.request(
                "sidecar.subscribe",
                {
                    "expression": "severity = 5",
                    "client_id": "integration-client-2",
                },
            )
            if second.get("id") != 2:
                raise AssertionError(f"restored ID sequence was not preserved: {second}")

            for subscription_id, client_id in (
                (1, "integration-client-1"),
                (2, "integration-client-2"),
            ):
                reply = client.request(
                    "sidecar.unsubscribe",
                    {"id": subscription_id, "client_id": client_id},
                )
                if reply != {"id": subscription_id, "removed": True}:
                    raise AssertionError(f"unexpected unsubscribe reply: {reply}")

            print("sidecar JetStream integration test passed")
        except Exception:
            for path in (server_log, *sidecar_logs):
                if path.exists():
                    print(f"\n--- {path.name} ---\n{path.read_text(errors='replace')}")
            raise
        finally:
            if client is not None:
                client.close()
            if sidecar is not None:
                stop_process(sidecar, "sidecar")
            if server is not None:
                stop_process(server, "nats-server")


if __name__ == "__main__":
    main()
