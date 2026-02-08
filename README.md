# nats_sidecar

Content-based filtering sidecar for NATS. Receives binary-encoded messages on a NATS subject, evaluates them against boolean expression subscriptions using an [a-tree](https://github.com/mrayva/a-tree), and fans out matching messages to per-subscription output topics.

## Features

- Boolean expression subscriptions (e.g. `temperature > 30.0 AND location = "warehouse"`)
- Supports MessagePack, CBOR, FlexBuffers, and Zera binary formats
- Multi-threaded worker pool for parallel message processing with RCU snapshot-based lock-free reads
- Soft-state leases via NATS KV with automatic TTL-based cleanup
- Expression deduplication across clients

## Prerequisites

- C++20 compiler (GCC 13+ or Clang 16+)
- CMake 3.25+
- Rust toolchain (for building the a-tree FFI library)
- [vcpkg](https://github.com/microsoft/vcpkg) (for C++ dependencies)

## Building

```bash
# Configure (first time only)
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake

# Build
cmake --build build

# Run tests
./build/tests/sidecar_test
```

The executable is placed at `build/bin/nats_sidecar`.

## Usage

```bash
./build/bin/nats_sidecar -c config/example.yaml
```

### CLI Options

| Flag | Description |
|------|-------------|
| `-c, --config PATH` | Path to YAML config file (required) |
| `-a, --address HOST` | NATS server address (overrides config) |
| `-p, --port PORT` | NATS server port (overrides config) |
| `-v, --verbose` | Enable debug logging |
| `-h, --help` | Print help |

## Configuration

See [`config/example.yaml`](config/example.yaml) for a full annotated example. Key settings:

```yaml
# NATS connection
nats_address: "127.0.0.1"
nats_port: 4222

# Input: NATS subject carrying binary-encoded messages
input_subject: "sensor.data"
format: msgpack          # msgpack | cbor | flexbuffers | zera

# Output: matched messages published to <output_prefix>.<subscription_id>
output_prefix: "sensor.filtered"

# Subscription management (request/reply)
subscribe_subject: "sidecar.subscribe"
unsubscribe_subject: "sidecar.unsubscribe"

# Soft-state leases (NATS KV)
lease_bucket: "sidecar-leases"
lease_ttl_seconds: 3600

# Attribute schema for boolean expressions
attributes:
  - name: temperature
    type: float
  - name: location
    type: string

# Worker threads (0 = auto-detect via hardware_concurrency)
worker_threads: 0
```

### Attribute Types

| Type | Description |
|------|-------------|
| `boolean` / `bool` | True/false |
| `integer` / `int` | 64-bit signed integer |
| `float` / `double` | 64-bit floating point |
| `string` / `str` | UTF-8 string |
| `string_list` | Array of strings |
| `integer_list` / `int_list` | Array of integers |

## Client Protocol

### Subscribe

Send a JSON request to the `subscribe_subject` (request/reply):

```json
{"expression": "temperature > 30.0 AND location = \"warehouse\"", "client_id": "my-client"}
```

Response:

```json
{
  "id": 1,
  "topic": "sensor.filtered.1",
  "lease_bucket": "sidecar-leases",
  "lease_key": "1.my-client",
  "lease_ttl_seconds": 3600
}
```

The client should then:
1. Subscribe to the returned `topic` for filtered messages
2. Periodically refresh the lease key in the KV bucket before TTL expires

### Unsubscribe

Send a JSON request to the `unsubscribe_subject`:

```json
{"id": 1, "client_id": "my-client"}
```

Response:

```json
{"id": 1, "removed": true}
```

`removed` is `true` if the subscription was fully removed (no remaining lease holders), `false` if other clients still hold leases.

## Architecture

```
NATS input subject
       |
  ASIO I/O Thread ──────────────────────────────────────────
       |                                                    |
  recv data msg                                subscribe/unsubscribe
       |                                            |
  copy payload                               mutex-protected write
       |                                     rebuild tree if needed
  enqueue to queue ──→ Worker Pool (N threads)      |
                       dequeue payload        atomic_store(new snapshot)
                       atomic_load(snapshot)
                       deserialize + match
                       co_spawn publish ──→ ASIO thread publishes to NATS
```

- The ASIO I/O thread handles all NATS network I/O and subscription control
- Worker threads process messages in parallel using lock-free RCU snapshots of the a-tree
- NATS publishes are posted back to the ASIO thread via `co_spawn`

## License

See LICENSE file.
