# nats_sidecar

Content-based filtering sidecar for NATS. Receives binary-encoded messages on a NATS subject, evaluates them against boolean expression subscriptions using an [a-tree](https://github.com/mrayva/a-tree), and fans out matching messages to per-subscription output topics.

## Features

- Boolean expression subscriptions (e.g. `temperature > 30.0 AND location = "warehouse"`)
- Supports MessagePack, CBOR, FlexBuffers, and Zera binary formats
- Multi-threaded worker pool for parallel message processing with RCU snapshot-based lock-free reads
- Soft-state leases via NATS KV with automatic TTL-based cleanup
- Expression deduplication across clients
- Schema generators for automatic attribute discovery (CLI from sample files, SQL from PostgreSQL tables)
- Full CLI configuration — run with a YAML config file, pure CLI flags, or both

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
# With a config file
./build/bin/nats_sidecar -c config/example.yaml

# Purely from CLI (no config file needed)
./build/bin/nats_sidecar -i sensor.data -f msgpack --attr temperature:float --attr location:string

# Config file with CLI overrides
./build/bin/nats_sidecar -c config/example.yaml -a 10.0.0.5 -p 4223 --workers 8
```

### CLI Options

All configuration parameters can be set via CLI flags. When a config file is also provided, CLI flags take precedence.

| Flag | Description |
|------|-------------|
| `-c, --config PATH` | Path to YAML config file |
| `-a, --address HOST` | NATS server address |
| `-p, --port PORT` | NATS server port |
| `-i, --input-subject SUBJ` | Input NATS subject |
| `-f, --format FMT` | Binary format (`msgpack`, `cbor`, `flexbuffers`, `zera`) |
| `--output-prefix PREFIX` | Output subject prefix (defaults to input subject) |
| `--queue-group GROUP` | Input queue group for load balancing |
| `--subscribe-subject SUBJ` | Subscription request subject |
| `--unsubscribe-subject SUBJ` | Unsubscription request subject |
| `--lease-bucket NAME` | NATS KV lease bucket name |
| `--lease-ttl SECS` | Lease TTL in seconds |
| `--lease-check-interval SECS` | Lease check interval in seconds |
| `--attr NAME:TYPE` | Attribute definition (repeatable) |
| `--workers N` | Worker thread count (0 = auto) |
| `--tls-cert PATH` | TLS certificate path |
| `--tls-key PATH` | TLS key path |
| `--tls-ca PATH` | TLS CA certificate path |
| `--stats-interval SECS` | Stats log interval in seconds |
| `--log-level LEVEL` | Log level (`debug`, `info`, `warn`, `error`) |
| `--generate-schema PATH` | Infer attributes from a sample binary file and print YAML |
| `-v, --verbose` | Enable debug logging (shorthand for `--log-level debug`) |
| `-h, --help` | Print help |

## Configuration

The sidecar can be configured via a YAML file, CLI flags, or a combination of both. When both are provided, CLI flags override the corresponding YAML values. A config file is not required — all parameters can be supplied via CLI.

The only required settings are `input_subject` and at least one attribute definition. Everything else has sensible defaults.

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

## Schema Generation

Writing the `attributes:` section by hand can be tedious and error-prone, especially for wide tables. Two helpers generate it automatically by inspecting actual data.

### CLI: `--generate-schema`

Point the sidecar at a sample binary file (e.g. one message captured from NATS or produced by pg_zerialize) and it will deserialize the message, inspect each field's runtime type, and print a ready-to-use YAML block:

```bash
./build/bin/nats_sidecar --generate-schema sample.msgpack
# or with an explicit format:
./build/bin/nats_sidecar --generate-schema sample.cbor -f cbor
```

Output (paste directly into your config file, or use as a reference for `--attr` flags):

```yaml
attributes:
  - name: id
    type: integer
  - name: temperature
    type: float
  - name: location
    type: string
  - name: tags
    type: string_list
```

The format defaults to `msgpack` if `-f` is not specified. Supported formats: `msgpack`, `cbor`, `flexbuffers`, `zera`.

For arrays, the generator peeks at the first element to distinguish `integer_list` from `string_list`. Null or unrecognizable fields default to `string` with a warning on stderr.

### SQL: `generate_sidecar_attributes()`

For tables serialized via pg_zerialize, load [`sql/generate_sidecar_attributes.sql`](sql/generate_sidecar_attributes.sql) into your PostgreSQL database, then:

```sql
SELECT generate_sidecar_attributes('sensor_readings');
-- or with explicit schema:
SELECT generate_sidecar_attributes('sensor_readings', 'public');
```

The function maps PostgreSQL column types to sidecar attribute types following pg_zerialize's `datum_to_dynamic()` dispatch:

| PostgreSQL type(s) | Sidecar type |
|---|---|
| `boolean` | `boolean` |
| `smallint`, `integer`, `bigint` | `integer` |
| `real`, `double precision`, `numeric` | `float` |
| `text`, `varchar`, `char(n)`, others | `string` |
| `int2[]`, `int4[]`, `int8[]` | `integer_list` |
| `text[]`, `varchar[]`, other arrays | `string_list` |

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
