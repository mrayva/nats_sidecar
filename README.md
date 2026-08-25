# nats_sidecar

Content-based filtering sidecar for NATS. Receives binary-encoded messages on a NATS subject, evaluates them against boolean expression subscriptions using [a-tree](https://github.com/mrayva/a-tree) or [be-tree](https://github.com/mrayva/be-tree) (selectable per deployment, see `--engine` below), and fans out matching messages to per-subscription output topics.

## Features

- Boolean expression subscriptions (e.g. `temperature > 30.0 AND location = "warehouse"`)
- Two selectable matching engines (a-tree, be-tree) sharing the same expression syntax
- Supports MessagePack, CBOR, FlexBuffers, Zera, Ion, BSON, and BEVE binary formats
- Multi-threaded worker pool for parallel message processing with RCU snapshot-based lock-free reads
- Soft-state leases via NATS KV with automatic TTL-based cleanup
- Expression deduplication across clients
- Schema generators for automatic attribute discovery (CLI from sample files, SQL from PostgreSQL tables)
- Full CLI configuration — run with a YAML config file, pure CLI flags, or both

## Prerequisites

- C++23 compiler (GCC 13+ or Clang 16+)
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

# Run the strict-JetStream lifecycle integration test
python3 tests/integration_sidecar.py \
  --nats-server /path/to/nats-server \
  --sidecar build/bin/nats_sidecar \
  --config config/example.yaml

# Optional AddressSanitizer + UndefinedBehaviorSanitizer build
cmake -B build-asan -S . \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DSIDECAR_SANITIZER=address
cmake --build build-asan
ctest --test-dir build-asan --output-on-failure
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

The input-related flags below (`--input-subject` through `--input-stream-storage`) only apply to
the legacy single-connection config shape - they're rejected if the config file defines
`connections:` (see "Multiple input connections"). Every other flag works with either shape.

| Flag | Description |
|------|-------------|
| `-c, --config PATH` | Path to YAML config file |
| `-a, --address HOST` | NATS server address |
| `-p, --port PORT` | NATS server port |
| `-i, --input-subject SUBJ` | Input NATS subject (repeatable: `-i a -i b` for multiple inputs) |
| `-f, --format FMT` | Binary format (`msgpack`, `cbor`, `flexbuffers`, `zera`, `ion`, `bson`, `beve`) |
| `--engine ENGINE` | Matching engine (`atree`, `betree`); defaults to `atree` |
| `--output-prefix PREFIX` | Output subject prefix (defaults to input subject) |
| `--queue-group GROUP` | Input queue group for load balancing (plain, non-durable mode) |
| `--input-stream NAME` | JetStream stream name for the durable-consumer input mode (see below); enables it when set, alongside the four flags below |
| `--consumer-durable-name NAME` | Shared JetStream durable consumer name (required with `--input-stream`) |
| `--consumer-deliver-subject SUBJ` | Fixed, shared push-delivery subject - required, never left to auto-generate (see "Durable JetStream consumer" below for why) |
| `--consumer-deliver-group GROUP` | Queue group on the delivery subject - the JetStream analog of `--queue-group` |
| `--consumer-max-ack-pending N` | Flow-control cap on unacked in-flight messages per consumer (default 1000) |
| `--consumer-ack-wait SECS` | Redelivery timeout for unacked messages (default 30) |
| `--input-stream-storage file\|memory` | JetStream input-stream storage backend (default `file`); `memory` exists purely for isolating the durable mode's protocol cost from its disk-I/O cost - not for production use, since it loses all persisted messages on a `nats-server` restart |
| `--subscribe-subject SUBJ` | Subscription request subject |
| `--unsubscribe-subject SUBJ` | Unsubscription request subject |
| `--lease-bucket NAME` | NATS KV lease bucket name |
| `--lease-ttl SECS` | Lease TTL in seconds |
| `--lease-check-interval SECS` | Lease reconciliation interval in seconds |
| `--attr NAME:TYPE` | Attribute definition (repeatable) |
| `--workers N` | Worker thread count (0 = auto) |
| `--input-queue-max-messages N` | Maximum queued input messages |
| `--input-queue-max-bytes N` | Maximum queued input bytes |
| `--publish-max-inflight N` | Maximum in-flight publication tasks |
| `--publish-backpressure-timeout-ms MS` | NATS output backpressure timeout |
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

The only required settings are at least one input subject (via `connections` or the legacy
`input_subjects`) and at least one attribute definition. Everything else has sensible defaults.

See [`config/example.yaml`](config/example.yaml) for a full annotated example. Key settings:

```yaml
# NATS connection
nats_address: "127.0.0.1"
nats_port: 4222

# Input: NATS subject(s) carrying binary-encoded messages, all sharing the
# one attribute schema below - `-i`/`--input-subject` is repeatable on the
# CLI for the same effect. With more than one subject, output_prefix must
# be set explicitly (see below); with exactly one, it defaults to that
# subject. This is the legacy single-connection shape; see "Multiple input
# connections" below for running several independently-configured inputs
# (mixing durable and best-effort mode) in one process.
input_subjects: ["sensor.data"]
format: msgpack          # msgpack | cbor | flexbuffers | zera | ion | bson | beve
engine: atree             # atree | betree

# Output: matched messages published to <output_prefix>.<subscription_id>,
# shared by every input connection - a client's subscribed expression
# matches a message regardless of which connection it arrived on.
output_prefix: "sensor.filtered"

# Subscription management (request/reply)
subscribe_subject: "sidecar.subscribe"
unsubscribe_subject: "sidecar.unsubscribe"

# Soft-state leases (NATS KV)
lease_bucket: "sidecar-leases"
lease_ttl_seconds: 3600
lease_check_interval_seconds: 60

# Attribute schema for boolean expressions
attributes:
  - name: temperature
    type: float
  - name: location
    type: string

# Worker threads (0 = auto-detect via hardware_concurrency)
worker_threads: 0

# Bounded flow control
input_queue_max_messages: 10000
input_queue_max_bytes: 67108864
publish_max_inflight: 1024
publish_backpressure_timeout_ms: 5000
```

### Multiple input connections

A single process can consume from any number of independently configured input connections at
once, each choosing its own mode - `js` (durable JetStream consumer, the default) or `core`
(plain queue-group, best-effort) - instead of the whole process being one mode or the other. This
is the config-file-only `connections:` list, an alternative to (not combinable with) the flat
`input_subjects`/`input_stream`/etc. fields above:

```yaml
connections:
  - name: orders            # required, unique among connections
    mode: js                 # "js" (default) or "core" - omit for js
    subjects: ["orders.in"]
    stream: ORDERS_STREAM
    consumer_durable_name: orders-durable
    consumer_deliver_subject: orders.deliver
    consumer_deliver_group: orders-group    # optional
    consumer_max_ack_pending: 1000          # optional, default shown
    consumer_ack_wait_seconds: 30           # optional, default shown
    stream_storage: file                    # optional, "file" | "memory"

  - name: telemetry
    mode: core
    subjects: ["telemetry.in"]
    queue_group: telemetry-workers          # optional

output_prefix: "matched"    # still one shared value - see below
attributes:
  - name: value
    type: integer
```

A realistic use: durable delivery for subjects where losing a message is unacceptable (orders),
best-effort for everything else (telemetry/heartbeats), in one sidecar process instead of two.

Every connection matches against the **same** shared matching tree, attribute schema, and
`output_prefix` - a client subscribes once and gets matches regardless of which connection they
arrived on. Only the input side (subjects, mode, and JetStream-specific settings) is
per-connection; `subscribe_subject`/`unsubscribe_subject`/`output_prefix`/`attributes`/`engine`
stay process-wide, exactly as in the single-connection shape. All connections also still share
one underlying NATS connection - `connections` is not a way to talk to multiple NATS servers.

`stream`, `consumer_durable_name`, and `consumer_deliver_subject` must each be unique across every
`js`-mode connection, and no subject may be repeated across any two connections regardless of
mode - both are rejected at startup with a clear error rather than silently double-enqueuing or
merging two unrelated durable consumers.

CLI single-value flags (`--input-subject`, `--input-stream`, `--consumer-durable-name`, etc.) only
apply to the legacy single-connection shape; they're rejected with an error if the loaded config
file already defines `connections:`, since there's no way to know which named connection a bare
flag should target. Every other flag (`--workers`, `--log-level`, `--lease-ttl`, ...) still works
normally alongside `connections:`.

### Attribute Types

| Type | Description |
|------|-------------|
| `boolean` / `bool` | True/false |
| `integer` / `int` | 64-bit signed integer |
| `float` / `double` | 64-bit floating point |
| `string` / `str` | UTF-8 string |
| `string_list` | Array of strings |
| `integer_list` / `int_list` | Array of integers |

### Expression Syntax

Both engines accept the same expression syntax verbatim: `=`, `<>`, `<`, `<=`, `>`, `>=`, `and`, `or`, `not`, `in`, `not in`, `one of`, `none of`, `all of`, `is null`, `is not null`, `is empty`. Multi-word operators are space-separated (`not in`, not `not_in`) - this matches both engines' actual grammars, confirmed against their lexers directly.

One operator is engine-specific: **`is not empty` is only available under `engine: atree`**. be-tree's grammar has no rule for it at all; subscribing with it under `engine: betree` is rejected at subscribe time with a clear error rather than silently misbehaving. There's no substitute expression to fall back to if you need this on be-tree.

`in` / `not in` list literals accept integer or string values only, not floats. `one of` / `none of` / `all of` apply to list-typed attributes (`string_list` / `integer_list`); `is empty` / `is not empty` likewise.

`all of` means "the attribute contains every listed value" on both engines - but only because it's made to: a-tree's own native `all of` actually checks the opposite (the attribute is a subset of the literal list), so `matching_engine` transparently rewrites `X all of (v1, ..., vn)` into `X one of (v1) and ... and X one of (vn)` before handing it to a-tree, which evaluates to the same (be-tree-matching) result. This is invisible in normal use - the rewrite happens automatically inside `insert()` - but worth knowing if you're reading a-tree's own docs/tests, which describe `all of` differently.

## Client Protocol

### Subscribe

Send a JSON request to the `subscribe_subject` (request/reply):

```json
{"expression": "temperature > 30.0 AND location = \"warehouse\"", "client_id": "my-client"}
```

Optionally include a client-generated `"id"` (a `uint64`) to broadcast one subscribe request to
N instances sharing a single control subject, instead of one per-instance-unique subject each:
every instance that processes it adopts the same id (via `subscription_manager::restore()`, the
same mechanism that already re-adopts a persisted id on lease-restart) rather than each assigning
its own from an uncoordinated counter - so all instances converge on the identical output topic
without needing a reply first. Control-plane setup latency: flat ~13-18ms regardless of N,
compared to the old per-instance-request design's linear cost (9.1ms->257ms, N=1->32). Omitting
`"id"` keeps the original per-instance auto-assigned behavior. A genuine id conflict (that id
already bound to a different expression on an instance) is returned as a real error, not silently
absorbed.

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

The sidecar creates the initial KV lease atomically with the subscription. The
client should then:

1. Subscribe to the returned `topic` for filtered messages.
2. Repeat the same subscribe request before the TTL expires. This is idempotent
   and refreshes the server-owned lease record.

The sidecar creates the KV bucket when it is absent and validates its TTL and
history settings when it already exists. A configuration mismatch fails startup.
Lease records contain enough metadata to restore active subscriptions with their
original IDs after a sidecar restart.

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

## Durable JetStream consumer: loss-proof input, at a real cost

By default (`--queue-group`, or a `connections:` entry with `mode: core`), the sidecar consumes
its input via a plain core-NATS queue-group subscription - simple and fast, but core NATS pub/sub
is at-most-once by design: no ack, no redelivery, no flow control beyond a hard connection-buffer
disconnect. Real stress testing found this can lose messages under load (confirmed via
`nats-server`'s own "Slow Consumer Detected" log line under a JetStream-publish burst).

Setting `--input-stream`/`--consumer-durable-name`/`--consumer-deliver-subject`/
`--consumer-deliver-group` (or, for the `connections:` shape, `mode: js` - the default - with
`stream`/`consumer_durable_name`/`consumer_deliver_subject`/`consumer_deliver_group`) switches
input consumption to a durable JetStream push consumer with explicit application-controlled acks
instead - additive, not a replacement; plain/core mode still works unchanged on connections that
don't opt in. This choice is made independently per connection: one process can run some
connections in durable `js` mode and others in best-effort `core` mode at once (see "Multiple
input connections" above). Ack timing: a matched-and-published message is acked only
after the publish write succeeds; a legitimately non-matching message is acked immediately;
malformed/unparseable input is `term()`'d (not silently dropped) so it never redelivers forever;
anything left over (backpressure, a transient publish failure) is left unacked so `--consumer-ack-wait`
triggers real redelivery instead of silent loss. Verified against the exact stress condition that
found the loss above, 15 real trials: 0/15 stalled, exact ground truth every time.

**The real, measured cost**: roughly 2.6-2.9x slower true end-to-end throughput than
`--queue-group` mode (e.g. N=16: ~195,770/s plain vs. ~66,771/s durable). Profiled directly with
`perf` on the real `nats-server` process (differential: plain vs. durable-consumer) to find out
why, rather than assume:

- It is **not** disk persistence. `--input-stream-storage memory` (an isolation-testing flag,
  never meant for production - it loses everything on a `nats-server` restart) only recovers 3-6%
  of the gap. The `perf` profile confirms this precisely: the disk-write function family
  (`writeMsgRecordLocked`/`writeAt`/`compactWithFloor`/`loadBlock`, ~2.5% combined) shows up only
  in the file-backed profile and is completely absent from the memory-backed one.
- It is **not** `(*client).flushOutbound` (the function already root-caused as plain NATS's own
  connection-scaling ceiling) - that function's *share* drops sharply under the durable-consumer
  mode (11.55%->3.74% of samples), because other costs grow around it, not because it gets worse.
- It is **not** `sync.RWMutex` lock contention hiding as blocked/waiting time invisible to
  `perf`'s on-CPU sampling - a real Go block-profile capture (`nats-server`'s own
  `prof_block_rate`, not a patched binary) found `RWMutex` accounts for only ~2.5% of total
  blocking weight; ~97% is benign idle-goroutine parking, not lock contention.
- It **is** genuinely new/growing work spread across JetStream's own tracking machinery -
  roughly 15% of the profile is symbols absent (or much smaller) in plain mode: `ackReplyInfo`
  (ack processing), `Sublist.match` (consumer re-matching), `ipQueue.push` (internal queueing),
  and - the single largest identified cluster, larger than any one named function - Go hash-map
  operations (`aeshashbody`/`maps.Iter.Next`/`mapaccess2_faststr`/`mapassign_faststr`, ~6%
  combined, with `mapdelete_faststr` right alongside them), consistent with per-message
  ack/redelivery state being tracked in a live map keyed by stream sequence number. This is a
  broad, diffuse tax across the durable-consumer protocol's bookkeeping, not one concentrated hot
  spot - there's no single obvious fix to claw the 2.6-2.9x back.

**Practical takeaway**: use `--queue-group` when raw throughput matters more than delivery
guarantees; use the durable-consumer flags when losing data is unacceptable and ~2.6-2.9x lower
throughput is an acceptable price for a real, verified loss-proof mode.

## Operational note: cycling a whole table through core-mode connections

A real use case motivating multiple input connections (see "Multiple input connections" above):
replacing ad-hoc application-side `SELECT`s against a Postgres table with a continuous periodic
flush - a DB-side driver republishes the table's rows (or just the changed subset) to per-category
NATS subjects, and one or more `nats_sidecar` instances apply real client-side content filters on
top, so clients get a live, continuously-updated, filtered view instead of hitting the database
directly. Tested end to end against a real 115M-row NYSE trade table
(`nyse_eqy_us_all_trade_20260102`, ~19 real exchange codes), publishing every row via `pgnats`'s
`nats_publish_from_sql.py` to `nyse.exchange.<code>` subjects, consumed by `nats_sidecar` through a
single `mode: core` connection subscribed to the wildcard `nyse.exchange.>` - one connection, no
per-exchange connection entries needed at all (see the `connections:` example above).

**A single core-mode instance cannot sustain a real full-table cycle.** `pgnats`'s single
unbatched, row-by-row publisher runs at ~130-480k rows/s (see below); one `nats_sidecar` instance,
even with an active content-based subscription doing real matching, sustains only ~24-25k
messages/s of actual delivered throughput. That mismatch overflows `nats-server`'s per-connection
buffer (`max_pending`, default 64MB) and it force-disconnects the sidecar with "Slow Consumer
Detected" - repeatedly, roughly every 10 seconds, for the entire run, never stabilizing. Since core
NATS pub/sub is at-most-once, every message published during a disconnect window is gone for good.
Measured against 2 full cycles (230M row-publish attempts): **~82% of everything published was
lost** (41.4M of 230M received), split further by a second, independent bottleneck -
`publish_max_inflight`'s output-side backpressure cap silently dropped ~40% of the messages that
*did* survive and match a filter, before they ever reached a subscriber.

**Raising `nats-server`'s `max_pending` (and `publish_max_inflight`) makes this worse, not
better.** With `max_pending` at 512MB (8x default), `pgnats` ran completely unthrottled at ~480k
rows/s (3.7x faster - the smaller buffer had accidentally been throttling the publisher via its
disconnect/reconnect churn) and blew through the bigger buffer even faster in wall-clock terms.
Worse, the connection didn't just hiccup this time - `nats_asio` logged `connection timeout: 2
pings without response` 90 seconds in and **never reconnected**, leaving the sidecar completely
dead until manually restarted. One Slow Consumer event instead of 175, but a permanent stall
instead of a self-healing (if lossy) trickle - a strictly worse failure mode.

**The real fix is parallel core-mode instances behind a shared queue group, not bigger buffers.**
Every instance uses the identical config (same `connections:` block, same `queue_group:` value) -
`nats-server` load-balances data deliveries one-message-to-one-instance across the group, while
every instance still independently receives the shared broadcast control-plane requests (see
"Multiple input connections"), so they all converge on the same matching tree regardless of which
one processed a given message. Measured with `worker_threads: 1` per instance (to keep total
thread count sane at higher N) and default `nats-server` limits, three clean, non-overlapping,
verified-uncontaminated full-table cycles:

| instances | cycle duration | throughput | delivered | Slow Consumer events |
|---:|---:|---:|---:|---:|
| 8  | 341s | 337,300 rows/s | 115,020,848 / 115,020,848 (100%) | 0 |
| 16 | 361s | 318,616 rows/s | 115,020,848 / 115,020,848 (100%) | 0 |
| 24 | 358s | 321,288 rows/s | 115,020,848 / 115,020,848 (100%) | 0 |

Zero loss, zero disconnects, at every N tested - not a partial mitigation, a complete fix.
Verified two independent ways each run: summing every instance's own `received` counter, and
cross-checking against `nats-server`'s own `out_msgs` varz counter (matched to within ~0.0002%,
the residual being control-plane/lease traffic, not lost data).

**Throughput stops improving past N=8** (337k/319k/321k rows/s is flat within noise). Once there
are enough parallel consumers to keep any single connection's backlog small enough to never trip
Slow Consumer, the ceiling moves entirely to the *publisher* - `pgnats`'s single unbatched,
one-Postgres-connection, row-by-row publish loop.

**Parallelizing the publisher alone reintroduces the exact same problem - consumer and publisher
parallelism have to scale together, not independently.** Fixing the consumer side at N=8 (the
setup that gave zero loss above) and increasing `nats_publish_from_sql.py --workers` instead
(still one-row-per-message - `--batch-size` is a separate, *incompatible* lever, see below):

| `--workers` | publish rate | delivered | lost | Slow Consumer events |
|---:|---:|---:|---:|---:|
| 4  | 299,533 rows/s | 99.6% | 0.42% | 2 |
| 8  | 340,298 rows/s | 87.4% | 12.62% | 65 |
| 16 | 458,250 rows/s | 60.3% | 39.72% | 178 |

N=8 consumers happened to sit almost exactly at the ceiling of one *unparallelized* publisher
connection (~320-340k rows/s) - that's why it looked like a clean fix. The moment the publisher
gets parallelized past that, the same Slow-Consumer-disconnect mechanism reappears and scales
right back up with publish rate (2 -> 65 -> 178 events), because the *consumer* side was never
scaled to match.

**Co-scaling both sides closes most, but not all, of the gap - and finds a real, hardware-bounded
ceiling.** Holding `--workers 8` (a burstier ~340-363k rows/s, from 8 concurrent publish
connections instead of 1) and raising the consumer count to match:

| `--workers` | instances | publish rate | delivered | lost | Slow Consumer events |
|---:|---:|---:|---:|---:|---:|
| 8  | 8  | 340,298 rows/s | 87.4%  | 12.62% | 65 |
| 8  | 16 | 362,842 rows/s | 99.17% | 0.83%  | 4  |
| 8  | 24 | 362,842 rows/s | 99.81% | 0.19%  | 1  |
| 16 | 24 | 391,227 rows/s | 91.37% | 8.63%  | 44 |

Tripling the consumer count (8->24) for the same `--workers 8` load gets loss down from 12.6% to
0.19% - real, substantial improvement, but not a clean zero even at 3x, because N concurrent
publish connections create more simultaneous burst pressure than a linear "N consumers per
publish worker" model would predict. Pushing the publisher further (`--workers 16`, ~391k rows/s)
overwhelms even N=24 consumers again (8.63% loss) - this host has 24 logical/12 physical CPUs, and
N=24 core-mode instances (`worker_threads: 1` each) already uses all of it; going higher would
mean real oversubscription, not just more config.

**Practical, hardware-bounded conclusion for this box**: the highest *reliable* (not just fast)
full-table-cycling throughput found is ~360k rows/s, using `--workers 8` paired with N=24
core-mode sidecar instances (~0.2% residual loss). Going faster than that on this hardware would
need either more CPU cores (to add consumer instances without oversubscribing) or `nats_sidecar`
gaining columnar-batch matching support so `--batch-size` becomes usable - not more tuning of the
current knobs.

`nats_publish_from_sql.py --batch-size` (opt-in row batching, collapses N rows into one columnar
message per subject: `{"col1":[v,v,...],"col2":[v,v,...]}` instead of one row-object message per
row) was deliberately **not** tested against this sidecar setup - it changes the wire format in a
way `nats_sidecar`'s current per-message scalar-attribute matching can't interpret as multiple
candidate rows (it would need columnar-unbatching support added to `worker_pool`/the matching
path first, not built as part of this investigation).

**Practical takeaway for a periodic full-table/snapshot flush**: run N≈8 core-mode instances
sharing one `queue_group`, leave `nats-server`'s `max_pending` and the sidecar's
`publish_max_inflight` at their defaults, and don't reach for bigger buffers as a fix - they mask
the real throughput mismatch and can turn a lossy-but-self-healing failure into a permanent one.

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

The format defaults to `msgpack` if `-f` is not specified. Supported formats: `msgpack`, `cbor`, `flexbuffers`, `zera`, `ion`, `bson`, `beve`.

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
- Worker threads process messages in parallel using lock-free RCU snapshots of the matching engine (a-tree or be-tree)
- NATS publishes are posted back to the ASIO thread via `co_spawn`

## License

See LICENSE file.
