# nats_sidecar

Content-based filtering sidecar for NATS. Receives binary-encoded messages on a NATS subject, evaluates them against boolean expression subscriptions using [a-tree](https://github.com/mrayva/a-tree) or [be-tree](https://github.com/mrayva/be-tree) (selectable per deployment, see `--engine` below), and fans out matching messages to per-subscription output topics.

## Features

- Boolean expression subscriptions (e.g. `temperature > 30.0 AND location = "warehouse"`)
- Two selectable matching engines (a-tree, be-tree) sharing the same expression syntax
- Supports MessagePack, CBOR, FlexBuffers, Zera, Ion, BSON, and BEVE binary formats, plus read-only Apache Arrow columnar input (see "Arrow columnar input" below)
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
- `libarrow-dev` (system package, found via pkg-config - not a vcpkg dependency; see "Arrow columnar input" below). On Ubuntu: `sudo apt-get install libarrow-dev`.

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
| `-f, --format FMT` | Binary format (`msgpack`, `cbor`, `flexbuffers`, `zera`, `ion`, `bson`, `beve`, `arrow`) |
| `--output-format FMT` | Republish format for columnar batches, if different from `--format` (see "Arrow columnar input") |
| `--engine ENGINE` | Matching engine (`atree`, `betree`, `pstree`); defaults to `atree` |
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
| `--list-subscriptions-subject SUBJ` | On-demand subscription-listing request subject |
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
| `--stats-format FORMAT` | Stats log format: `text` (default), `json`, or `both` - see "Stats output" below |
| `--log-level LEVEL` | Log level (`debug`, `info`, `warn`, `error`) |
| `--generate-schema PATH` | Infer attributes from a sample binary file and print YAML |
| `-v, --verbose` | Enable debug logging (shorthand for `--log-level debug`) |
| `-h, --help` | Print help |

### Stats output

By default (`stats_format: text`), the periodic stats line every `stats_interval_seconds` is a
single spdlog-formatted line, e.g.:

```
stats: received=230276 processed=230276 matched=5514544 published=5514544 match_failures=0
publish_failures=0 input_dropped=0 publish_tasks_dropped=0 subscriptions=1 queue_depth=0
queue_bytes=0 publish_inflight=0 avg_fanout_us=0.05 avg_match_us=0.58
```

Set `stats_format: json` (or `--stats-format json`) to instead emit the same fields as a single
JSON object, under a `stats_json:` prefix (never `stats:`, so it can't collide with any tooling
already `grep`-ing for the text line):

```
stats_json: {"avg_fanout_us":0.05,"avg_match_us":0.58,"input_dropped":0,"match_failures":0,
"matched":5514544,"processed":230276,"publish_failures":0,"publish_inflight":0,
"publish_tasks_dropped":0,"published":5514544,"queue_bytes":0,"queue_depth":0,
"received":230276,"subscriptions":1}
```

`stats_format: both` emits both lines every interval. An unrecognized value silently falls back to
`text`, matching `log_level`'s own convention for an unrecognized level (main.cpp's
`set_log_level()`) rather than rejecting the config at startup. This exists specifically so
external tooling can parse a real object instead of regex-scraping the text line, the way every
benchmark script under `nyse-matrix/` in this project's own history has had to.

Besides the periodic log line, the same JSON is available on demand: publish an empty NATS
request to `stats_request_subject` (default `sidecar.stats`, configurable the same way as
`subscribe_subject`/`unsubscribe_subject`) and the reply is exactly what `stats_json:` would have
logged at that instant - useful for polling stats from an external tool without scraping logs or
waiting for the next `stats_interval_seconds` tick.

`avg_fanout_us` is wall-clock time resolving output subjects and estimating publish size for a row
that matched (`worker_pool.cpp`, between `matching_engine::search()` returning and the publish
coroutine being handed off) - only rows that actually reach the publish coroutine count, and
deliberately *not* the coroutine itself (frame serialization is real CPU, but it's interleaved with
`co_await write_raw()`/backpressure waits - timing the whole thing would mix real work with network
I/O wait time in one number). Deserialize/populate (before matching), matching itself, and the
actual NATS write (after fan-out resolution) all stay unmeasured by this stat.

`avg_match_us` times `matching_engine::search()` alone, one row at a time. A previous version of
this stat was removed: a real `perf` profile found its `clock_gettime` + 1-in-8-row sampling design
~100-140x off from ground truth, with the sampling's own `clock_gettime` calls costing ~7.6% of
real CPU even at that reduced rate - both inaccurate and expensive, so it was cut rather than kept
disabled (`perf` was the trusted way to measure matching cost in the meantime). It's back
(`src/match_timing.hpp`), rebuilt on an RDTSC cycle-counter read instead of `clock_gettime`, cheap
enough to time *every* row rather than sample: a thread_local pair of counters accumulates cycles
around each `tree.search()` call and worker_pool.cpp drains them once per
`deserialize_and_match*()` call (no per-call output parameter threaded through those functions -
`worker_loop()` runs each worker on one fixed thread, so accumulate-then-drain-on-that-same-thread
is safe). Cycles are converted to microseconds only at report time, via a cycles-per-microsecond
ratio calibrated once at process startup (RDTSC delta vs. `steady_clock` delta over a 20ms
busy-wait).

**Accuracy verification** (this exact concern is why the old stat was removed, so it wasn't
trusted without checking): a full-pipeline cross-check via `perf`-sampling
`sidecar_pipeline_bench` initially showed a confusing ~2.6x gap between the reported `avg_match_us`
and a perf-derived estimate - traced to sampling/attribution noise from profiling a deeply
recursive call path (`PSTDynamic::matchEvent`) mixed with unrelated fan-out work in the same
process, not a flaw in the timing mechanism. A cleaner, non-sampled ground truth settled it:
`benchmarks/perf_search_loop.cpp` (extended to report this) calls `search()` in a tight loop for a
fixed wall-clock duration with nothing else running, so wall-clock-time-elapsed / iteration-count
is an independent, sampling-free average with no `perf`/dwarf skid to second-guess. Across 4 runs,
the RDTSC-based `avg_match_us` matched that ground truth to within 0.6-0.7% (ratio 0.993-0.994) -
see its own `ground truth: wall_clock_avg_us=... rdtsc_avg_us=... ratio=...` output line.
**Overhead** was checked the same way the old stat's ~7.6% cost was found: a `perf` profile at a
high-match-rate config never showed `match_message()`'s own self-time (which is where the two
`read_cycles()` calls live) high enough to appear even in the top 25 hottest symbols - consistent
with RDTSC's few-cycles-per-call cost and nowhere near the old design's overhead.

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
format: msgpack          # msgpack | cbor | flexbuffers | zera | ion | bson | beve | arrow
# output_format: msgpack  # optional; required (and must differ from `format`) when format: arrow
                           # - see "Arrow columnar input" below. Unset means "same as format".
engine: atree             # atree | betree | pstree

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

# Subscription-id registry (NATS KV) - the authoritative expression -> id
# mapping shared by every instance in a fleet. Entries are permanent (no
# TTL), so there's no check-interval field to go with it.
registry_bucket: "sidecar-subscriptions"

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
# Max bytes buffered before flushing a partial publish write - bounds peak
# PER-TASK memory to this regardless of how many subscriptions a message's
# rows collectively match (a wide-range predicate shared by many
# subscriptions, or large columnar batches, can otherwise push one task's
# combined buffer into the hundreds of MB without this cap).
publish_chunk_bytes: 4194304
# Max AGGREGATE bytes reserved across every currently in-flight publish task
# at once - publish_max_inflight alone only bounds task *count*, and
# publish_chunk_bytes alone only bounds one task's own buffer, so without
# this, up to publish_max_inflight * publish_chunk_bytes could still
# accumulate under sustained high match fan-out.
publish_max_inflight_bytes: 67108864
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
    columnar: true                          # optional, default false - see "Columnar batch input"

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

### Columnar batch input

Setting `columnar: true` on a connection (or the legacy flat `input_columnar: true` /
`--input-columnar` for a single-connection config) tells that connection's messages are
pg_zerialize-style columnar batches - `{"col1":[v,v,...],"col2":[v,v,...]}`, N rows collapsed into
one message, exactly the shape `nats_publish_from_sql.py --batch-size N --batch-encoding native`
produces - instead of one scalar-attribute row per message. The sidecar unpacks each batch into
its N rows and matches every row independently: a matching row is published as its own standalone
message (the same shape a non-batched connection would have sent), to whichever subscriptions that
*specific row* matched - not the whole batch republished verbatim. Orthogonal to `mode` (js/core);
a batch's JetStream message (if js-mode) is acked once, after every row's matches have been
published, regardless of how many rows matched.

Supported for 6 of the 7 `format` values: msgpack, cbor, flexbuffers, zera, ion, beve. **Not
supported for `format: bson`** (rejected at startup with a clear error) - unpacking works by
materializing a root-level array internally, and BSON's wire format cannot round-trip a root-level
array (a document and an array are byte-identical on the wire; only a *parent* element's header
records which one a value is, and the root has no parent).

Two things worth knowing before enabling it:
- A payload that doesn't actually match the columnar contract (every field an array, all the same
  length) is rejected as malformed - same handling as any other unparseable message (`term()` in
  js-mode, dropped in core-mode). One narrow case this can't distinguish: a normal, non-batched
  message whose *entire* schema happens to be `string_list`/`integer_list`-typed (already
  legitimately array-shaped) sent by mistake to a columnar connection would be silently
  misinterpreted as a batch rather than rejected - a misconfiguration scenario, not a normal
  operational path, and not specially guarded against.
- `publish_max_inflight` now bounds one *batch* in flight, not one *message* - a single credit can
  cover up to (rows matched) x (subscriptions per row) outbound frames instead of just
  (subscriptions per row), so size it accordingly for a columnar connection carrying large batches.

### Arrow columnar input

`format: arrow` consumes [Apache Arrow](https://arrow.apache.org/) IPC stream batches - the exact
bytes produced by the [`pg_arrow`](https://github.com/mrayva/pg_arrow) Postgres extension's
`rows_to_arrow(anyarray) -> bytea`, one `RecordBatch` per message. It's a distinct code path from
the other 7 formats (`src/arrow_columnar_rows.hpp`), not routed through `zerialize`, since Arrow's
columnar arrays support true random access rather than needing zerialize's own sequential-decode
machinery.

Arrow input has two hard constraints, both enforced at startup with a clear error:

- **Columnar-only.** Every connection must set `columnar: true` when `format: arrow` - there is no
  row-mode Arrow reader (a one-row `RecordBatch` is nearly all fixed overhead, the same reason
  `pg_arrow` itself has no `row_to_arrow`).
- **Read-only, so `output_format` is required.** Arrow has no practical single-row encoder, so
  matched rows can't be republished as Arrow - `output_format` must be set to a *different*
  non-arrow format (e.g. `output_format: msgpack`) telling the sidecar what to re-encode matches
  as. `output_format: arrow` is rejected.

For every other (non-arrow) `format`, `output_format` - if set at all - must equal `format`;
cross-format translation among the 6 non-arrow formats is not yet supported (`output_format` exists
as a general, format-agnostic mechanism for this reason, but only Arrow's asymmetry requires it
today). Like `format` itself, `output_format` is process-wide, not per-connection.

**Type mapping** (Arrow -> `attribute_type`):

| Arrow type | `attribute_type` | Notes |
|---|---|---|
| `int16` / `int32` / `int64` | `integer` | |
| `uint8` / `uint16` / `uint32` / `uint64` | `integer` | `pg_arrow`'s own encoding of `bit(8)/bit(16)/bit(32)/bit(64)` - rides entirely on the existing `integer` path, no engine restriction. `uint8/16/32`'s full range always fits `int64_t`; `uint64` values from 2^63 to 2^64-1 (only reachable via `bit(64)`) throw a clear error at match time rather than silently wrapping negative |
| `float32` / `float64` / `half_float` | `float` | `half_float` (16-bit) widens losslessly into `double` - every value is exactly representable, unlike the decimal cases below |
| `boolean` | `boolean` | |
| `utf8` | `string` | zero-copy view into Arrow's own buffer |
| `binary` | `string` | raw bytes reinterpreted as a string, **not** base64-tagged (unlike `pg_arrow`'s own `arrow_to_jsonb`) - `attribute_type` has no blob kind |
| `decimal32(p,s)` / `decimal64(p,s)` / `decimal128(p,s)` / `decimal256(p,s)` | `decimal` | **pstree-only** - native `pstree::Int256` (four `uint64_t` limbs), see below |
| `date32`, `timestamp` | **unsupported** | rejected at read time (clear error naming the column) - `attribute_type` has no timestamp kind, and silently mapping to `integer` risks a silently-wrong threshold comparison |

### Decimal: native support, pstree-only

All four Arrow decimal widths map to `attribute_type::decimal`, backed by a real `pstree::Int256`
(four `uint64_t` limbs, two's-complement) - not a lossy `double`, and not the earlier `string`
stopgap this project shipped before a native type existed. `a-tree`/`be-tree` have no decimal
representation at all and reject a `decimal`-typed attribute at schema-construction time
(`matching_engine_error`) - this is `pstree`-only, the mirror image of `string_list`/`integer_list`
being atree/betree-only (not pstree).

**Scale is declared once per attribute, never per value**: `decimal_scale` is required in the
attribute's own config (`decimal_scale: 2` in YAML, or a config-file-declared attribute - not
currently settable via `--attr`). Every Arrow decimal value reaching that attribute - regardless of
which of the four widths its own source column actually is - gets widened to `arrow::Decimal256`
(via its own explicit widening constructors from 128/64/32) and `Rescale()`d to that ONE canonical
scale before it ever becomes a `pstree::Value`. If two source columns feeding the same logical
attribute declare different scales, pick one canonical scale wide enough for both.

**Event values are exact; subscription literal thresholds are not, by design.** `be-tree`'s own
reused parser (`pstree_dialect.cpp`, reused specifically because it's already-tested - see that
file's own doc comment) has no decimal literal kind at all: a subscription's own numeric literal
(e.g. the `100.50` in `amount > 100.50`) is parsed as an ordinary `int64`/`double`, indistinguishable
from a real integer/float attribute's own literal, and gets *promoted* to the target attribute's
canonical-scale `Int256` from that already-double-rounded value. This means the EVENT side is exact
even at 76 significant digits (`decimal256`'s own ceiling), but the QUERY side is capped at
IEEE-754 double precision (~15-17 significant digits) - the same cap every other numeric attribute
type in this system already lives with, not a new decimal-specific regression. Getting an exact
literal through would mean extending `be-tree`'s own lexer/grammar to carry a real decimal token -
real, bounded, but cross-repo (the user's own diverged `be-tree` fork) and deliberately out of scope
for now.

**`in`/`not in` is not supported against a decimal attribute at all** - confirmed empirically, not
assumed: decimal attributes are declared as an ordinary `float` in the parsing-only `be-tree`
instance `pstree_matching_engine` uses (`build_betree_for_pstree_parsing`), and `be-tree`'s own
semantic binding already rejects `in`/`not in` against ANY float-typed attribute in this whole
project (`BETREE_FLOAT` never matches an integer literal list) - a real, pre-existing `be-tree`
limitation decimal simply inherits, not something new this support introduced.

No Arrow list/struct/nested type ever appears in `pg_arrow`'s own output (rejected at the SQL
layer), so `string_list`/`integer_list` attributes are simply unreachable via Arrow input.

Example:

```yaml
connections:
  - name: prices
    mode: core
    subjects: ["prices.arrow"]
    columnar: true

format: arrow
output_format: msgpack
attributes:
  - name: symbol
    type: string
  - name: price
    type: float
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

### Expression Syntax

All three engines accept the same expression syntax verbatim: `=`, `<>`, `<`, `<=`, `>`, `>=`, `and`, `or`, `not`, `in`, `not in`, `one of`, `none of`, `all of`, `is null`, `is not null`, `is empty`. Multi-word operators are space-separated (`not in`, not `not_in`) - this matches every engine's actual grammar (`pstree`'s own expressions are parsed by be-tree's own parser - see below - so it inherits be-tree's grammar exactly), confirmed against their lexers directly.

One operator is engine-specific: **`is not empty` is only available under `engine: atree`**. be-tree's grammar has no rule for it at all; subscribing with it under `engine: betree` (or `engine: pstree`, which parses via be-tree's own parser) is rejected at subscribe time with a clear error rather than silently misbehaving. There's no substitute expression to fall back to if you need this on be-tree/pstree.

`in` / `not in` list literals accept integer or string values only, not floats. `one of` / `none of` / `all of` apply to list-typed attributes (`string_list` / `integer_list`); `is empty` / `is not empty` likewise.

`all of` means "the attribute contains every listed value" on both a-tree and be-tree - but only because it's made to: a-tree's own native `all of` actually checks the opposite (the attribute is a subset of the literal list), so `matching_engine` transparently rewrites `X all of (v1, ..., vn)` into `X one of (v1) and ... and X one of (vn)` before handing it to a-tree, which evaluates to the same (be-tree-matching) result. This is invisible in normal use - the rewrite happens automatically inside `insert()` - but worth knowing if you're reading a-tree's own docs/tests, which describe `all of` differently.

### `engine: pstree`

A third matching engine (`src/matching_engine.cpp`'s `pstree_matching_engine`), built on
[mrayva/pstree](https://github.com/mrayva/pstree) - a from-scratch implementation of PS-Tree/PSTDynamic,
the boolean-expression matching design from ["Efficient Parallel Boolean Expression Matching"](https://doi.org/10.1145/3736756)
(Ji, Yao, Wang, Wei, Jacobsen — ACM TODS 2025). Expressions are parsed by reusing be-tree's own
parser (`betree_make_sub()`, against a throwaway, never-searched be-tree instance built purely to
parse and type-check) rather than a second hand-written parser - see `src/pstree_dialect.hpp`'s own
doc comment for the full reasoning. The resulting AST is converted to Disjunctive Normal Form
(De Morgan negation-pushdown for `not`, clause expansion for `or`) since PSTDynamic's own model is a
pure conjunction of predicates with no AND/OR/NOT combinators at all; each DNF clause becomes its
own PSTDynamic subscription, and `pstree_matching_engine::search()` deduplicates a subscription
matched via more than one of its own OR'd clauses back down to a single result.

Two real, structural limitations (not omissions - PSTDynamic's own predicate-space-index design has
no way to represent either):

- **No list-valued attributes** (`string_list` / `integer_list`) - an event attribute in
  PSTDynamic's model is always a single value, never a list. Referencing one (`one of`/`none of`/
  `all of`/`is empty`/`is not empty`, or any comparison against a list-typed attribute) is rejected
  at subscribe time with a clear error.
- **`X is null` can't be indexed if it's the subscription's only usable predicate.** PSTDynamic
  picks one predicate per subscription (the "access predicate") to index into a per-dimension tree;
  `is null` can never be chosen for that role, since "this attribute was absent" has no
  representable position in a value-ordered index (`MatchEvent` only ever consults a dimension's
  tree for events that *do* have that attribute). A subscription with at least one other,
  indexable predicate alongside `is null` works fine; `"X is null"` alone (or every branch of an
  `or` reducing to bare `is null` checks) is rejected at subscribe time. `is not null` has no such
  problem - it's always indexable (as an unselective "matches every leaf" fallback).
- String attributes are compared only up to a fixed prefix length (`kPstreeStringMaxLen`, 32 bytes,
  in `matching_engine.cpp`) - PS-Tree's string encoding is a fixed-depth tree, one level per byte
  position, not a variable-length comparison. Two distinct strings sharing a 32-byte prefix are
  indistinguishable to `pstree`; unlikely to matter for realistic attribute values (names, symbols,
  categories, ids) but worth knowing if an attribute can hold long strings - raise the constant
  (cost scales linearly with it, not exponentially) if one genuinely needs to. Kept deliberately
  small rather than defaulting to something very generous: PSTree::matchPoint() walks this many
  tree levels on every lookup regardless of the actual string's length, real, measured overhead
  for a real workload - see matching_engine.cpp's own comment for the numbers.

Independent throughput comparison against a-tree/be-tree (the paper's own self-reported benchmarks
are not otherwise verified here) is future work - see the project status notes for the current
state of that investigation.

## Client Protocol

### Subscribe

Send a JSON request to the `subscribe_subject` (request/reply):

```json
{"expression": "temperature > 30.0 AND location = \"warehouse\"", "client_id": "my-client"}
```

There is no client-supplied id in this request - the subscription id is always assigned by the
shared `registry_bucket` (a NATS JetStream KV bucket every instance talks to), which is what lets
a broadcast subscribe request - published once to a control subject that N instances behind a
shared data-plane `queue_group` all independently receive - converge on the identical id/output
topic without any coordination between clients or instances. The mechanism: the first instance
(anywhere, at any time) to see a given expression does an atomic `kv_create` on the registry; the
KV revision NATS itself assigns on that create *is* the subscription id, so there's no separate
counter to keep in sync. Every other instance/request for the same expression reads back that same
revision via `kv_get`. Registry entries are never deleted, so an expression's id/output topic is
stable for the life of the registry bucket, not just for the life of one subscription - a
resubscribe to a previously fully-unsubscribed expression gets back its original id, not a new one.
An earlier revision of this design let clients supply their own id to achieve the same convergence;
that turned out to have two real bugs (a legitimate collision between two clients' independently-
chosen ids hard-failed instead of merging, and a first-registration race could leave instances
permanently disagreeing) which this registry-based design eliminates by construction.

(This design adds one or two extra synchronous NATS round trips - `kv_create`, sometimes followed
by a `kv_get` - on the first-ever-subscribe path, before `persist_lease`'s own round trip. The
previous design's measured control-plane latency numbers no longer apply and haven't yet been
re-benchmarked under this design.)

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

The sidecar creates both the lease bucket and the `registry_bucket` when absent, and validates
their settings when they already exist (TTL/history for leases, history for the registry - which
has no TTL of its own). A configuration mismatch fails startup. Lease records contain enough
metadata to restore active subscriptions with their original IDs after a sidecar restart.

### Unsubscribe

Send a JSON request to the `unsubscribe_subject`:

```json
{"id": 1, "client_id": "my-client"}
```

Response:

```json
{"id": 1, "removed": true}
```

`removed` is `true` if the subscription was fully removed (no remaining lease holders), `false` if other clients still hold leases. Like subscribe, this is safe to repeat/retry: unsubscribing a lease that's already gone (a double-unsubscribe, a retried request after a network hiccup, or one that already expired naturally via TTL) still replies `{"removed": true}` rather than an error - the caller's actual intent is already satisfied either way.

### Grace period / dead-client cleanup

There's no explicit "client disconnected" signal in this design - a client's only renewal action is
calling `subscribe` again for the same expression/`client_id` (cheap: `subscription_manager::
subscribe()`'s "reused expression" branch is just a lease-holder-set insert, no tree mutation). A
client that stops renewing - crashes, forgets to unsubscribe, network partition - has its
subscription torn down somewhere between `lease_ttl_seconds` (time since its last renewal) and
`lease_ttl_seconds + lease_check_interval_seconds` later (worst case, if it just missed a sweep of
`lease_manager`'s own `cleanup_loop()`/`reconcile_once()`). That window is this sidecar's grace
period for a gone client, and it's a pure config tradeoff via `config/example.yaml`'s
`lease_ttl_seconds`/`lease_check_interval_seconds` (defaults 3600s/60s - see that file's own comment
for a "responsive" example profile, ~15-20s worst-case, trading off against needing clients to
re-subscribe roughly that often to stay alive).

A transport-level alternative - reacting directly to the client's NATS connection dropping instead
of an absent renewal - was investigated and deliberately not pursued: `nats_asio` doesn't expose a
connection's identity to application code at all (no CID, no settable client-name field), and this
sidecar's `client_id` is a pure application-level string with no relationship to any actual NATS
connection today. Making that work would mean building a new client-side convention (a client's
NATS connection name equal to its `client_id`), extending `nats_asio` to support it, standing up a
second, `$SYS`-account-scoped sidecar connection with elevated credentials to receive
`$SYS.ACCOUNT.<acct>.DISCONNECT` events, and accepting a documented NATS clustering caveat
(nats-server#3177) where those events don't reliably reach every fleet node - real cross-repo work
for a signal that's still only as trustworthy as client cooperation. The existing TTL/reconcile
mechanism achieves the same practical goal (bounded, tunable dead-client cleanup) with zero new
code, so that's what this project uses.

One thing that makes a short `lease_check_interval_seconds` cheap even under real subscription
churn: every lease `reconcile_once()` finds expired now goes through the same incremental
`matching_engine::remove()` path (pstree) the "High-churn-under-load" section above measures - a
tight sweep interval no longer means paying an O(K) tree rebuild per expiry the way it would have
before that fix.

### List subscriptions

The only other control-plane gap `stats_request_subject` doesn't cover: `active_count()` is a
count, not a list, and every other subscription lookup (by id, by expression) requires the caller
to already know the key. Send an (entirely optional) JSON request to the
`list_subscriptions_subject`:

```json
{"client_id": "my-client", "offset": 0, "limit": 500}
```

All three fields are optional - an empty request body lists every active subscription, first page.
- No `client_id`: the "what's the server actually doing" admin use case - every active
  subscription.
- `client_id` present: narrows to only subscriptions that client currently leases - a lower-risk,
  self-service use case (a client reconciling its own held subscriptions after a restart, without
  needing to remember every id itself).
- `offset`/`limit` paginate the (optionally filtered) result set. `limit` defaults to 500, capped
  at 5000 server-side regardless of what's requested, to bound worst-case reply size against
  NATS's own max-payload limit without the caller needing to know that constraint exists.

Response:

```json
{
  "subscriptions": [
    {"id": 1, "expression": "temperature > 30.0 AND location = \"warehouse\"", "lease_holder_count": 2}
  ],
  "total_matching": 1,
  "offset": 0,
  "returned": 1
}
```

`total_matching` is the filtered-but-unpaginated count, so a caller knows whether more pages exist
(request the next page with `offset` = this response's own `offset` + `returned`); `returned` is
just `subscriptions.length` in this specific reply.

Deliberately **not** included: the raw list of client ids holding each lease - only a count.
Exposing individual client identifiers to any caller that can reach this NATS subject would be a
new category of information disclosure this control plane doesn't have anywhere else today
(`subscribe`/`unsubscribe` only ever return information about the *requesting* client's own
state); a count is enough to answer "what's actually subscribed right now" without introducing
that. Every instance in a fleet answers identically for the *unfiltered* case (`subscribe_subject`/
`unsubscribe_subject` are broadcast, not queue-grouped - see the Subscribe section above - so
every instance's live subscription state converges to the same set), so a plain `request()` call
capturing whichever instance replies first is a valid fleet-wide answer, not just that one
instance's own view.

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
need either more CPU cores (to add consumer instances without oversubscribing), or using
`--batch-size` now that `nats_sidecar` supports columnar-batch matching (see "Columnar batch
input" below) to cut the per-row publish overhead this whole investigation was bottlenecked on -
not more tuning of the current knobs.

**Fixing the consumer count at N=12 (this host's physical core count) and sweeping `--workers`
down instead pinpoints the boundary precisely**, rather than just bracketing it from above:

| `--workers` | instances | publish rate | delivered | lost | Slow Consumer events |
|---:|---:|---:|---:|---:|---:|
| 8 | 12 | 349,607 rows/s | 98.96%  | 1.04%  | 5 |
| 6 | 12 | 345,408 rows/s | 100.00% | 0.00%  | 0 |
| 4 | 12 | 340,298 rows/s | 100.00% | ~0.00% | 0 |

N=12 is exactly-loss-free through `--workers 6` (~345k rows/s) and tips into real, measurable loss
at `--workers 8` (~350k rows/s, 5 disconnects) - a sharp, real threshold, not a gradual one, and
notably right where the *publish rate itself* barely changed (345k -> 350k) but the *connection
count* did (6 -> 8 concurrent publishers) - reinforcing that burst concurrency, not just average
throughput, is what actually trips Slow Consumer.

`nats_publish_from_sql.py --batch-size` (opt-in row batching, collapses N rows into one columnar
message per subject: `{"col1":[v,v,...],"col2":[v,v,...]}` instead of one row-object message per
row) was not tested against this sidecar setup as part of *this* investigation, but is now a
supported input mode - see "Columnar batch input" below - not left as a follow-up.

**Practical takeaway for a periodic full-table/snapshot flush**: N=12 core-mode instances sharing
one `queue_group`, paired with `--workers` in the 4-6 range, is the cleanest reliable
(zero-measured-loss) setup found on this host - already ~14x this session's original single-
instance ceiling (~24-25k msgs/s). Push instances toward N=24 with `--workers 8` for more raw
throughput (~360k rows/s) at a small, real, non-zero loss cost. Either way, leave `nats-server`'s
`max_pending` and the sidecar's `publish_max_inflight` at their defaults, and don't reach for
bigger buffers as a fix - they mask the real throughput mismatch and can turn a lossy-but-self-
healing failure into a permanent one.

**Follow-up with columnar batching: the bottleneck moves from NATS transport to per-instance
processing capacity.** Same N=12 core-mode fleet (`worker_threads: 1` each, default `nats-server`
limits), now with every connection's `columnar: true` and `pgnats` publishing via
`--batch-size 500 --batch-encoding native` instead of one row per message. Deliberately pushed hard
(`--workers 24`, aggressive by design - the point was to find the new ceiling, not confirm the old
one) against the same real 115,020,848-row NYSE table, one full cycle:

| metric | value |
|---|---:|
| rows published | 115,020,848 |
| batches published (500 rows/batch) | 230,264 |
| publish-side rate | 9.13M rows/s aggregate (24 workers, 12.6s) |
| batches delivered by NATS to the fleet | 230,264 / 230,264 (100.00%) |
| Slow Consumer events | 0 |
| batches actually processed (unpacked + matched + published) | 124,157 (53.9%) |
| batches dropped (local input queue full) | 106,107 (46.1%) |
| rows matched (`price > 500.0`) and republished individually | 3,212,038 |

**NATS transport handled the burst perfectly** - every single batch reached some instance's
connection, zero Slow Consumer disconnects, at a publish rate (9.13M rows/s) that would have
been wildly impossible in row-mode (the old ceiling was ~360k rows/s before Slow Consumer started
triggering). Batching one NATS message per 500 rows really does eliminate the *old* bottleneck
(per-message transport/protocol overhead) as completely as hoped.

**But a new, different bottleneck appeared in its place: each instance's own bounded input queue
(`input_queue_max_messages`, default 10000) filling up faster than its single worker thread could
unpack-and-match batches.** Loss here happens at `worker_pool::enqueue_impl()`'s local queue-full
check, not a NATS-level disconnect - `received` (what actually arrived) and `processed +
input_dropped` (what happened to it) reconcile exactly (230,264 = 124,157 + 106,107), confirming
the drop is a clean, well-understood local capacity limit, not a mystery loss. Measuring the
drain-only phase (after publishing stopped, while the fleet worked through its backlog: 50,838
queued batches processed in 96s) gives a real **sustained processing ceiling of ~265k rows/s
aggregate for N=12 single-threaded instances** - each instance unpacking, matching, and
re-publishing roughly one 500-row batch every ~23ms.

**`perf`-profiled the single-threaded per-instance processing cost directly, rather than assume
where it went** - and found the "~265k rows/s ceiling" above was itself measuring a real, fixable
bug, not a fundamental cost:

**Found: 58% of all sampled CPU time was going into `event_bridge.cpp`'s own row loop indexing
`rows[i]` into the just-`expand_columnar`'d row array**, one row at a time
(`zerialize::MsgPackDeserializer::operator[](size_t)` -> `zerialize::mp_skip`, recursively - a
linear skip-from-the-start per call, confirmed directly in the call-graph). This is the exact same
O(n^2) antipattern already fixed on the *producing* side inside zerialize's
`write_expanded_columnar()` earlier in this session (see "Columnar batch input" above and
zerialize commit `2417547`) - it simply wasn't obvious that *consuming* an already-expanded row
array by index, in nats_sidecar's own new orchestration code, pays the identical cost. The real
matching-engine work (`atree::Tree::search()`, ~20% of the *original* profile) and event
construction (`populate_event()`, ~12%) were comparatively small next to this one indexing bug.

**Fixed** by walking the expanded row array via its `.elements()` single-pass sequential iterator
(the same primitive/fallback idiom used for the zerialize-side fix) instead of indexing `rows[i]`
- a small, contained change to `event_bridge.cpp`'s `match_columnar_batch()`, no zerialize changes
needed this time. Verified with a clean, controlled before/after measurement (single instance, a
real ~34.5M-row backlog built up faster than one thread could drain it, steady-state batches/sec
measured over multiple 10-second windows, immediately before vs. after rebuilding with the fix):

| | batches/s (single instance) | rows/s equivalent |
|---|---:|---:|
| before fix | ~181 | ~90,500 |
| after fix | ~1,330 | ~665,000 |

**A real, measured 7.3x single-instance speedup** from one line. A follow-up `perf` capture after
the fix confirms `mp_skip`/row-indexing from *this* bug is gone from the hot path. Re-examining
that post-fix profile in full (categorized by subsystem, not just top-line symbols) - prompted by a
direct challenge to justify "genuinely necessary work" rather than take it at face value - found the
remaining cost is **not** dominated by msgpack row-unpacking at all:

| category | share of CPU time |
|---|---:|
| a-tree's own per-row event construction (Rust `HashMap<String,...>` alloc+rehash, SipHash key hashing, `rust_decimal` compares, FFI panic-guard) | 53.7% |
| msgpack row-unpacking (`mp_skip`, `memmove`, buffer writes) | 20.5% |
| this codebase's own per-row stats timing (`clock_gettime`, called twice per row) | 2.5% |
| this codebase's own orchestration (`populate_event`, `match_columnar_batch`, schema lookup) | 5.0% |
| the actual matching algorithm (`ATree::search`, `process_predicates`) | ~2.4% |

**That 53.7% figure turned out to itself be an artifact, caught by trying to act on it.** Attempting
to actually fix it (see below) required checking how `nats_sidecar`'s `build-perf/` (RelWithDebInfo)
profiling build compiled a-tree's Rust code - and found `cmake/BuildAtree.cmake` selected the
optimized `cargo build --release` profile only for the exact string `"Release"`, silently leaving
`RelWithDebInfo` (every profiling build in this project) compiling a-tree with **zero Rust
optimizations** while the surrounding C++ was fully optimized. That asymmetry inflated a-tree's
apparent share of CPU time in every profile captured up to this point - the 53.7%/20.5% split above
does not hold once compilation is made fair. Fixed the build-type check (only `Debug`/unset now
stays unoptimized, matching how CMake treats build types everywhere else), then re-profiled all
three configurations - old a-tree (`HashMap`), fixed a-tree (`Vec`, see below), and be-tree - as
properly-optimized `--release` builds, single instance, same continuous-backlog methodology:

| category | old a-tree (HashMap) | fixed a-tree (Vec) | be-tree |
|---|---:|---:|---:|
| matching-engine event construction (Rust FFI / be-tree equivalent) | 16.6% | 15.5% | 12.1% |
| msgpack row-unpacking (`mp_skip`, `memmove`, buffer writes) | 42.7% | 43.5% | 37.1% |
| stats timing (`clock_gettime`) | 3.4% | 3.2% | 8.5% |
| this codebase's own orchestration | 12.8% | 12.7% | 15.1% |

Corrected picture: msgpack row-unpacking is the real dominant cost (~37-44%) at roughly 2.5-3x the
matching engine's own share (~12-17%), the exact opposite emphasis from the flawed unoptimized
profile. This also resolves the open question from the a-tree/be-tree N=12 comparison above -
their near-identical fleet throughput was never surprising: engine choice was never going to move
the needle much when the shared msgpack pipeline costs 2.5-3x more than either engine's own
overhead. **Separately, found `atree::Tree::make_event()` allocates a fresh Rust
`HashMap<String, EventAttributeValue>` on every row for `FfiEventBuilder`'s own staging (checked
`atree.hpp`: no reset/reuse API, single-use by design) even though the core a-tree crate's own
`EventBuilder` is already index-based and allocation-light** - fixed upstream in
`mrayva/a-tree`@`b2bc18f` by replacing that staging `HashMap` with a `Vec<(String, ...)>` (insertion
order preserved, so replaying it into the core builder gives identical last-write-wins semantics;
all 15 existing a-tree-ffi tests pass unchanged). Real, but modest once fairly measured: ~1
percentage point of total CPU (16.6%->15.5%), consistent with a same-order single-instance
steady-state throughput check (~6,350 vs ~6,270 batches/s, within normal run-to-run noise for a
single 10-second sample) - a legitimate, zero-risk upstream fix worth keeping, not the large win
the unoptimized profile first suggested. The bigger remaining lever is still the msgpack side:
`expand_columnar()` does a full write pass (columns -> a new row-major buffer) that
`match_columnar_batch` then reads back row-by-row - a real write-then-read round trip. A row view
reading directly off the original column arrays, skipping that intermediate buffer entirely, would
cut into roughly half of the ~37-44% msgpack share - not yet built.

**Built and measured.** Added `zerialize::columnar_rows()` (zerialize commit `9878b79`) - a
forward-only cursor that walks a columnar-shaped `Reader` directly, one row at a time, giving the
same per-row field values `expand_columnar()` does with no `Writer` pass and no second
`Deserializer` parsing the result back in. Same validation, same O(n*m) column/row access pattern
(each column resolved once, walked sequentially via `.elements()` when the protocol provides it -
same idiom as `write_expanded_columnar()`), just never materializing the expanded document.
Cross-checked row-for-row against `expand_columnar()`'s own output across all 7 non-BSON protocols
in zerialize's test suite before this project ever depended on it. `match_columnar_batch()` in
`event_bridge.cpp` was rewritten to use it in place of `expand_columnar<Protocol>()` - `populate_event`/
`match_message` needed **zero changes** (a `ColumnarRows` row is just another `Reader`).

Verified with the same controlled methodology as the a-tree/be-tree comparison above: two binaries
built from the same properly-optimized `RelWithDebInfo` config, one at the commit just before this
change (still calling `expand_columnar()`), one after, single instance, same full 115,020,848-row
NYSE table published as a `--batch-size 500 --batch-encoding native` backlog, `perf record` for a
10-second window while each drained it, steady-state batches/s measured from the stats log across
that same window:

| | batches/s (single instance) | msgpack/round-trip share of CPU | msgpack/round-trip cost per batch |
|---|---:|---:|---:|
| before (`expand_columnar`) | ~4,575 | 44.5% | ~97.3µs |
| after (`columnar_rows`) | ~6,105 | 27.6% | ~45.2µs |

**A real ~33% single-instance batch-throughput gain, and it's cleanly attributable to the round trip
specifically, not a general speedup that happens to correlate.** Converting both profiles' relative
percentages to absolute per-batch CPU cost (each profile's total CPU-time-per-batch is just
`1/throughput`) makes this precise: total cost dropped from ~218.6µs/batch to ~163.8µs/batch (a
54.8µs drop), and the msgpack/round-trip category alone accounts for 52.1µs of that (~95% of the
total reduction) - every other category (matching engine, allocator, this codebase's own
orchestration, stats timing) stayed flat or improved slightly in absolute terms, none of them
regressed to offset the gain. The round-trip's own cost roughly halved (97.3µs -> 45.2µs, a 53.5%
cut), matching the "roughly half of the ~37-44% share" estimate above almost exactly - what's left
in that category now is `columnar_rows::advance()`'s own single sequential `mp_skip` walk over the
source payload, which is unavoidable (something has to read the bytes once), not a second write-then-
reparse of them.

**Applied the same principle to the write side, as asked ("build the row-view optimization for
expand_columnar's msgpack write side too")**: `serialize_row()` (and `expand_columnar()`'s own column
writes, for any other caller) go through `zerialize::write_value()`, which decodes a value
(`isInt()`/`asInt64()`, `isString()`/`asStringView()`, ...) and re-encodes it through the destination
Writer - wasted work whenever the source and destination are the *same* wire format, since the
source's bytes are already a complete, valid encoding in the destination's own format. Added
`write_value()`'s `raw_copy_compatible<V, W>` fast path (zerialize commit `9e64adc`, `raw_copy.hpp`) -
an opt-in trait, false by default, that a protocol specializes true for its own
(Deserializer, Serializer) pair; when true, `write_value()` copies the value's raw bytes straight
through (`v.raw_view()` -> `w.raw(...)`) instead of decoding and re-encoding, at *any* recursion depth
(a whole document, a nested map/array, or one scalar are all just "some bytes" to copy once this
fires). Wired up for `MsgPack`<->`MsgPack` specifically (`MsgPackSerializer::raw()`, using the same
`pk_.callback`/`pk_.data` primitive msgpack-c's own `pack_*` calls use internally). Verified with a
dedicated zerialize test asserting a same-protocol `translate<MsgPack>()` comes back byte-identical to
its source - true only if the fast path fires at every level, not just an equivalent decode - plus
the full existing suite (which checks decoded values, not bytes) unaffected.

This benefits every caller of `write_value()` generically, with **no call-site changes** in
nats_sidecar: `serialize_row()`'s per-field writes (via `columnar_rows()`'s `mapEntries()`, each field
already a real `MsgPackDeserializer`) now hit this path automatically. Measured with the same
controlled two-binary methodology as above (one binary just before this change, one after, single
instance, same saturated 115,020,848-row backlog, `perf` for a 10s window):

| | batches/s | `serialize_row()`'s own share of CPU |
|---|---:|---:|
| before | ~6,056 | 1.78% |
| after | ~6,006 | 0.60% |

`serialize_row()`'s own cost dropped by roughly two thirds (1.78% -> 0.60%) - real, and confirmed by
the byte-identity test to be the actual fast path firing, not noise - but aggregate single-instance
throughput did **not** measurably improve (~6,056 vs ~6,006 batches/s is within normal run-to-run
noise for a single 10-second sample, and if anything reads slightly lower). Consistent with the rest
of that same profile: `msgpack_sbuffer_write`'s own share rose by close to the same amount
serialize_row's fell (0.91% -> 1.48%) - `raw()` still has to hand the same bytes to the same
underlying write primitive `msgpack_pack_double()`/`msgpack_pack_str()` etc. already called internally,
it just skips the decode+re-encode logic *around* that call, so the win here is real but narrow: this
schema has one attribute (`price`) and two fields per row total, so there's only ever one or two
values per matched row for the fast path to save anything on. Unlike the read-side round trip (which
was ~40%+ of the whole per-batch cost because it touched *every* column of *every* row in the batch,
not just matched rows' own fields), the write side was already a small slice (~5% of the profile,
matching-engine/allocator/read-side costs dominate at ~65%+ combined) - a wider schema (more
attributes) or a higher match rate (more rows reaching `serialize_row()` per batch) would show a
larger effect; at this schema's actual shape, the fix is real, zero-risk, and worth keeping, but not
a lever that moves this benchmark's aggregate throughput.

**This means the N=12 full-table "~265k rows/s sustained ceiling" measured above is now stale** -
it was measured against the pre-fix binary. Re-ran the *exact same* N=12/`--workers 24` full-table
benchmark (same 115,020,848-row NYSE table, same `--batch-size 500 --batch-encoding native`, same
`sidecar.yaml`) against the fixed binary to measure the real-world impact rather than extrapolate:

| | before fix | after fix |
|---|---:|---:|
| batches received (=published by pgnats) | 230,264 | 230,261 |
| batches processed | 124,157 (53.9%) | 139,881 (60.8%) |
| batches dropped (local input queue full) | 106,107 (46.1%) | 90,380 (39.2%) |
| publish-side aggregate rate | 9.13M rows/s | 10.28M rows/s |

Loss dropped but did **not** go to zero, and that's expected rather than a sign the fix
underperformed: `--workers 24` was deliberately chosen in the original test to *saturate* the old
~265k rows/s ceiling by publishing far faster than N=12 single-threaded instances could ever drain
- and it still publishes (here, 10.28M rows/s aggregate) well above the new, ~7.3x-higher
per-instance ceiling once contention across 12 concurrent instances on one 12-physical-core host is
accounted for. `slow_consumers=0` in `nats-server`'s varz confirms the loss mechanism is still
exactly the same one as before (local per-instance input-queue overflow, not NATS transport) - just
quantitatively smaller.

**Found the actual zero-loss `--workers` setting for N=12 columnar, rather than leave it as an
untested follow-up.** Same N=12 fleet, same full 115,020,848-row table, `--workers` swept down from
24 (mirroring the row-mode N=12 sharpening methodology above, adapted since columnar's real ceiling
turned out to be much lower than row-mode's):

| `--workers` | publish rate | batches processed | batches dropped |
|---:|---:|---:|---:|
| 24 | 10.28M rows/s | 60.8% | 39.2% |
| 6  | 5.59M rows/s  | 71.5% | 28.5% |
| 4  | 4.14M rows/s  | 80.4% | 19.6% |
| 2  | 2.27M rows/s  | **100.0%** | **0%** |

**`--workers 2` is exactly loss-free** (0 batches dropped, `slow_consumers=0`) - unlike row-mode,
where N=12 tolerated `--workers` up to 6 before tipping into loss, columnar's per-instance
processing ceiling is low enough relative to how fast `--batch-size 500` batches arrive that only
2 parallel publish connections are sustainable at N=12 without raising `input_queue_max_messages`.
Matches the back-of-envelope expectation from the single-instance 7.3x speedup measurement (~1.93M
rows/s naive extrapolation for N=12) reasonably closely - the real threshold (between 2.27M
loss-free and 4.14M lossy) is a bit higher than that naive number, consistent with the isolated
single-instance backlog-drain test not perfectly predicting real multi-instance-contention
behavior. **Practical takeaway**: for a columnar connection at this N=12/single-worker-thread
scale, keep `--batch-size 500` publish concurrency at 2 workers (or raise `input_queue_max_messages`
substantially and/or `worker_threads` per instance if higher publish concurrency is required) - the
`--workers 4-6` range that was loss-free for row-mode does **not** carry over to columnar mode.

**be-tree comparison, same N=12/`--workers 24` benchmark, everything else identical (`engine: betree`
in place of the default `engine: atree`):**

| | a-tree | be-tree |
|---|---:|---:|
| batches processed | 139,881 (60.7%) | 139,488 (60.6%) |
| batches dropped (local input queue full) | 90,380 | 90,773 |
| rows matched (`price > 500.0`) | 3,733,599 | 3,286,903 |
| publish-side aggregate rate | 10.28M rows/s | 9.46M rows/s |

**(Historical snapshot - see the "Every fix in this whole investigation was measured with
`engine: atree`" note further down for why this no longer holds once the subsequent a-tree-specific
fixes are accounted for.)** Processing capacity is statistically indistinguishable between engines
at this workload (60.7% vs. 60.6% of batches processed - within run-to-run noise). The two runs' `--workers 24` publish jobs
raced the fleet at slightly different real-world rates (10.28M vs. 9.46M rows/s, ordinary
scheduling jitter between separate runs, not a deliberate variable), so the matched-row counts
differ too - different runs drop different specific batches under this kind of queue-full race,
not a correctness difference between engines. **This is expected, not a wash**: the perf-identified
bottleneck fixed above lives in nats_sidecar's own row-unpacking loop
(`event_bridge.cpp`'s `match_columnar_batch`), which runs identically regardless of which matching
engine sits behind it - at N=12/columnar scale, that unpack/serialize cost dominates enough that
`atree::Tree::search()` vs. be-tree's own search were never going to show up as the deciding
factor. A matching-engine choice would be expected to matter more under conditions where search
itself is the bottleneck (e.g. many more concurrent expressions per instance) - not measured here.

**Re-ran the same N=12/`--workers 24` full-table benchmark once more with both perf fixes in place**
(`columnar_rows()` replacing `expand_columnar()`, plus `write_value()`'s raw-copy fast path) - the
single-instance profiling above already measured each fix in isolation; this confirms what they add
up to at full N=12 fleet scale, rather than assuming the single-instance numbers simply multiply:

| | before either fix | `columnar_rows()` only | both fixes |
|---|---:|---:|---:|
| batches processed | 124,157 (53.9%) | 139,881 (60.8%) | 145,606 (63.2%) |
| batches dropped (local input queue full) | 106,107 (46.1%) | 90,380 (39.2%) | 84,661 (36.8%) |
| rows matched (`price > 500.0`) | 3,212,038 | 3,733,599 | 3,426,161 |
| publish-side aggregate rate | 9.13M rows/s | 10.28M rows/s | 9.88M rows/s |

Every instance's own `received`/`processed`/`input_dropped` counters reconcile exactly (230,267 =
145,606 + 84,661, summed across all 12 instances) - as before, confirming NATS itself delivered
every batch to some instance (100%, zero loss in transit) and the drop is entirely local
input-queue capacity, not a transport issue. Processed share climbed again (60.8% -> 63.2%), a
further real improvement on top of the `columnar_rows()` fix alone - consistent with, though this
run's publish rate was itself ~4% lower than the `columnar_rows()`-only run (9.88M vs. 10.28M
rows/s, ordinary run-to-run publish jitter, not a controlled variable), which if anything works
*against* seeing a higher processed share here, not for it. **Matches the single-instance profiling
finding exactly**: the raw-copy fast path was measured there as real-but-narrow (didn't move
single-instance batches/s outside noise, at this one-attribute schema) - here too, it adds a real
but modest further gain (60.8% -> 63.2%) on top of `columnar_rows()`'s much larger jump
(53.9% -> 60.8%), not a second dramatic step change. Both fixes are additive, not redundant, and
neither regresses the other.

**Swept `--workers` down at N=12 with both fixes to find the new zero-loss point**, same
methodology as the earlier row-mode and columnar sweeps above:

| `--workers` | batches processed | batches dropped |
|---:|---:|---:|
| 24 | 144,330 (62.7%) | 85,937 (37.3%) |
| 6  | 197,446 (85.8%) | 32,654 (14.2%) |
| 4  | 213,398 (92.8%) | 16,679 (7.2%)  |
| 3  | 230,070 (**100.0%**) | **0** |

`--workers 3` is the new exactly-loss-free point (up from `--workers 2` with `columnar_rows()`
alone) - a sharp threshold again, not gradual (7.2% loss at 4, 0% at 3), consistent with every
other threshold found in this investigation. Both perf fixes together bought this fleet roughly
one more `--workers` step of headroom at N=12.

**Investigated the three next-largest profile categories on request** (allocator, read-side
msgpack unpacking, this codebase's own orchestration/glue) rather than assume any of them had
easy further headroom:

- **Orchestration/glue - found and fixed a real, avoidable double allocation.**
  `populate_event()` did `std::string key(key_sv)` - allocating a new string every (row, attribute)
  pair - purely to satisfy `attribute_schema::lookup()` and `event_sink`'s `with_*()` methods,
  both of which were typed `const std::string&`. Neither actually needed an owned string:
  `atree::EventBuilder`'s own methods (`atree.hpp`) already take `std::string_view` throughout,
  converting to an owned string only once, at the point they need a null-terminated C string for
  the Rust FFI boundary - `populate_event()`'s own `std::string key` was a second, wholly redundant
  allocation on top of that unavoidable one. Fixed: `event_sink`'s virtual interface and both
  concrete implementations (`atree_event_sink`, `betree_event_sink`) now take `std::string_view`
  directly; `attribute_schema::types` and `betree_event_sink`'s index map both switched to a
  transparent-hash `unordered_map` (new `string_view_lookup_map<V>` in `matching_engine.hpp`) so
  `.find()` accepts a `string_view` without constructing a `std::string` first;
  `populate_event()` now passes `key_sv` straight through everywhere, allocation-free. Also caught
  and fixed a second, smaller redundant copy in the same function: the `string` case wrapped
  `value.asStringView()` in `std::string(...)` before calling `with_string()`, even though that
  method's value parameter was already `std::string_view` - pure wasted copy-then-discard.
  Verified: full 260-test suite passes unchanged (behavior, not implementation, is what's tested).
  **Measured honestly**: single-instance steady-state throughput came back statistically
  indistinguishable before/after (~6,158 batches/s both times, in windows chosen to be equally
  deep in a saturated backlog) - matching-engine and allocator categories both did shrink a little
  in the profile (25.7%->22.6% and 22.9%->21.8% respectively, consistent with a real eliminated
  allocation), but at this one-attribute schema the absolute effect is too small to clear
  measurement noise in a single-instance benchmark. Real, zero-risk, worth keeping (a schema with
  more attributes would show more) - not a lever that moved this benchmark, the same honest
  pattern as the write-side raw-copy fix above.

- **Allocator, beyond the fix above - built the identified lever, upstream in a-tree.**
  `matching_engine::make_event()` returned `std::unique_ptr<event_sink>` - a heap allocation for the
  sink object on *every* row, on top of what the concrete engine's own event builder allocates
  internally (a-tree's `EventBuilder::new()` was one of the largest single symbols in the profile,
  ~4.3%). Fixed at the source rather than worked around: **`mrayva/a-tree`@`61a85fe`** adds
  `Event::into_builder()`/`ATree::recycle_event()`, reclaiming a previously-built `Event`'s
  already-allocated buffer as a fresh `EventBuilder` (every attribute reset to `Undefined`) in
  place, no allocation - the buffer is a fixed-size `Box<[AttributeValue]>` sized to the tree's
  attribute count, so it never needs to grow or shrink and is safe to reuse indefinitely.
  `atree_search()` (the FFI boundary) no longer frees its `builder` argument or consumes its values
  by-value - it clears the FFI-side staging `Vec` in place (keeping its allocated capacity) and
  recycles the core `Event` via the above instead of `make_event()` when one is available from a
  prior call, stashing the new one back for next time. `atree.hpp`'s existing one-shot
  `search()`/`try_search()` keep their exact documented "consumed by this call" contract (now via
  an explicit free right after `atree_search()`, since Rust no longer does that automatically); a
  new `search_reusing()` is the opt-in path that leaves the builder valid for another row.
  Verified upstream with two new tests (core Rust and FFI/C++ level) specifically checking that a
  value from a prior reuse cycle never leaks into one that doesn't set it again, across three
  reuse cycles each - plus the full existing a-tree/a-tree-ffi suites (203+14 unit, 5+1 doc, 2
  smoke tests) unchanged.
  
  Wired up here: `matching_engine::reuses_events()` (new, default `false`) - `true` for a-tree
  (whose `search()` now routes through `search_reusing()` unconditionally, safe whether or not the
  caller actually reuses the sink - its own destructor still frees exactly once either way), left
  `false` for be-tree (its own `Event` allocates a fresh native variable object on every *individual*
  attribute set via its underlying C library, not once per event like a-tree - this codebase
  doesn't have enough visibility into whether replacing an already-set slot's variable is
  memory-safe to risk it, so be-tree keeps a fresh event per row exactly as before). A new
  `match_message()` overload takes a caller-supplied `event_sink&`; `match_columnar_batch()` makes
  *one* per batch (not per row) when `tree.reuses_events()`, reused across every row in that batch.
  Full 260-test nats_sidecar suite (including the a-tree/be-tree differential-agreement matrix and
  the multi-row-per-batch columnar tests, which already exercised values changing row to row across
  a reused sink) passes unchanged.

  **Measured real**: single-instance steady-state throughput, saturated backlog, same methodology as
  above: **~6,150 -> ~6,950 batches/s, a ~13% single-instance gain** - `EventBuilder::new` drops out
  of the profile's top symbols entirely. Re-ran the N=12/`--workers` sweep: the zero-loss point moved
  from `--workers 3` to **`--workers 4`**:

  | `--workers` | batches processed | batches dropped |
  |---:|---:|---:|
  | 6 | 206,383 (89.7%) | 23,717 (10.3%) |
  | 5 | 218,338 (94.9%) | 11,752 (5.1%) |
  | 4 | 230,077 (**100.0%**) | **0** |

  Real, measurable progress toward `--workers 6` (was 85.8%/14.2% before this fix, now 89.7%/10.3%)
  but not there yet - `--workers 6` and `--workers 5` both still drop some. `ATree::search()` itself
  (not just event construction) still allocates fresh scratch vectors on *every* call
  (`matches: Vec::with_capacity(50)`, `queues: vec![Vec::with_capacity(50); max_level-1]` -
  core a-tree's own `atree.rs`) - a further, separately-scoped lever in the same spirit as this one,
  not pulled in here to keep this change reviewable and its correctness risk (a second buffer-reuse
  change, this time to the search algorithm's own scratch state, not just event construction)
  isolated and verifiable on its own.

- **Followed up on that lever directly - traced it with `perf`'s call graph first, not just the flat
  profile, before deciding how much of it to build.** `ATree::search()`'s three scratch allocations
  turned out to have very different risk profiles once actually traced to their callers:
  `EvaluationResult::new()`'s allocation (~1.85% of `_int_malloc`'s own self-time, confirmed via
  `perf report -g`) is *not* lifetime-coupled to the tree at all - it just owns three fixed-size
  `Box<[u64]>` bitmaps - while `matches`/`queues` hold `&Entry<T>` references borrowed from the
  tree's own nodes, meaning reusing *their* allocations safely across FFI calls would need either
  unsafe lifetime-erasure or dropping `Report`'s lifetime parameter entirely (cloning every match
  instead of referencing it) - a real jump in engineering risk for what the same call-graph trace
  showed as the smaller remaining piece (`_int_free_chunk`'s own cost, checked the same way, turned
  out to be dominated by `row_match` destruction on the publish side, not search scratch at all).
  Built only the safe part: **`mrayva/a-tree`@`95c99f9`** adds `EvaluationResult::reset()`/
  `capacity()` and `ATree::search_with_results()`, reusing a caller-owned `EvaluationResult` (or
  transparently reallocating if it's undersized) instead of `search()` allocating a fresh one every
  call - `matches`/`queues` deliberately left exactly as they were, allocated fresh each time, not
  bundled into this change. a-tree-ffi's `atree_search()` routes through it automatically, stashing
  a reusable `EvaluationResult` in `FfiEventBuilder` the same way it already stashes a reusable
  `Event` - **no nats_sidecar code changes needed at all**, only the pin bump. Verified: 4 new tests
  (reset()/capacity() at the core level, `search_with_results()` reused across three distinct events
  checked against fresh `search()` each time) plus the full existing a-tree/a-tree-ffi suites
  unchanged.

  **Measured honestly**: `EvaluationResult::new` drops out of the profile entirely (confirms the fix
  works), but single-instance throughput came back statistically indistinguishable
  (~6,950 -> ~6,800 batches/s, a *decrease* smaller than the run-to-run noise already seen elsewhere
  in this investigation) - the same "real but too small to clear single-sample noise" pattern as the
  write-side raw-copy and `string_view` fixes earlier in this file. The **fleet-level** N=12 re-sweep
  told a clearer story, since it averages out single-capture noise:

  | `--workers` | before this fix | after this fix |
  |---:|---:|---:|
  | 6 | 89.7% processed | 93.7% processed |
  | 5 | 94.9% processed | 97.3% processed |
  | 4 | 100.0% (zero-loss) | 100.0% (zero-loss) |

  Real further progress at `--workers 5` and `--workers 6` (both got measurably closer to zero), but
  the zero-loss threshold itself is unchanged at `--workers 4` - neither `--workers 5` nor
  `--workers 6` crossed to 100%. **Stopping the "close the `--workers` 6 gap" thread here rather than
  building the riskier `matches`/`queues` lever**: three fixes deep into this investigation (columnar
  round trip, write-side raw-copy + `populate_event` allocation, now event/search-state recycling),
  the remaining gap keeps costing more engineering risk for less measurable gain each time - the
  clearest sign this is genuinely diminishing returns, not a lever away from `--workers 6` cleanly
  closing.

- **Read-side msgpack unpacking - little further low-risk headroom found.** `mp_skip` and
  `ColumnarRows::advance()`'s sequential walk are already the O(n) minimum for msgpack's
  variable-length encoding after this session's earlier round-trip fix - there's no second
  redundant pass left to cut, short of a fundamentally different wire representation (e.g. a
  fixed-width columnar format) that's out of scope for "improve the existing msgpack path."
  Nothing changed here.

**Every fix in this whole "improve sidecar performance" investigation was measured with
`engine: atree` (the default) - be-tree only ever got continuous *correctness* coverage
(`differential_matrix/matching_engine_differential`'s 46 a-tree-vs-be-tree agreement cases, run
after every change in this file) via `matching_engine::search(event_sink&)`'s shared interface,
never a performance re-check.** That matters because the "statistically indistinguishable" a-tree
vs. be-tree finding above (60.7% vs. 60.6% processed) predates *every* fix below it in this file -
the round-trip elimination and write-side raw-copy apply equally to both engines (they're in the
msgpack/orchestration layer, not the matching engine), but event/search-state recycling is a-tree-
specific by deliberate design (`matching_engine::reuses_events()` stays `false` for be-tree - its
own `Event` allocates per individual attribute set via its underlying C library, not once per
event, and this codebase doesn't have enough visibility into whether reusing an already-set slot
there is memory-safe to risk it). Re-ran the single-instance profile with `engine: betree`,
otherwise identical setup, to check whether that combination left the earlier "indistinguishable"
finding stale:

| | a-tree (all fixes) | be-tree |
|---|---:|---:|
| batches/s (single instance) | ~6,800-6,950 | ~5,900-5,970 |

**No longer indistinguishable - a-tree is now measurably faster, roughly 15%, purely because it
received three more rounds of engine-specific optimization be-tree structurally can't share.**
be-tree's own profile confirms *why*, and it's a different bottleneck shape than a-tree's:
`__strcmp_evex` (7.99%) and `betree_make_float_variable`/`cfree`/`_int_malloc`/`_int_free_chunk`/
`free_event`/`free_value` (allocation/deallocation, ~18% combined) dominate - be-tree's underlying
C library looks up variable names by string comparison and allocates a fresh native variable object
on every single attribute set, for every row, exactly as it always has (this fix intentionally left
that untouched - see above). This is the **current, accurate picture** - the "statistically
indistinguishable" language earlier in this file describes a snapshot from *before* this
investigation's a-tree-specific work and should be read as historical, not current.

**Fixed the `__strcmp_evex` piece directly, in be-tree itself.** Traced it to
`fill_event()` (`mrayva/be-tree`'s `src/tree.cpp`, called on every `betree_search_with_event()` -
i.e. every single row): it unconditionally re-resolved every variable's internal id via
`try_get_id_for_attr()` - lowercase the whole name, then a *linear `strcmp` scan over every
attribute in the schema* - even for variables `betree_set_variable()` had already resolved
correctly and cheaply moments earlier, straight from `event->config->attr_domains[index]`, no
string comparison at all. Every event this codebase's own index-based `Event::set_*()` C++ wrapper
builds (the only way nats_sidecar ever builds one) already has that id resolved by the time
`fill_event()` runs - it was being thrown away and recomputed identically via the slow path, on
every attribute, every row. **`mrayva/be-tree`@`3d13dd3`**: `fill_event()` now only falls back to
the name-based scan when the id is still genuinely unresolved (e.g. an event parsed from JSON,
which starts that way) - zero behavior change for that path, skips straight past the scan for
everything else. Verified against be-tree's own full test suite (24 CTest targets, 100% pass) -
including `cpp_wrapper_smoke`'s index-vs-JSON parity check and its explicit
reuse-with-a-replaced-value-then-re-search scenario, exactly the case this change's correctness
depends on. **No nats_sidecar code changes needed, only the pin bump.**

**Measured real**: single-instance throughput ~5,900-5,970 -> **~6,300 batches/s, roughly a 7%
gain** - `__strcmp_evex` and `tolower` both drop out of the profile's top symbols entirely,
narrowing (not closing) the gap to a-tree from ~15% to roughly ~8%. `betree_make_float_variable`
(6.69%) - the per-attribute-set native allocation `try_get_id_for_attr` sat next to, not itself -
is now be-tree's single largest remaining symbol: a further, bigger, and meaningfully riskier lever
than this one (it would mean either a new be-tree API for updating an existing variable's value in
place instead of allocating a fresh one, or reusing variable objects across searches the way
a-tree's `EventBuilder` now does - not attempted here, offered as a next step rather than pulled in
unprompted).

**Extended the write-side raw-copy fast path to BEVE, ruled CBOR out architecturally.** Asked to do
the same `raw_copy_compatible<V, W>` treatment for CBOR and BEVE that MsgPack already got above.
CBOR was tried and reverted: jsoncons' CBOR encoder (`basic_cbor_encoder`) tracks a private
per-container item count, set by `begin_array(n)`/`begin_object(n)` and validated against what
actually got written at `end_array()`/`end_object()` - it throws
(`"Too few items were added to a CBOR map or array of known length"`) if a raw value is spliced in
without going through its own counted write calls, and there's no public API to inject pre-encoded
bytes while keeping that counter in sync. Confirmed by an actual crash, then fully reverted
(`cbor.hpp` back to its original state, full zerialize suite re-verified clean) - this is a settled
architectural conclusion for jsoncons as it stands today, not a "needs more effort" item.

BEVE's own hand-rolled writer has no such bookkeeping (`begin_array(n)`/`begin_map(n)` write the
count up front, `end_array()`/`end_map()` are literal no-ops), so it was safe. One extra wrinkle
BEVE has that MsgPack doesn't: its lazy reader can *synthesize* a value with no real backing byte
range at all (e.g. one element pulled out of a typed array, built on the fly from the array's flat
packed storage). zerialize's `write_value()` gained a new, general, optional per-*value* escape
hatch for this (`raw_copy_safe()`, `raw_copy.hpp`) alongside the existing per-*type*
`raw_copy_compatible` trait - `BeveDeserializer::raw_copy_safe()` returns false for these
synthesized elements, falling back to the normal decode/re-encode path. Verified with a test that
hand-crafts a raw BEVE document containing a numeric typed array at the byte level specifically to
exercise this guard (not just decoded-value equality, which wouldn't have caught the CBOR-style
failure mode), plus the standard byte-identity test mirroring MsgPack's. Zerialize commit `6e9d417`,
nats_sidecar pin bump `4d01fd5` - no nats_sidecar code changes needed.

**Measured single-instance** (`format: beve`, `columnar: true`, same real 115,020,848-row NYSE
backlog and two-binary methodology as the MsgPack write-side measurement above):

| | `serialize_row`+`write_value`+`build_pub_frames` combined self-time |
|---|---:|
| before | ~0.81% |
| after | ~0.67% |

A real but modest reduction - smaller than MsgPack's own write-side win, plausibly because BEVE's
scalar encoding (fixed-width header + memcpy) was already cheaper than MsgPack's variable-length
encoding, so there's less to save per value. Aggregate single-instance throughput was statistically
unchanged (~9,350-9,520 batches/s both ways, within normal run-to-run noise) - same "real, narrow,
schema-shape-limited" pattern as MsgPack's write-side fix: this schema has one attribute and two
fields per row, and the read-side columnar/matching-engine work still dominates the profile (the
top 15 symbols are the same functions in nearly the same proportions before and after). A wider
schema or higher match rate would show a larger effect.

**Profiled all 4 prioritized formats (msgpack, cbor, beve, flexbuffers) side by side to find each
one's own top hot functions** - single instance, same real 115,020,848-row NYSE backlog, current
binary (every fix above applied), `format:`/`--format` swapped per run:

| rank | msgpack | cbor | beve | flexbuffers |
|---|---|---|---|---|
| 1 | `mp_skip` (12.78%) | `CborDeserializer::KeysView::iterator::read_head` (8.02%) | `ColumnarRows<Beve>::advance` (10.23%) | `__strlen_evex` (7.36%) |
| 2 | `EventBuilder::with_float` (9.64%) | `EventBuilder::with_float` (6.94%) | `shared_ptr::_M_release` (6.61%) | `EventBuilder::with_float` (7.30%) |
| 3 | `atree_search` (6.26%) | `KeysView::iterator::skip` (5.62%) | `EventBuilder::with_float` (6.26%) | `atree_search` (5.28%) |
| 4 | `__strlen_evex` (5.96%) | `atree_search` (5.44%) | `atree_search` (5.30%) | `match_message<Flex>` (4.07%) |
| 5 | `ColumnarRows<MsgPack>::advance` (4.87%) | `__strlen_evex` (3.78%) | `shared_ptr::_M_add_ref_copy` (4.94%) | `alloc::Vec::from_elem` (3.96%) |

Each format's #1 spot is its own read-side unpacking cost (msgpack's `mp_skip`/CBOR's key-iteration/
BEVE's `ColumnarRows::advance` sequential walk), exactly as expected given the earlier per-format
architecture analysis. The one truly interesting cross-cutting finding: `__strlen_evex` (libc) sat
in **every single format's** top 5 (traced with `perf report -g` to confirm it's the same call path
in all four - not a coincidence). BEVE's own list additionally surfaces its `shared_ptr`
refcounting (`glz::lazy_beve_document` is wrapped in a `shared_ptr`) as a real, BEVE-specific cost
not present in the other three - not yet investigated further.

**Fixed the shared `__strlen_evex` cost - it wasn't format-specific at all.** Traced its call graph:
every format routes through `atree_event_builder_with_float()` (`a-tree-ffi`'s C ABI), which read
its `name` parameter via `CStr::from_ptr(name).to_str()` - a `strlen()` scan for the null
terminator, on every single row, even though (a) the C++ caller (`atree.hpp`'s
`EventBuilder::with_float(std::string_view name, ...)`) already knew the length from
`std::string_view::size()`, and (b) the attribute name (`"price"`) is invariant across an entire
batch. Pure, repeated, format-agnostic waste. **Fixed in `mrayva/a-tree`@`57650d0`**: every
`atree_event_builder_with_*()` C function (`with_boolean`/`with_integer`/`with_string`/`with_float`/
`with_string_list`/`with_integer_list`/`with_undefined`) now takes an explicit `name_len` (and
`with_string` a `value_len`), reading via `slice::from_raw_parts` + `str::from_utf8` instead of
`CStr::from_ptr` - no strlen scan, no null-terminator requirement. `atree.hpp`'s public C++
`EventBuilder` API is completely unchanged (still `std::string_view` in, no call-site changes
anywhere) - only its internals changed, from `std::string(name).c_str()` (an owned, materialized,
null-terminated copy) to `name.data()`/`name.size()` passed straight through, which also drops a
C++-side allocation that existed purely to satisfy the old C string convention. Verified: 14
a-tree-ffi unit tests + the C smoke test (updated to pass explicit lengths) + the C++ smoke test
(**unchanged** - proof the public API didn't move) all pass; core a-tree's 207 unit + 5 doc tests
unaffected. `nats_sidecar` pin bump `64e70a5`, full 260-test suite (including the a-tree/be-tree
differential-matrix correctness suite, which exercises every `with_*` variant, not just
`with_float`) passes unchanged - no nats_sidecar code changes needed.

**Re-profiled msgpack to confirm**: `__strlen_evex` is completely absent from the post-fix top 20
(was 5.96%, rank 4). This run's absolute batches/s isn't directly comparable to the table above (the
host was measurably busier during this capture - aggregate publish rate dropped from ~11M to ~4M
rows/s between the two runs, a confound unrelated to this change) - the qualitative confirmation
(the symbol vanishing entirely) is the reliable signal here, not this particular run's throughput
number. A clean re-measurement of all 4 formats' absolute throughput is a natural follow-up, not
done in this pass.

**Went one step further on the same call path.** Removing the strlen scan still left
`FfiEventBuilder::values.clear()` (end of every `atree_search()`) dropping every
`(String, EventAttributeValue)` tuple, and the next row's `with_*()` call immediately
re-allocating a fresh heap `String` via `.to_owned()` for the *same* attribute name - the strlen
fix removed the scan but not the malloc/free pair around it, and the name is invariant across an
entire batch in the common case (one schema attribute set on every row). **Fixed in
`mrayva/a-tree`@`318917b`**: `FfiEventBuilder` now tracks `values_used` (how many slots are live
for the *current* row) separately from the backing `Vec`'s actual size - a slot left over from a
longer previous row gets its existing `String` buffer cleared and refilled via `push_str()` in
place instead of being dropped and reallocated, reaching zero allocation once the `Vec`/`String`s
have grown to a batch's steady-state size. A new test
(`reused_slot_correctly_overwrites_a_shorter_or_longer_attribute_name`) specifically exercises the
buffer-reuse path with names of different lengths in the same slot, on top of the existing
stale-value-leak test (still passing - a shorter row genuinely doesn't see a longer previous row's
leftover attributes, since every reader respects `values_used`, never `.len()`). All 15 a-tree-ffi
unit tests + C smoke + C++ smoke (**unchanged** - still no C++ API surface change) pass.
`nats_sidecar` pin bump `7afeebb`, full 260-test suite passes unchanged.

**Measured real**: re-profiled msgpack again. `EventBuilder::with_float` +
`atree_event_builder_with_float` (the two symbols the strlen fix left as msgpack's #2/#3 hottest,
combined ~18.8%) collapsed to `push_attr` + `EventBuilder::with_float` at combined ~5.8% -
`atree_event_builder_with_float` itself no longer registers separately in the top 20 at all.
`_int_malloc`/`cfree` are visibly quieter too. Absolute throughput moved in the same noisy-but-
positive direction as the strlen fix (~10,834 -> ~11,566 batches/s, one 10s sample each side - real
signal, not a rigorous multi-sample measurement) - the profile-share collapse is the reliable
finding here, same as with the strlen fix itself.

**Fixed a related CMake gap found while bumping the pin twice in a row for this work,
`nats_sidecar`@`7afeebb`.** `BuildAtree.cmake`'s Rust build step was an `add_custom_command` keyed
only on its output path (`liba_tree_ffi.a`) with no `DEPENDS` on the actual source - `make` silently
skipped invoking `cargo build` at all once that file already existed in a reused build directory,
even after a pin bump changed the fetched source underneath it (caught needing a manual
`cargo build` workaround twice this session). Switched to a plain custom *target* - unlike an
`OUTPUT`-tracked custom command, a target's `COMMAND`s run unconditionally on every build
invocation, so `cargo build`'s own fast (~0.3-0.8s), correct incremental staleness detection
decides instead of CMake trying to replicate it. Verified using this session's own second pin bump
as the test case: reconfigured, ran `make` alone (no manual workaround) in both `build/` and
`build-perf/` - both correctly showed `Compiling a-tree-ffi` and relinked. A following no-op
`make` completes in ~0.2s, confirming negligible steady-state cost.

**Went one level deeper on the same call path again: resolving an attribute *name* to its internal
id, not just reading or copying it.** `EventBuilder::with_float()` (core `a-tree`) resolves `name`
via `AttributeTable::by_name()` - a hash-and-probe lookup - on every single call, even though a
caller building many events against the same tree (e.g. one per row of a batch) very often sets the
exact same, invariant attribute names every time. Showed up in the post-fix profile as
`core::hash::sip::Hasher::write` plus part of `EventBuilder::with_float`'s own share.

**First attempt didn't actually work, caught by re-profiling rather than assumed.**
`mrayva/a-tree`@`9caa557` added `ATree::attribute_id()` + index-based `EventBuilder::with_*_by_id()`
methods (skip the hash lookup given an already-resolved id) and cached every attribute's id in
`a-tree-ffi`'s `ATreeHandle` at `atree_new()` time - but `atree_search()`'s replay loop still called
`handle.attribute_ids.get(name)` on **every row**, which is itself a hash lookup by name. The fix
relocated the cost from core a-tree's internal hashmap to a-tree-ffi's own one, without eliminating
it from the per-row path at all. Caught immediately by re-profiling instead of assuming success:
`hash_one::<&String>` was still sitting in the top 20, at roughly the same share the original
lookup had.

**Real fix, `mrayva/a-tree`@`bc434fb`**: `push_attr()` (see the buffer-reuse fix above) now also
caches the resolved `AttributeId` *in the slot itself*, invalidating it back to `None` only when
the incoming name genuinely differs from what that slot held last time - in the steady-state case
(the same schema attribute set on every row), a slot's id is resolved exactly **once**, on its
first use, and never looked up again. `atree_search()`'s replay loop only touches
`attribute_ids` when a slot's cached id is `None`, writing the result back into the slot for every
subsequent row to reuse. The existing `reused_slot_correctly_overwrites_a_shorter_or_longer_attribute_name`
test (alternates two different names in the same slot across three searches) already exercised the
invalidation path and still passes - proof the cache genuinely tracks per-slot identity correctly,
not just "does it happen to work for the common case." `nats_sidecar` pin bump `38710b5`, full
260-test suite passes.

**Measured real**: re-profiled msgpack a third time. `core::hash::sip`/`hash_one::<&String>` are
completely gone from the top 20 this time (the only remaining `_Hash_bytes` symbol, ~0.95%, is
unrelated - libstdc++'s own, not core a-tree's `AttributeTable`). `with_float_by_id` (8.19%) and
`push_attr` (5.18%) are now genuinely just the value-write/`Decimal::new()` cost and the name
comparison/slot write respectively, not hashing hiding inside either. This is the third fix on this
exact call path (strlen -> String-buffer reuse -> name-to-id resolution) - each one real, each one
verified by re-profiling rather than assumed from the code change alone, which is exactly what
caught this one's first attempt not actually working.

**Next target: `shared_ptr<spdlog::logger>` passed by value through the per-row match path.**
`populate_event()`/`match_message()`/`match_columnar_batch()`/`deserialize_and_match()`/
`deserialize_and_match_columnar()` (`event_bridge.hpp`/`.cpp`) all took
`std::shared_ptr<spdlog::logger> log` by value, even though every one of them only ever reads
through it synchronously (`log->debug/warn(...)`) - never stores it or extends its lifetime.
`match_columnar_batch()`'s per-row `process_row()` lambda calls `match_message()`, which calls
`populate_event()` - two nested by-value copies per row, four atomic refcount operations, purely to
satisfy parameters that never needed ownership in the first place. Showed up in every format's
profile (not just one) as `shared_ptr::_M_add_ref_copy`/`_M_release`, since the logger is threaded
through on every row regardless of which wire format is in use.

**Fixed** (`nats_sidecar`@`9f1a049`): changed all 8 signatures (5 in `event_bridge.hpp`, 3 in
`event_bridge.cpp`) to take `const std::shared_ptr<spdlog::logger>&` instead. Every call site
already passes an lvalue (a parameter, a member like `worker_pool::m_log`, or a reference captured
by `process_row`'s lambda), so nothing else needed to change - a pure signature fix. Full 260-test
suite passes unchanged.

**Measured real**: re-profiled msgpack again. `shared_ptr::_M_add_ref_copy`/`_M_release` are
completely gone from the top 20 (were ~1.27% each, ~2.5% combined, in the previous capture) -
`populate_event`'s own profile symbol now shows `std::shared_ptr<spdlog::logger> const&` in its
demangled signature, directly confirming the fix is the one actually running.

## be-tree: event-container reuse ("Increment A")

**Re-ran the a-tree/be-tree head-to-head** (single instance, msgpack, same real 115,020,848-row NYSE
backlog, current binary) to check whether the gap - last measured at ~8% - had moved, since a-tree
had since picked up three more engine-specific fixes (strlen, String-buffer reuse, name-to-id
caching) be-tree structurally doesn't share:

| | a-tree | be-tree |
|---|---:|---:|
| steady-state throughput | ~11,700 batches/s | ~9,380 batches/s |

**The gap had widened to ~25%.** be-tree's own profile pointed at one clear, dominant cause:
`betree_make_float_variable` (9.64%), `betree_matching_engine::make_event()` (4.43%),
`__libc_calloc` (5.88%), and `cfree` (3.41%) - ~23% of the whole profile - all traced to the same
root: `matching_engine::reuses_events()` was `false` for be-tree, so every row got a brand new
`betree_event` (a fresh `bmalloc()` for the struct plus a fresh `bcalloc()` for its
variable-pointer array via `betree_make_event()`) instead of reusing one across the batch the way
a-tree's `EventBuilder::recycle_event()` already does.

**Traced into be-tree's own C source before touching anything, to scope the risk properly.**
`Event::clear(index)` (`betree_cpp.hpp`) turned out to already be exactly the primitive needed:
just `betree_set_variable(event, index, nullptr)`, which safely frees whatever was in that slot and
clears it - an existing, already-exercised be-tree call (every `with_undefined()` already goes
through it), not new or unverified C code. That meant reusing the event *container* was
substantially lower-risk than reusing individual *variables* (which would need genuinely new
be-tree C API to update a value in place, and remains a separate, deliberately-deferred "Increment
B" - see below).

**Fixed** (`nats_sidecar`@`d94730a`): `betree_matching_engine::reuses_events()` now returns `true`.
`betree_event_sink::reset()` loops `Event::clear()` over every attribute index;
`betree_matching_engine::search()` calls it unconditionally after every search (success or
failure), so a reused sink is always "blank" for the next row - the same blanket-reset principle
a-tree's own recycling uses, rather than tracking which specific slots need clearing. New test
`event_sink_reused_across_searches_has_no_stale_values` (parameterized over both engines) reuses one
sink across three searches, the second deliberately setting *fewer* attributes than the first, and
checks against expressions chosen so a leaked stale value would flip the answer. Verified two ways:
the normal 262-test suite, and the same suite again under nats_sidecar's existing ASan+UBSan
sanitizer build (`build-sanitizer/`) - both clean, no leaks, no sanitizer errors.

**Measured real**: re-profiled be-tree again. `betree_matching_engine::make_event()` dropped from
4.43% to 0.02%, and `betree_make_event` itself (the be-tree C function) to 0.00% - both now called
once per *batch* instead of once per *row*, exactly as designed; `match_columnar_batch`'s own
self-time dropped from 8.44% to 1.98% alongside it. Steady-state throughput: ~9,380 -> ~10,770
batches/s (~15% gain) - the a-tree/be-tree gap is back down to roughly ~9%, close to where it stood
before this session's a-tree-only fixes widened it to ~25%.

**What's still not covered ("Increment B" at that point, not yet attempted)**:
`betree_make_float_variable` itself (11.63%, then the single largest be-tree-specific symbol) -
be-tree allocates a fresh native variable object on every individual attribute *set* regardless of
whether the event container is fresh or reused, and there was no existing be-tree API to update an
already-set slot's value in place.

## be-tree: update scalar variables in place ("Increment B")

**Traced the actual allocation before writing anything, to scope the risk properly** (per the
user's "how to approach Increment B" ask): `struct value` (be-tree's internal tagged-union-shaped
struct for a variable's payload) stores `float_value`/`integer_value`/`boolean_value` as *inline
fields*, not behind a pointer - `betree_make_float_variable()`'s only allocations are the outer
`betree_variable` struct itself and a `bstrdup()` of the attribute name. That means updating an
*existing* float/int/bool variable's value is a single field write, with no allocation at all -
`attr_var` (name + resolved id) and `value_type` never need to change once a variable exists for a
given slot, since a slot's attribute (and therefore its type) is fixed for the tree's whole
lifetime.

**The naive design has a real correctness trap, found before writing code.** The first sketch -
"keep a touched slot's variable alive across rows for reuse, clear only untouched ones" - silently
reintroduces the exact stale-value bug Increment A exists to prevent: the decision to keep a slot
alive is made at the end of row N based on row N's own touched-set, before anyone knows whether row
N+1 will touch it too. be-tree's regular search path has exactly one way to represent "undefined" -
a null pointer in the slot - so an allocated-but-not-yet-cleared variable sitting there is
indistinguishable from a genuinely current one. The actual fix needed a small object pool, kept
deliberately separate from the event's own `variables[]` array.

**Fixed** (`mrayva/be-tree`@`bebd8b6`, `nats_sidecar`@`0fb611f`): added
`betree_update_{boolean,integer,float}_variable()` to be-tree (a plain field overwrite, additive
API, scoped to these three scalar types only - string/list values hold their own nested
allocations, where "update in place" would need real size-comparison/reallocation logic, a separate
and harder problem not attempted). `betree_event_sink` now keeps a per-slot pool
(`m_spares`) of detached-but-not-freed variables: `reset()` (Increment A) detaches a touched
slot's variable directly (`event->variables[i] = nullptr`, bypassing `Event::clear()`'s free) into
the pool instead of freeing it; `with_boolean`/`with_integer`/`with_float` check the pool first -
a hit calls the new update function and reattaches the same pointer directly (also bypassing
`betree_set_variable()`, which would otherwise redundantly re-derive and `bstrdup()` the attribute
name on every single call even though it's already correct); a miss falls back to the existing
allocate-and-attach path. The sink's destructor frees anything still sitting in the pool, since
`betree_free_event()` only walks what's *currently attached*.

New test `event_sink_reused_across_many_cycles_with_staggered_attributes`: 12 searches on one
reused sink, staggering which of three scalar attributes get touched each cycle on different
periods, so every attribute cycles through touched->untouched->touched several times - checked
against expressions that would give a different answer if a value leaked through or failed to
update. Verified the same way as Increment A: the normal suite (264 tests, up from 262) and the
full suite again under the ASan+UBSan sanitizer build, this time with `detect_leaks=1` explicitly
enabled given this is exactly the kind of manual C-level memory lifecycle code most likely to leak
- both clean, zero leaks, zero sanitizer errors.

**A real build-system gap found along the way**: be-tree's `betree.h` only exposes its C
declarations when compiling as *C* - under C++ (`__cplusplus` defined, true for every real
consumer including nats_sidecar), it `#include`s a separate `betree.hpp` instead. The first attempt
added the new functions only to `betree.h`'s C branch, which compiled and passed be-tree's own
*C-only* `betree_tests.c` suite fine, and only failed once nats_sidecar's own (C++) build tried to
call them - `betree.hpp` needed the exact same declarations added separately.

**Measured real**: re-profiled be-tree a third time. `betree_make_float_variable` collapsed from
11.63% to 0.04%, `betree_set_variable` from ~1.56% to 0.00%; the new `betree_update_float_variable`
itself costs only 0.23% (matching the "single field write" design). Steady-state throughput:
~10,770 -> ~11,000+ batches/s - **the a-tree/be-tree gap, which had widened to ~25%, is now down to
roughly 5-8%**, close to fully closed.

## be-tree: attribute-name lookup via linear scan, not a hashmap

Re-profiling after Increment B surfaced `betree_event_sink::index_for()`'s attribute-name-to-index
lookup - a `std::unordered_map<std::string_view, std::size_t>` with transparent hashing
(`string_view_lookup_map`) - as a small but real cost on the hot per-attribute path, once per
`with_*()` call. be-tree schemas are small (real ones seen so far: single digits to low tens of
attributes), the same "hash overhead exceeds linear-scan cost at this size" reasoning a-tree-ffi's
own event-builder already relies on (a `Vec`, not a `HashMap`, for the identical name-to-id
problem).

**Fixed** (`nats_sidecar`@`b532097`): added `small_attr_map<V>`, a linear-scan
`std::vector<std::pair<std::string, V>>`, replacing `string_view_lookup_map<std::size_t>`
everywhere be-tree's own attribute-index map is built and read (`build_betree()`,
`betree_event_sink`, `betree_matching_engine`). No behavior change - `find()`/`set()` have the same
semantics as the map they replace - so this needed no new test coverage beyond the existing suite
passing unchanged.

## be-tree: reuse the report and undefined-bitmap buffers ("Increment C")

Re-profiling again after Increment B (with the columnar-batch feature - see "Columnar batch input"
below - now in the picture, moving the workload from one search per NATS message to one search per
*row* of a batch) surfaced two more distinct `__libc_calloc` callers, unrelated to per-row event
setup: `make_undefined()` (5.27%) - be-tree's undefined-variable bitmap, one fresh
`(attr_domain_count+63)/64`-`uint64_t` allocation per search - and `make_report()` (1.27%) - the
`struct report` returned by every search, freed and reallocated fresh every single time even though
its shape never changes between searches against the same tree.

**Fixed** (`mrayva/be-tree`@`80048d3`, `nats_sidecar`@`02e4768`): added
`betree_refresh_undefined()` (recomputes `make_undefined()`'s bitmap into an existing, caller-owned
buffer instead of allocating a new one), `betree_search_with_event_reusing()` (like
`betree_search_with_event()`, but takes that buffer as a required parameter), and
`betree_reset_report()` (resets an existing report's counters/subs/state for another search instead
of `free_report()`+`make_report()`), plus a `Tree::search_reusing()` C++ wrapper mirroring a-tree's
own `search_reusing()` convention (returns matched ids directly). `betree_event_sink` now owns a
persistent `report*` (`make_report()` in its constructor, `free_report()` in its destructor) and a
`std::vector<uint64_t>` undefined-bitmap scratch buffer, both sized once per sink instead of
allocated fresh per row; `reset()` (already called unconditionally after every search to prepare the
sink for its next row, from Increment A) now also calls `betree_reset_report()` on the report,
keeping the same "always call reset() after search, sink is ready to reuse again" invariant for the
report as it already had for the event. `betree_search_with_preds()`'s signature changed to take
`undefined` as a required caller-owned parameter instead of allocating/freeing it internally - its
sole existing caller (`betree_search_with_event_filled()`, be-tree's own non-reusing path) preserves
the old allocate-and-free-every-time behavior, so no other call site's behavior changes.

New be-tree-level test `test_search_with_event_reusing` (C, `tree.cpp`'s level) and
`verify_search_reusing()` (C++, `betree_cpp.hpp`'s level) both exercise several searches in a row
against the same reused report/undefined buffers, including a variable flipping to/from undefined
between searches, proving both buffers are correctly recomputed each time rather than going stale.
Verified the same way as Increments A and B: be-tree's own 24-target suite (normal build and
`build-asan/`, `ASAN_OPTIONS=detect_leaks=1`), and nats_sidecar's full 264-test suite (normal
`build/` and `build-sanitizer/`, same leak-detection flag) - all clean.

**Measured real**: re-profiled a fourth time. `make_undefined` and `make_report` are both gone
entirely from the profile's `__libc_calloc` call graph - the only remaining `__libc_calloc` caller on
the be-tree engine path is now `make_environment()` (0.84%, its own array allocation, deliberately
out of scope for this increment - not confirmed as individually significant the way
`make_undefined`/`make_report` were). Total `__libc_calloc` self-time across the whole profile:
0.86%, down from a combined ~6.5% for `make_undefined`+`make_report` alone before this fix.

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

The format defaults to `msgpack` if `-f` is not specified. Supported formats: `msgpack`, `cbor`, `flexbuffers`, `zera`, `ion`, `bson`, `beve`, `arrow` (the sample file is then a whole Arrow IPC batch, e.g. from `pg_arrow`'s `rows_to_arrow()`, not a single-row zerialize message - type is read from row 0's own values, same as every other format).

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

## Matching-engine benchmark: a-tree vs be-tree vs pstree

The end-to-end fleet benchmarks earlier in this file found matching-engine choice barely mattered
("Processing capacity is statistically indistinguishable between engines... A matching-engine
choice would be expected to matter more under conditions where search itself is the bottleneck -
not measured here") because NATS transport and row-unpacking dominated at that scale. This is that
missing measurement: `benchmarks/matching_engine_bench.cpp` (`-DSIDECAR_BUILD_BENCHMARKS=ON`,
`RelWithDebInfo`) isolates pure matching-engine cost in-process - no NATS/Postgres/OS I/O at all -
specifically to independently check the PS-Tree paper's own self-reported claim (PSTDynamic beats
both BE-Tree and A-Tree on matching time and index construction time).

**Methodology**: a fixed non-list schema (`trade_price` float, `trade_volume` integer, `symbol`
string, `active` boolean - pstree has no list-attribute support, so this benchmark's schema and
generated subscriptions avoid them entirely, for a fair 3-way comparison). K subscriptions and
20,000 events are generated once per K with fixed seeds (identical input to all three engines);
each K runs two full passes with engine order reversed between passes (atree/betree/pstree, then
pstree/betree/atree) and the two passes' timings are averaged - cheap insurance against
warmup/cache-locality bias between consecutive engine runs in the same process (see the run-order
lesson earlier in this file, though its actual mechanism - OS page-cache/process-level drift - does
not really apply to a single in-process CPU benchmark with no I/O).

**A real a-tree bug surfaced immediately, before any performance number could be trusted.**
Aggregate match counts across all 20,000 events agreed exactly between all three engines at
K=1,000, but a-tree's count started diverging from be-tree/pstree (which agreed with each other)
at K=2,000 and grew with K - a correctness bug, not noise. Root-caused to a real gap in a-tree's
own `insert_root()`: when a brand new subscription's whole expression turns out to already exist
as some *other* subscription's non-accessor AND-child (a-tree picks the cheaper of an AND's two
predicates as the "accessor" - the one that's eagerly evaluated and can trigger the AND; the other
is only ever reached lazily, from inside the AND's own evaluation), the new subscription's id gets
attached to that node, but the node itself was never registered as an eager evaluation entry point
- so it would silently never match, for any event, no matter how obviously true the predicate is.
Fixed upstream (`mrayva/a-tree@a9a8829`, with a dedicated regression test) and confirmed: the exact
per-event/per-subscription diff that exposed it (a diagnostic tool built for this investigation,
not kept in the repo) found zero further disagreements after the fix, and this file's own
`test_matching_engine_differential.cpp`/`test_matching_engine.cpp` suites - which only ever
exercise a handful of subscriptions at a time - never had a chance to catch it. This is exactly the
kind of bug that scale-dependent testing (not hand-picked small examples) exists to find.

**Results, before any fix** (RelWithDebInfo, one host, 20,000 events per K):

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 1.04 ms    | 960,698 subs/s | 225.75 ms   | 88,595 events/s | 5,286,377  |
| 1,000  | betree | 2.85 ms    | 350,518 subs/s | 400.51 ms   | 49,937 events/s | 5,286,377  |
| 1,000  | pstree | 14.46 ms   | 69,155 subs/s  | 314.54 ms   | 63,586 events/s | 5,286,377  |
| 5,000  | atree  | 9.48 ms    | 527,374 subs/s | 1,361.91 ms | 14,685 events/s | 27,180,339 |
| 5,000  | betree | 46.91 ms   | 106,578 subs/s | 2,554.98 ms | 7,828 events/s  | 27,180,339 |
| 5,000  | pstree | 508.07 ms  | 9,841 subs/s   | 5,794.04 ms | 3,452 events/s  | 27,180,339 |
| 10,000 | atree  | 34.77 ms   | 287,613 subs/s | 2,862.11 ms | 6,988 events/s  | 54,413,004 |
| 10,000 | betree | 239.08 ms  | 41,827 subs/s  | 5,916.02 ms | 3,381 events/s  | 54,413,004 |
| 10,000 | pstree | 2,434.18 ms| 4,108 subs/s   | 19,637.96 ms| 1,018 events/s  | 54,413,004 |

Match counts agree exactly across all three engines at every K - the correctness bar this
comparison needs to mean anything.

**pstree did not win here, and degraded far faster than either a-tree or be-tree as K grew.** At
K=1,000 it was already the slowest to insert (5-14x a-tree/be-tree) but competitive on search
(within 1.4x of a-tree). By K=10,000, pstree's insert rate had fallen 17x further behind a-tree
(vs. 3.3x at K=1,000) and its search rate had fallen from 1.4x slower than a-tree to 6.9x slower -
a real, disproportionate blowup, not a fixed constant-factor gap.

**Root cause was architectural, not a bug.** `PSTDynamic::insertSubscription()` attached a
subscription to *every leaf* its access predicate's range covered - correct and necessary under the
paper's own design, since `MatchEvent`'s O(1) per-event lookup only works if every leaf already
knows every predicate covering it, with no further search at match time. For an equality predicate
this is one leaf; for `trade_price > X` (30% of this benchmark's generated subscriptions have this
as their *only* predicate, so it's forced to be the access predicate) it was every leaf from X to
the domain maximum - roughly half the leaves in that dimension's tree, on average. As more
`trade_price > X` subscriptions inserted, the tree partitioned ever more finely, so each new
wide-range insertion touched proportionally more, smaller leaves - quadratic-leaning insertion cost
that grows with subscription-set density, exactly what Section 2.3's own selectivity ranking
(`{=} > {∈} > {in} > {<,≤,>,≥} > ...`) implicitly warns about: PS-Tree's design assumes an access
predicate is *usually* narrow, and has no defense when a subscription's only available predicate is
a wide, unbounded comparison - a very common real subscription shape ("alert me when price exceeds
$X").

**Fixed** (`mrayva/pstree@54a66dc` - see that repo's own README for the full design): the
leaf-materialization approach for range operators was replaced with canonical ancestor markers (the
classical "canonical decomposition" technique for stabbing queries, generalized to the value
encoding's existing digit-trie), attaching a range predicate to O(depth) ancestor nodes - a small,
per-attribute-type constant (16 for int64/double) independent of K - instead of O(leaves-covered).

**Results, after the insert-side fix** (before the search-side dedup fix described further below),
extended to K=50,000 to confirm the insert fix holds well past the original scale:

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 1.10 ms     | 906,468 subs/s  | 225.31 ms    | 88,768 events/s  | 5,286,377   |
| 1,000  | betree | 3.15 ms     | 317,525 subs/s  | 400.74 ms    | 49,907 events/s  | 5,286,377   |
| 1,000  | pstree | 4.20 ms     | 238,284 subs/s  | 334.73 ms    | 59,750 events/s  | 5,286,377   |
| 5,000  | atree  | 9.00 ms     | 555,424 subs/s  | 1,320.73 ms  | 15,143 events/s  | 27,180,339  |
| 5,000  | betree | 46.03 ms    | 108,635 subs/s  | 2,532.58 ms  | 7,897 events/s   | 27,180,339  |
| 5,000  | pstree | 24.45 ms    | 204,484 subs/s  | 6,206.57 ms  | 3,222 events/s   | 27,180,339  |
| 10,000 | atree  | 32.30 ms    | 309,571 subs/s  | 2,837.66 ms  | 7,048 events/s   | 54,413,004  |
| 10,000 | betree | 220.03 ms   | 45,448 subs/s   | 5,704.79 ms  | 3,506 events/s   | 54,413,004  |
| 10,000 | pstree | 46.62 ms    | 214,481 subs/s  | 19,548.38 ms | 1,023 events/s   | 54,413,004  |
| 50,000 | atree  | 760.74 ms   | 65,725 subs/s   | 14,903.86 ms | 1,342 events/s   | 271,572,457 |
| 50,000 | betree | 7,895.31 ms | 6,333 subs/s    | 59,806.84 ms | 334 events/s     | 271,572,457 |
| 50,000 | pstree | 253.78 ms   | 197,019 subs/s  | 371,186.76 ms| 54 events/s      | 271,572,457 |

Match counts still agree exactly across all three engines at every K, confirming the redesign
preserved correctness while changing the complexity.

**Insert is completely fixed - a clean, unambiguous win.** pstree's insert rate is now flat across
the entire K=1,000-50,000 range (roughly 200-240k subs/s, no downward trend at all) instead of
falling from 69k to 4k subs/s as K grew - exactly the O(depth), K-independent behavior the redesign
targeted. At K=10,000, insert improved **52x** (4,108 -> 214,481 subs/s); at K=50,000 pstree is now
the **fastest** of the three engines to insert (197k subs/s vs. atree's 66k and betree's 6.3k -
betree's own insert cost scales badly with K here too, for unrelated reasons not investigated as
part of this work).

**Search initially looked unfixed, and at K=50,000 appeared to degrade faster than either
competitor.** The first re-benchmark after the insert-side fix showed search throughput essentially
unchanged from before it (e.g. 1,023 vs. 1,018 events/s at K=10,000) and, at K=50,000, growing **19x**
slower for only a 5x increase in K - worse than either competitor. At the time this was written up as
a real but only-partially-understood cost, hypothesized (by code inspection, *not* confirmed via
profiling) to be `PSTDynamic`'s dimension-signature Bloom-filter grouping losing precision as the
insert-side fix spread each wide predicate's coverage across more, smaller buckets. That hypothesis
was never actually profiled, and turned out to be looking in the wrong place entirely.

**The real cause, found via `perf record`/`perf annotate` on `matching_engine_bench` at K=20,000:
an O(n^2) dedup loop in `nats_sidecar`'s own wrapper, not anywhere in pstree.**
`pstree_matching_engine::search()` (`src/matching_engine.cpp`) maps each of PSTDynamic's matched DNF
clause ids back to the caller's subscription id and de-duplicates repeat ids (a subscription with an
`OR` becomes multiple PSTDynamic "subscriptions" internally, any number of which can match the same
event) using `std::find(result.begin(), result.end(), subId)` - a linear scan of the
already-deduplicated output vector, run once per raw clause match. For an adversarial workload where
a large, K-proportional fraction of subscriptions genuinely match every event (thousands of matches
per event at K=50,000), this is O(matches) work per match, i.e. **O(matches^2) per event** - and the
profile confirmed it directly: 66.8% of *all* search-phase self-time at K=20,000 was inside this one
function's own tight compare-and-branch loop (`perf annotate` shows the hot instructions are a bare
`cmp`/`je` pair, the disassembled shape of `std::find` over a `vector<uint64_t>`), dwarfing
`PSTDynamic::matchEvent()` itself (7.1% self-time) and everything else in pstree combined. This bug
predates the canonical-ancestor-marker redesign entirely - it is orthogonal to it, just invisible
until the insert-side fix stopped masking it. **Fixed** by replacing the `std::find`/`vector` scan
with a reused `unordered_set<uint64_t>` scratch member (O(1) average dedup, cleared per call instead
of reallocated) - output order and content are unchanged (dedup still keeps first-seen order), only
the algorithmic complexity of finding it changes.

**Results, after the dedup fix:**

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 0.95 ms     | 1,057,650 subs/s | 221.45 ms    | 90,315 events/s | 5,286,377   |
| 1,000  | betree | 3.13 ms     | 319,331 subs/s   | 416.29 ms    | 48,044 events/s | 5,286,377   |
| 1,000  | pstree | 3.84 ms     | 260,636 subs/s   | 286.87 ms    | 69,719 events/s | 5,286,377   |
| 5,000  | atree  | 9.04 ms     | 553,388 subs/s   | 1,317.95 ms  | 15,175 events/s | 27,180,339  |
| 5,000  | betree | 41.79 ms    | 119,632 subs/s   | 2,546.14 ms  | 7,855 events/s  | 27,180,339  |
| 5,000  | pstree | 21.17 ms    | 236,180 subs/s   | 2,787.63 ms  | 7,175 events/s  | 27,180,339  |
| 10,000 | atree  | 32.97 ms    | 303,304 subs/s   | 2,845.64 ms  | 7,028 events/s  | 54,413,004  |
| 10,000 | betree | 215.73 ms   | 46,355 subs/s    | 5,803.07 ms  | 3,446 events/s  | 54,413,004  |
| 10,000 | pstree | 51.17 ms    | 195,444 subs/s   | 6,487.35 ms  | 3,083 events/s  | 54,413,004  |
| 50,000 | atree  | 759.92 ms   | 65,796 subs/s    | 15,075.71 ms | 1,327 events/s  | 271,572,457 |
| 50,000 | betree | 8,383.14 ms | 5,964 subs/s     | 83,111.28 ms | 241 events/s    | 271,572,457 |
| 50,000 | pstree | 315.66 ms   | 158,398 subs/s   | 74,776.45 ms | 267 events/s    | 271,572,457 |

Match counts still agree exactly across all three engines at every K, and insert numbers are
unchanged (this fix is search-only) - confirming the dedup fix is a pure performance change, not a
behavioral one.

**Search improved substantially, and the "worse than either competitor" result reverses.** pstree's
search time dropped **1.2x-5.0x** across the K range (largest gain at the largest K: 371,186.76 ms ->
74,776.45 ms at K=50,000). More importantly, pstree is no longer uniformly the worst of the three: at
K=1,000 it's already faster than be-tree (69,719 vs. 48,044 events/s), and at K=50,000 it's faster
than be-tree again (267 vs. 241 events/s) - it's only in the K=5,000-10,000 middle range that be-tree
edges it out slightly (within ~1.1x). The un-profiled Bloom-filter-fragmentation hypothesis from the
earlier writeup is retracted as an explanation for the K=50,000 blowup (fully accounted for by the
dedup bug above).

**A second real bottleneck found the same way, immediately after the dedup fix - profiling did not
stop at "good enough."** Re-profiling `matching_engine_bench` at K=20,000/50,000 with the dedup fix
in place showed `PSTDynamic::matchEvent()`'s own self-time still dominated by one thing:
`subscriptions_.at(id)` - an `unordered_map` hash lookup, run once per raw candidate match, inside
the innermost matching loop (`pst_dynamic.hpp`). `perf annotate` attributed ~63% of `matchEvent`'s
self-time to this single call at a many-thousand-subscription K. **Fixed** (`pstree@3033469`):
`LeafGroupState::groups` now stores a `const Subscription*` directly alongside each id, captured
once at `InsertSubscription` time, instead of just the id - eliminating the lookup from the hot loop
entirely. Safe because `unordered_map` never invalidates references/pointers to an element except by
erasing that exact element, and `DeleteSubscription` always removes an id (and its pointer) from
every group before erasing the subscription itself. Full 8-test pstree suite green, normal and
ASan+UBSan+leak-detection.

**Results, after both the dedup fix and the `unordered_map::at()` elimination:**

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 0.95 ms     | 1,057,521 subs/s | 221.48 ms    | 90,302 events/s | 5,286,377   |
| 1,000  | betree | 3.28 ms     | 305,220 subs/s   | 393.44 ms    | 50,833 events/s | 5,286,377   |
| 1,000  | pstree | 3.93 ms     | 254,315 subs/s   | 248.62 ms    | 80,444 events/s | 5,286,377   |
| 5,000  | atree  | 9.46 ms     | 528,497 subs/s   | 1,353.10 ms  | 14,781 events/s | 27,180,339  |
| 5,000  | betree | 45.67 ms    | 109,490 subs/s   | 2,471.47 ms  | 8,092 events/s  | 27,180,339  |
| 5,000  | pstree | 19.53 ms    | 255,980 subs/s   | 1,986.40 ms  | 10,068 events/s | 27,180,339  |
| 10,000 | atree  | 32.67 ms    | 306,111 subs/s   | 2,860.87 ms  | 6,991 events/s  | 54,413,004  |
| 10,000 | betree | 197.54 ms   | 50,623 subs/s    | 5,383.31 ms  | 3,715 events/s  | 54,413,004  |
| 10,000 | pstree | 35.76 ms    | 279,606 subs/s   | 4,762.44 ms  | 4,200 events/s  | 54,413,004  |
| 50,000 | atree  | 751.95 ms   | 66,494 subs/s    | 14,547.29 ms | 1,375 events/s  | 271,572,457 |
| 50,000 | betree | 7,832.73 ms | 6,383 subs/s     | 61,575.89 ms | 325 events/s    | 271,572,457 |
| 50,000 | pstree | 197.25 ms   | 253,491 subs/s   | 34,806.38 ms | 575 events/s    | 271,572,457 |

Match counts still agree exactly across all three engines at every K; insert numbers are consistent
with earlier runs (search-only fix) - only run-to-run noise, no behavioral change.

**pstree now beats be-tree at every tested K, not just the extremes**, and the gap to a-tree has
roughly halved: pstree search is 1.1x-2.4x behind a-tree (was 1.3x-5.0x before this fix) and 1.1x-1.8x
*ahead* of be-tree at every K (was: behind be-tree in the K=5,000-10,000 middle). pstree's own residual
super-linearity also improved: 140x time growth for the same 51x match-volume increase (K=1,000-50,000),
down from 260x, and now genuinely *better* than be-tree's own super-linearity (156x) at the same
comparison - no longer the worst-scaling of the three on this metric.

A third cost was identified via the same profiling pass: `pstree_matching_engine::search()`'s own
remaining self-time was still dominated (~71%) by two per-match `unordered_map` operations -
`m_clause_to_sub.find()` (translating a DNF clause id back to the caller's subscription id) and
`m_seen_scratch.insert()` (dedup) - both real hashmap costs, no longer *quadratic* ones, but still
paid on every one of thousands of matches per event.

**User pushed back that "competitive with be-tree, behind a-tree" wasn't good enough for a redesign
meant to demonstrate a modern technique - explicit ask to keep improving. Both operations above turn
out to be provably unnecessary for the common case, not just expensive.** A subscription with no `OR`
in its expression (a single DNF clause - ~90% of this benchmark's generated subscriptions) can, by
construction, appear **at most once** in `matchEvent`'s raw results for any one event: PS-Tree's
canonical-decomposition disjointness (the same property `test_between_exhaustive_small_domain` and
the complexity regression test already verify) guarantees a single predicate's own bucket is visited
at most once per query point. If it can never repeat, it needs no dedup; if there's only ever one
clause, there's nothing to translate.

**Fixed** (`nats_sidecar@5f7b911`): reserved the top bit of the `uint64_t` id space
(`kSyntheticIdBit`). A single-clause subscription whose caller-supplied id doesn't already use that
bit is inserted into `PSTDynamic` under its own real id directly, skipping `m_clause_to_sub`
entirely; `search()` recognizes the clear bit on a returned match and skips both the translation
lookup and the dedup check. Multi-clause (`OR`) subscriptions, and the vanishingly unlikely case of a
caller id that already has the reserved bit set, fall back to the original synthetic-id +
`m_clause_to_sub` path unchanged - full correctness preserved for every case, the fast path only
*adds* a shortcut for the common one. `sidecar_test` passes under both normal and ASan+UBSan builds.

**Results, after all three fixes (dedup, `unordered_map::at()` elimination, and the direct-id
shortcut):**

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 0.93 ms     | 1,079,697 subs/s | 222.65 ms    | 89,828 events/s  | 5,286,377   |
| 1,000  | betree | 3.13 ms     | 319,005 subs/s   | 398.42 ms    | 50,198 events/s  | 5,286,377   |
| 1,000  | pstree | 3.85 ms     | 259,773 subs/s   | 146.82 ms    | 136,222 events/s | 5,286,377   |
| 5,000  | atree  | 9.14 ms     | 547,023 subs/s   | 1,331.62 ms  | 15,019 events/s  | 27,180,339  |
| 5,000  | betree | 49.70 ms    | 100,602 subs/s   | 2,515.25 ms  | 7,951 events/s   | 27,180,339  |
| 5,000  | pstree | 21.44 ms    | 233,186 subs/s   | 1,264.03 ms  | 15,822 events/s  | 27,180,339  |
| 10,000 | atree  | 31.52 ms    | 317,261 subs/s   | 2,816.87 ms  | 7,100 events/s   | 54,413,004  |
| 10,000 | betree | 209.24 ms   | 47,791 subs/s    | 5,509.93 ms  | 3,630 events/s   | 54,413,004  |
| 10,000 | pstree | 35.88 ms    | 278,712 subs/s   | 2,947.15 ms  | 6,786 events/s   | 54,413,004  |
| 50,000 | atree  | 759.91 ms   | 65,797 subs/s    | 14,765.47 ms | 1,355 events/s   | 271,572,457 |
| 50,000 | betree | 8,415.70 ms | 5,941 subs/s     | 65,414.31 ms | 306 events/s     | 271,572,457 |
| 50,000 | pstree | 245.00 ms   | 204,081 subs/s   | 18,992.54 ms | 1,053 events/s   | 271,572,457 |

Match counts still agree exactly across all three engines at every K.

**pstree now beats a-tree outright at K=1,000 and K=5,000, and closes to within 1.05x-1.29x at
K=10,000/50,000** (down from 1.1x-2.4x after the previous fix, and 1.3x-5.0x before any of the three).
At K=1,000: 136,222 vs. a-tree's 89,828 events/s - pstree is **1.5x faster than a-tree**. At K=5,000:
15,822 vs. 15,019 - still ahead. Only at K=10,000 (6,786 vs. 7,100, 1.05x behind) and K=50,000 (1,053
vs. 1,355, 1.29x behind) does a-tree retake the lead, by a real but now-modest margin. pstree remains
comfortably ahead of be-tree throughout (2.7x at K=1,000, narrowing to 3.4x - *widening* - at
K=50,000, since be-tree's own search cost keeps degrading faster than either competitor's). Search
improved a further 1.6x-1.8x on top of the previous two fixes (largest relative gain at K=1,000:
248.62ms -> 146.82ms).

**Total improvement across all three fixes, from the original (unfixed) search regression**: at
K=10,000, search went from 19,637.96 ms to 2,947.15 ms - a **6.7x** improvement. At K=50,000, from
371,186.76 ms to 18,992.54 ms - a **19.5x** improvement.

A fourth cost was identified via the same profiling pass: with all three fixes above in place,
`matchEvent`'s own remaining self-time was ~86% (per `perf annotate`) a cache-miss-dominated pointer
chase - `subPtr -> Subscription::predicates.data() -> predicates[0].attr` - three genuinely different,
independently-heap-allocated locations (the `Subscription` sits in `subscriptions_`'s hashmap node
storage; its `predicates` vector is a separate allocation) touched for every candidate, with no
relationship to iteration order across different candidates.

**User explicitly pushed further still: "let's not leave any stone unturned - we need to try to beat
a-tree throughout," with the condition that anything risky gets extra test coverage and nothing gets
committed unless it demonstrably works end-to-end first.** This one is a genuine data-layout change
(not a bug fix), so it got treated accordingly:

**Fixed** (`pstree@c90e778`): added `PredicateList`, a hand-specialized small-vector for
`SubPredicate` with inline storage for up to 4 elements (covers realistic subscription shapes - this
benchmark's own generator produces 1-2), falling back to a heap buffer with standard doubling growth
only when a subscription genuinely has more. `Subscription::predicates` is now `PredicateList`
instead of `std::vector<SubPredicate>`; `Subscription` remains a plain aggregate (`PredicateList`'s
converting constructors from `std::vector<SubPredicate>` and `std::initializer_list<SubPredicate>`
preserve every existing aggregate-init call site, in both repos, unchanged). Deliberately
hand-specialized rather than a reusable template - a narrower, fully-reasoned-about surface carries
less risk than a general-purpose one.

Given the manual placement-new/destroy bookkeeping this kind of type requires, it got the extra
scrutiny the risk warranted: a new dedicated test file (`tests/test_predicate_list.cpp`) covering
growth past inline capacity checked at every step, copy/move independence and self-assignment safety
for both inline and heap-spilled sizes, `at()` bounds checking, and a mixed copy/move stress loop - on
top of the existing indirect coverage via `test_predicate.cpp`/`test_pst_dynamic*.cpp`, all of which
passed completely unmodified (including `test_pst_dynamic_stress.cpp`'s random-vs-brute-force oracle,
the strongest signal PSTDynamic's contract was unaffected). Crucially, the whole change was validated
against `nats_sidecar` - full `sidecar_test` suite under both normal and ASan+UBSan builds, plus a
live benchmark re-run - via a local `FETCHCONTENT_SOURCE_DIR_PSTREE` override, *before* committing
anything in either repo, per the "if it doesn't work, don't commit" instruction.

**Results, after all four fixes:**

| K | engine | insert | insert rate | search | search rate | total matches |
|---:|---|---:|---:|---:|---:|---:|
| 1,000  | atree  | 0.96 ms     | 1,041,292 subs/s | 224.32 ms    | 89,160 events/s  | 5,286,377   |
| 1,000  | betree | 3.16 ms     | 316,071 subs/s   | 414.20 ms    | 48,285 events/s  | 5,286,377   |
| 1,000  | pstree | 3.71 ms     | 269,285 subs/s   | 148.29 ms    | 134,870 events/s | 5,286,377   |
| 5,000  | atree  | 10.01 ms    | 499,667 subs/s   | 1,362.07 ms  | 14,684 events/s  | 27,180,339  |
| 5,000  | betree | 44.01 ms    | 113,607 subs/s   | 2,543.97 ms  | 7,862 events/s   | 27,180,339  |
| 5,000  | pstree | 17.23 ms    | 290,109 subs/s   | 1,125.55 ms  | 17,769 events/s  | 27,180,339  |
| 10,000 | atree  | 33.13 ms    | 301,805 subs/s   | 2,833.12 ms  | 7,059 events/s   | 54,413,004  |
| 10,000 | betree | 215.10 ms   | 46,490 subs/s    | 5,602.87 ms  | 3,570 events/s   | 54,413,004  |
| 10,000 | pstree | 34.68 ms    | 288,314 subs/s   | 2,671.22 ms  | 7,487 events/s   | 54,413,004  |
| 50,000 | atree  | 761.08 ms   | 65,696 subs/s    | 15,078.69 ms | 1,326 events/s   | 271,572,457 |
| 50,000 | betree | 8,627.80 ms | 5,795 subs/s     | 61,913.98 ms | 323 events/s     | 271,572,457 |
| 50,000 | pstree | 198.73 ms   | 251,592 subs/s   | 17,469.55 ms | 1,145 events/s   | 271,572,457 |

Match counts still agree exactly across all three engines at every K.

**pstree now beats a-tree outright at K=1,000, K=5,000, AND K=10,000** - not just the two smallest K
values as after the previous fix. At K=1,000: 134,870 vs. 89,160 events/s (1.51x faster). At K=5,000:
17,769 vs. 14,684 (1.21x faster). At K=10,000: 7,487 vs. 7,059 (**1.06x faster** - this K crossed over
from 1.05x *behind* to slightly *ahead*). Only at K=50,000 does a-tree keep the lead, and even there
the gap narrowed further: 1,326 vs. 1,145 events/s, **1.16x behind** (down from 1.29x after the
previous fix, 2.4x two fixes ago, 5.0x before any of the four). pstree remains far ahead of be-tree at
every K (2.8x-3.4x). This fix's own relative gain grew with K rather than shrinking (1.01x at
K=1,000 - within run-to-run noise - up to 1.12x at K=5,000/10,000 and 1.09x at K=50,000), consistent
with cache-miss cost mattering more as more distinct candidate subscriptions get touched per event.

**Total improvement across all four fixes, from the original (unfixed) search regression**: at
K=10,000, search went from 19,637.96 ms to 2,671.22 ms - a **7.4x** improvement. At K=50,000, from
371,186.76 ms to 17,469.55 ms - a **21.2x** improvement.

**Conclusion**: the insert-side scaling issue and four independent, real search-side bottlenecks (the
O(n^2) dedup bug, the per-match hashmap lookup inside `matchEvent`, the wrapper's own
translation+dedup overhead, and the Subscription/predicate storage cache-miss) are now fixed and
verified across a 50x range of K. pstree's insert is unambiguously the fastest of the three engines at
large K; its search beats be-tree outright at every tested K and now beats a-tree too at every K
except the largest, where it trails by a real but modest 1.16x - a complete turnaround from "uniformly
worse than both" at the start of this investigation. `engine: pstree` is now the strongest default
choice among the three for this workload shape, not one narrowly justified only by churn-heavy
workloads. The remaining a-tree gap at K=50,000 is a well-characterized, much smaller residual than
where this investigation started - a-tree's own evaluator remains more mature, and closing the rest
would mean chasing progressively smaller, progressively more specific costs for a shrinking return.

## The real-world check: a K=1 fleet test tells the opposite story, and why

Everything above is a synthetic, in-process benchmark with K=1,000-50,000 *subscriptions*. The actual
motivating real-world use case for this project (see "Operational note: cycling a whole table through
core-mode connections" above) is different: N=12 `nats_sidecar` instances, a real 115,020,848-row NYSE
trade table published through them, and typically **one or a few** live content filters, not thousands.
That's K=1, not K=50,000 - worth checking directly rather than assuming the synthetic benchmark's
result generalizes.

**It doesn't.** Running the exact N=12 fleet methodology from the "Operational note" section above
(one `price > 500.0` subscription, `pgnats`'s `--batch-size 500 --batch-encoding native --workers`
4/5/6, randomized cell order to avoid the run-order confound already documented in this file) found
be-tree zero-loss at every level, a-tree zero-loss except a small real loss at workers=6, and **pstree
losing real throughput at every level, worse than either competitor, and worse as load increased** -
reproduced across two independent randomized trials (average loss 2.78% / 6.08% / 13.93% at
pub-workers 4/5/6). The opposite ranking from the synthetic benchmark above.

**Root cause, found via `perf record --call-graph dwarf` on one fleet instance under real load**
(frame-pointer-based `-g` unwinding gave garbled, misattributed samples on this asio/coroutine-heavy
binary - `--call-graph dwarf` is required here, matching this project's own earlier profiling
lesson): pstree pays real, consistent per-row costs a-tree/be-tree structurally never pay -
`pstree::detail::encodeValue`/`DoubleCodec::encode` (~8-12% of CPU, encoding every event's value into
a 16-element `ElementKey` before any matching can happen at all - PS-Tree's whole indexing design
requires this on every single event, unlike a-tree/be-tree's direct native-value comparison),
`pstree_event_sink::with_float()` (~9-11%), and `PSTDynamic::matchEvent`'s own tree-walk/group-lookup
(~5-8%). This is the flip side of the synthetic benchmark's own story: the indexing machinery that
makes PS-Tree fast at high K is pure, unamortized overhead at K=1, where a-tree/be-tree's "just
compare the value" simplicity wins outright - same architecture, opposite verdict, depending entirely
on subscription count.

**Fixed the independently-actionable part** (`pstree@eaa0f19`): `ElementKey` was a plain
`std::vector<uint16_t>` - a genuine per-row heap allocation. Replaced with a small-vector-optimized
type (inline storage for up to 16 elements, covering int64/double/bool with zero heap allocation;
only `StringCodec`'s longer keys still spill to the heap, unchanged). This doesn't change the
fundamental architectural tradeoff (encoding itself still costs more than no encoding at all), but it
removes one concrete, measured, unforced cost on top of it.

**Re-verified with a third randomized N=12 trial using the fixed binary**: all three pstree cells
(pub-workers 4/5/6) showed **0% loss** - a complete elimination, not a partial improvement, versus the
2.78%/6.08%/13.93% average across the two prior unfixed trials. One trial, not yet independently
reconfirmed the way the "before" numbers were (two trials) - given the established run-to-run noise in
this exact test, a single result should be treated as a strong, mechanistically-explained signal, not
final proof. Match counts and publish rates in this trial were consistent with the prior two (not an
easier run), and the elimination was unanimous across all three tested load levels rather than a
partial win at just one - both make "this was just noise" a less likely explanation than "the fix
worked," but a fourth confirming trial would close the gap between "very likely" and "certain."

**Practical upshot**: the synthetic high-K benchmark and this real low-K fleet test are not in tension
- they're measuring genuinely different regimes of the same architecture, and both are real. For a
deployment with many independent subscriptions, pstree is now the strongest engine of the three (see
above). For a deployment with few subscriptions and high per-row throughput requirements - the shape
this project's own NYSE full-table-cycling use case actually has - pstree's real-world standing is
still being established; this fix closed the one clearly avoidable gap found so far, and a-tree/be-tree
were already profiled here too (see below) without turning up comparably clear-cut opportunities.

## Corrected fastest-full-table-push numbers, and why N=12 was never actually required

**The numbers above (and every historical rows/s figure earlier in this document) understate the
fleet's real throughput** - discovered by asking a simple question: could the same 115M-row dataset
be pushed without Postgres's own overhead diluting the measurement? `nats_publish_from_sql.py`'s
per-run "materialize" step (`materialize_partitioned()`) does a real `CREATE UNLOGGED TABLE ... AS
SELECT` (a full physical copy of the whole row set into a throwaway table) + `COUNT(*)`, then drops
it - repeated on *every single invocation*, even though the source data never changes between
benchmark runs. Measured directly: **~28 seconds** for this exact table, on every cycle.

**Fix**: `--source-table NAME` (new, added to `nats_publish_from_sql.py`) skips this entirely -
ctid-partitions an already-built persistent table directly (one cheap `COUNT(*)`, no write, no
drop). Build the snapshot once:

```sql
CREATE UNLOGGED TABLE nyse_bench_snapshot_price_exchange AS
  SELECT "Trade Price" AS price, "Exchange" AS exchange FROM nyse_eqy_us_all_trade_20260102;
```

then pass `--source-table nyse_bench_snapshot_price_exchange` instead of `--sql '...'` on every
subsequent run. Mutually exclusive with `--sql`, parallel-path only (`--workers > 1`), incompatible
with `--limit`.

**Result: not just cleaner numbers - a ~3x correction.** Same exact config as the K=1 fleet test
above (N=12, `worker_threads: 1`, `engine: pstree`, `pub_workers=16`): the old `--sql`-based
measurement gave ~3.06M rows/s; the clean `--source-table` measurement, three trials, gave a mean of
**9,560,360 rows/s** - every prior throughput figure in this whole investigation was diluted by
~25-30s of unrelated Postgres table-copy overhead per cycle, not just noisy around the right answer.

**Re-swept both axes clean, and found the fleet needs half the instances this project has always
assumed.** `pub_workers` at fixed N=12 (4/8/16/24/32/48/64): all zero-loss, but throughput plateaus
sharply from `pub_workers=24` onward (~11.1-11.4M rows/s flat to 64) - since `dropped=0` even at 64,
**this ceiling belongs to the publisher side, not the fleet** (see below). Re-swept N at that real
~11.4M rows/s rate instead of the old, much lower one - and finally found the fleet's true
threshold:

| N | loss | processed rate |
|---:|---:|---:|
| 12 | 0.0% | 11.5M rows/s |
| 10 | 0.0% | 10.2M rows/s |
| 8  | 0.0% | 11.6M rows/s |
| 6  | 0.0% | 11.5M rows/s |
| 4  | **16.4%** | 9.1M rows/s |
| 3  | 35.5% | 7.5M rows/s |
| 2  | 54.9% | 5.3M rows/s |
| 1  | 77.6% | 2.7M rows/s |

Sharp threshold between N=6 (zero loss) and N=4 (real loss) - the same "sharp, not gradual" pattern
every other threshold in this document shows. **Corrected recommendation: N=6, `worker_threads: 1`,
`engine: pstree`, `pub_workers=24`, publishing via `--source-table`** - zero loss, ~11.4M rows/s,
the entire 115M-row table through the fleet in under 10 seconds. N=6 is *half* the instance count
this whole project has defaulted to since its earliest benchmarks (originally chosen to match this
host's 12 physical cores) - the fleet was never actually tested against its own real ceiling before,
only against artificially low publish rates.

**The `pub_workers=24+` plateau is Postgres/pgnats-side, not the fleet, and it's already
well-understood.** Isolating the publish side entirely (no sidecar fleet running) pushed the same
`pub_workers=64` config to 13.15M rows/s - higher than with the fleet running concurrently,
confirming the fleet and Postgres backends were themselves competing for the same 12 physical
cores. `sudo perf record -a` (system-wide, during an isolated `pub_workers=64` run) showed 80.36%
of the ENTIRE host's CPU in `postgres` processes and another 10.99% in `tokio-rt-worker` threads
(pgnats's own async-nats runtime) - ~91% combined. Root cause: each Postgres backend connection runs
its own thread-local tokio runtime (`pgnats/src/ctx.rs`); a prior fix (already in this codebase, not
new) already cut that from ~25 default worker threads per backend down to a fixed `worker_threads(2)`
after finding the same oversubscription problem this document's own worker_threads sweep found on
the sidecar side (400 OS threads competing for 24 cores at 16 backends x 25 threads). At
`pub_workers=64` even with that fix, 64 backends x 3 threads (1 main + 2 tokio) still oversubscribes
24 logical cores - matching the plateau's own onset almost exactly at `pub_workers=24`. Not
currently worth chasing further: the fleet only needs ~11.4M rows/s at N=6, comfortably under what
the publisher can already deliver.

## Smallest fleet size: down to N=1, once the real bottleneck (queue capacity, not threads) was found

Following on directly from N=6 above: the natural next question was how much further the instance
count could go, on the same 115,020,848-row NYSE publish (`engine: pstree`, `pub_workers=24`,
`--source-table nyse_bench_snapshot_price_exchange`, msgpack format, columnar batches of 500 rows,
`price > 500.0` subscription). Two real findings came out of this pass, both worth internalizing
independently of the final numbers: **thread count was the wrong knob**, and **the obvious rate
metric can lie to you**.

**First attempt - sweeping `--workers` (worker_threads) at N=4 - failed.** N=4 (16.4% loss in the
table above) never reached zero loss at any `--workers` from 2 to 5; the closest was `--workers 4`
at 0.17% loss, and `--workers 5` was *worse* than 4 - the same non-monotonic signature the
worker_threads-scaling investigation earlier in this document already proved (via `perf`+`taskset`)
to be CPU oversubscription, not something more threads fixes.

**The real bottleneck: per-instance input-queue capacity.** A control run confirmed N=4/
`worker_threads=1` at base.yaml's own long-standing defaults (`input_queue_max_messages: 10000`,
`input_queue_max_bytes: 67108864` = 64MiB) drops 16.3% - matching the original finding almost
exactly, regardless of thread count. Bumping the queue 10x (100,000 messages / 640MiB) at
`worker_threads=1` - no other change - fully drains, reproducibly, at ~11.0-11.5M rows/s. The
bottleneck was backpressure (`worker_pool::enqueue_impl` dropping input once the bounded queue
fills), not matching throughput - the entire earlier worker_threads sweep was tuning the wrong
lever.

**Pushed down from there, N=3 → N=2 → N=1**, holding the queue bump fixed and adjusting only
`--workers`:

| N | worker_threads | dropped | rows/s |
|---:|---:|---:|---:|
| 4 | 1 | 0% | ~11.0-11.5M |
| 3 | 2 | 0% | ~10.6-11.5M |
| 2 | 1 | 0% | ~11.5-12.4M |
| 1 | 3 | 0% | ~14.4-14.9M |

None of these needed more than a handful of `worker_threads` - the earlier assumption that fewer
instances need proportionally more threads did **not** hold cleanly (N=2 at `worker_threads=1`
outran N=3 at `worker_threads=1`, which was rate-short at 9.85M and needed `worker_threads=2` to
clear 10M/s) - a real, unexplained inconsistency, not smoothed over here. N=1 - a single instance,
no fleet at all - needed `worker_threads=3` and (initially) a much bigger queue than N=2-4, since
one instance there absorbs the *entire* 230,276-batch volume alone with no NATS queue-group
load-balancing to split it.

**A real methodology trap, caught at N=1: the obvious rate metric can be wrong.** Every number
elsewhere in this document (`rows_published / publish_wall_clock_seconds`) is the *publisher's*
speed, not necessarily the sidecar's - it only works as a stand-in when the sidecar keeps pace with
the publisher in real time. A big enough input queue lets an instance report `dropped=0` by
absorbing the entire burst and quietly draining the backlog *after* the publisher already finished
- which makes that proxy read as a clean pass even when the instance never actually sustained
10M rows/s in real time. Caught this by checking each instance's own periodic `stats:` log lines for
stretches where `queue_depth` stayed nonzero throughout (genuinely backlogged, not idle-then-catch-
up) and computing rows/s from the received-count delta across exactly that stretch - a true lower
bound on sustained throughput, immune to end-of-run buffering. Re-verified N=2 this way (confirmed
genuine, ~11.5-12.4M sustained) and used it to validate every N=1 trial from the start, since N=1's
whole passing configuration depends on a large queue that would otherwise make this exact trap easy
to fall into.

**Queue size has a real cost (RAM) and shouldn't just be "big enough to pass."** The queue sizes
above were initially reused across N values rather than individually right-sized. Once actually
measured against each configuration's own observed peak `queue_depth` (with a ~15-20% margin, not
shaved to the exact peak):

| N | worker_threads | input_queue_max_messages | input_queue_max_bytes | observed peak queue_depth |
|---:|---:|---:|---:|---:|
| 4 | 1 | 25,000 | 150MB | 17,249-21,099 |
| 3 | 2 | 25,000 | 150MB | 18,442-20,545 |
| 2 | 1 | 80,000 | 500MB | 66,000-69,600 |
| 1 | 3 | 165,000 | 1,073,741,824 (1GiB) | 139,710-147,565 |

Every one of these shrank once measured individually - N=4/N=3 down to 150MB from a reused 640MiB,
N=1 down to 1GiB from a reused 2.5GiB. N=2's peak backlog (66k-69.6k) being *larger* than N=3/N=4's
(17k-21k) despite fewer instances splitting the load is the same non-monotonic pattern as the
worker_threads inconsistency above - noted, not explained.

**Final answer: a single instance (N=1) is sufficient** - `engine: pstree`, `worker_threads: 3`,
`input_queue_max_messages: 165000`, `input_queue_max_bytes: 1073741824`, `pub_workers=24` - fully
drains the entire 115M-row table and truly sustains ~14.4-14.9M rows/s, confirmed reproducibly. This
is now `base.yaml`'s own default configuration in this benchmark harness. If you want redundancy
(more than one process, so a single crash doesn't drop the whole fleet) rather than the bare
minimum instance count, N=2 at `worker_threads: 1` with an 80,000-message/500MB queue is the next
cheapest point that still clears both bars - the table above has the full picture for any N you
might actually want to run instead of the literal minimum.

**Retested at N=1 with Arrow input instead of msgpack (output still msgpack, per Arrow's own
read-only/different-output_format requirement) - the same settings carried over with no
adjustment, and came out faster.** Same `worker_threads: 3`, same `input_queue_max_messages: 165000`
/ `input_queue_max_bytes: 1073741824`, same `pub_workers=24`, only `format: arrow` +
`input_columnar: true` + `output_format: msgpack` changed: dropped=0 both trials, true sustained
rate **~15.6-16.5M rows/s** - higher than msgpack input's ~14.4-14.9M at the identical settings.
Plausibly Arrow's zero-copy columnar read (no per-row decode into an intermediate structure, unlike
msgpack's native decode) being cheaper per row - not profiled to confirm that's the actual
mechanism, just the observed direction of the effect.

## K-scaling investigation: pstree matching cost at K=8000+, and how far two real fixes got

The matching-engine benchmark above (`## Matching-engine benchmark`) measures small-to-moderate K.
Scaling the same 100%-degenerate `price > X` shape up to K=8000+ subscriptions exposed a real,
substantial matching-cost blowup - `avg_match_us` at K=8000 started at **76.6µs** (roughly
500x the K=1 baseline elsewhere in this document), driving true sustained throughput (see the
"obvious rate metric can lie to you" methodology above - `queue_depth`-continuous-window,
received-count-delta measurement, not naive wall-clock rate) far below the original 10M rows/s
target this whole benchmark suite otherwise reaches easily at low K.

**Ruled out first:** more fleet instances (every instance holds the *full* subscription set - only
row *volume* is load-balanced via the data-plane `queue_group`, never the subscription count -
confirmed by reading `sidecar.cpp`'s subscribe calls directly: the control-plane subscription has
no queue_group at all); swapping to atree/betree (both degrade on the identical workload, not
pstree-specific); and a more realistic mixed subscription shape alone (40% eq / 30% range / 20% AND
/ 10% OR cut `avg_match_us` only ~27%, 76.6µs → 56.1-56.3µs - real, insufficient on its own).

**Phase 3 - stop re-verifying a subscription's own access predicate at match time**
(pstree@85d40ea, this repo@3cd5cea/@3fe6cea): `PSTree::matchPoint()`'s own tree walk already
*exactly* proves a subscription's access predicate for every operator that gets a real,
value-specific tree placement (`kEq/kElemOf/kBetween/kLt/kLe/kGt/kGe` - confirmed against
`buildLowLevel`'s own switch; `kNe/kNotElemOf/kIsNotNull` get a "matches every leaf" catch-all
instead and must still be re-checked in full). Skipping the now-redundant re-check: **3.1x** at the
100%-degenerate K=8000 shape (76.6µs → ~24.6-24.8µs), **1.6x** at the mixed shape. Real, but still
80-115x short of both the original 10M rows/s target and any relaxed bar.

**Phase 4 - inline the hot per-candidate fields into `LeafGroupState::groups`**
(pstree@55dbd2b, this repo@ceb026b): a follow-up `perf annotate --symbol=PSTDynamic::matchEvent`
profile on a live K=8000 fleet trial found the *new* bottleneck after Phase 3 was a single
instruction - the read of `subPtr->accIdxSkippable` - eating 80%+ of the function's self-time. Not
compute: `subPtr` is a pointer chasing into `subscriptions_` (an `unordered_map`), whose entries sit
scattered across the heap unrelated to a leaf's own candidate-vector order - a cache miss per
candidate. Inlining `accIdx`/`accIdxSkippable`/`onlyPredicateIsAccess` directly into each group
entry (so the common case - a single-predicate subscription whose sole predicate is the access
predicate, exactly the degenerate benchmark shape - never dereferences `subPtr` at all) took
`avg_match_us` from ~24.6-24.8µs to a measured, fully-drained **9.79µs** (another ~2.5x), and true
sustained throughput from ~256 rows/s to **~620-635 rows/s** at K=8000 - a real ~2.4x improvement
that tracks almost exactly with the `avg_match_us` reduction. (This fast path is specific to
single-predicate subscriptions; multi-predicate subscriptions in the mixed-shape workload still pay
the `subPtr` dereference, so Phase 4's real-world win there is expected to be smaller than at the
100%-degenerate shape - not separately re-measured.)

**The honest bottom line:** two real, verified, independently-shipped fixes took K=8000's
`avg_match_us` from 76.6µs down to 9.79µs (~7.8x cumulative) and true sustained throughput from
whatever the original pre-Phase-3 rate was up to ~620-635 rows/s. That is still roughly **1,600x
short of a relaxed 1M rows/s bar**, and further still from the original 10M rows/s target - K=8000
single-attribute-degenerate subscriptions is a genuinely hard workload for this matching-engine
design (every one of 8,000 near-identical wide-range predicates lands in largely-overlapping tree
buckets, so group sizes - and thus per-event candidate-scan cost - stay large no matter how cheap
each individual candidate check gets). K=16000/K=32000 were not re-run after Phase 4 since K=8000
was not "meaningfully closer" to either target - a larger K would only look worse, not better.
Neither fix regressed anything: full sanitizer suites (plain, ASan+UBSan, ThreadSanitizer) stayed
green on both repos throughout, and pstree's own randomized differential stress-test oracle
confirmed byte-identical match results before and after each change.

**A more realistic subscription shape closes most of the gap on its own.** The 100%-degenerate
shape above assumes near-total overlap (many independent wide-range thresholds, log-uniform over
the full value domain - deliberately pathological, not representative). Re-tested instead with each
subscription's own selectivity constrained to a realistic 1%-5% of the dataset (`price > X`
thresholds drawn from the REAL price distribution's 95th-99th percentile via
`gen_price_threshold_subs_realistic.py`, K~5400 after the subscription registry's own identical-
expression dedup - see `subscription_manager::subscribe()` - collapsed some of the 8000 requested
thresholds down): `avg_match_us` dropped to **0.578µs**, and true sustained throughput rose to
roughly **5,400-8,200 rows/s** across trials (see the multi-trial table below) - closing the gap to
a relaxed 1M rows/s bar from ~1,600x down to roughly ~125-185x.

**Phase 5 - eliminate `search()`'s own redundant copy for the common (no-OR) case**
(nats_sidecar@a3abbe2): a live `perf record -a -g` profile of the realistic-selectivity trial found
the picture had shifted again now that matching-tree cost itself was cheap - `nats-server`'s own CPU
share (26.65% of total system time, upstream message routing/GC, out of this repo's control) became
a first-order cost, and **`sidecar::pstree_matching_engine::search()` itself (18.27%) started
costing MORE than `PSTDynamic::matchEvent()` (14.86%)** - the opposite of every earlier profile in
this investigation. `search()`'s own translate/dedup loop (`src/matching_engine.cpp`) was building a
second `result` vector that, for any workload with zero OR'd/multi-clause subscriptions (true of
every shape tested throughout this whole investigation), ends up byte-for-byte identical to
`matchEvent()`'s own already-correct return value - a provably redundant full copy over the entire
match set. Fixed by tracking (a monotonic `m_hasSyntheticClauses` flag, set once any subscription
ever needs clause-id translation) whether that copy can ever be necessary, and skipping it entirely
when it can't.

**Honestly, this fix's real-world impact was not measurable above this host's own run-to-run
noise.** Four PRE-fix trials of the identical realistic-selectivity benchmark (run back-to-back,
same binary, same config) already spanned 5,441-8,199 rows/s on their own (mean ~7,171) - a ~51%
spread from nothing but ordinary variance on this shared host. Two POST-fix trials measured
6,264-7,286 rows/s (mean ~6,775) - entirely within the pre-fix range, not distinguishably better
*or* worse. The fix is real and provably correct (verified: full `sidecar_test` suite green under
plain/ASan+UBSan/ThreadSanitizer, two new targeted tests confirming byte-identical results with and
without the fast path, `search()`'s own isolated cost genuinely eliminated per the profiler) - but
its actual contribution to end-to-end throughput here is smaller than this benchmark's own noise
floor can resolve, not a confirmed win. This is the same honest category of result this
investigation has already hit more than once elsewhere (see pstree's own project history) - reported
as such rather than oversold.

**`nats-server`'s own 26.65% CPU share is comparable in size to the sidecar's own remaining
matching-tree cost (37.82%)** - genuinely out-of-repo (upstream NATS server internals: subject-
routing `Sublist.match`, protocol parsing, Go GC), not pursued further.

## What subscription selectivity would it take to reach 1M rows/s? A controlled answer.

The question this whole investigation was ultimately trying to answer: given a target throughput
(1M rows/s), how selective (`s` = matched-fraction⁻¹, i.e. one subscription matching 1/s of the
dataset) would subscriptions need to be? Answering this rigorously needed something that didn't
exist yet: `avg_match_us` only ever timed matching - a single end-to-end throughput number couldn't
be decomposed into "would shrink with more selective subscriptions" vs. "a fixed floor regardless of
selectivity."

**Added `avg_fanout_us`** (`worker_pool.cpp`/`.hpp`, `sidecar.cpp`): times the synchronous,
CPU-bound work between `matching_engine::search()` returning and the publish coroutine being handed
to `asio::co_spawn()` - resolving `output_subjects` and estimating publish size for a row that
matched. Deliberately excludes the async publish coroutine itself (frame serialization is
interleaved with `co_await write_raw()`/backpressure waits - timing that whole thing would mix real
CPU cost with network I/O wait time in one misleading number, the same trap this document's own
"true sustained rate" methodology already avoids elsewhere) and excludes rows dropped by
backpressure (never actually fanned out). Verified via new targeted tests
(`tracks_fanout_time_when_message_is_published`,
`columnar_fanout_time_count_reflects_matching_row_count_not_total_row_count`) and the full
`sidecar_test` suite under plain/ASan+UBSan/ThreadSanitizer.

**Controlled sweep**: five trials at fixed schema/harness, each with `price > X` thresholds drawn
from the real price distribution via narrow percentile bands (`gen_price_threshold_subs_realistic.py`
`--lo-percentile`/`--hi-percentile`) centered at target selectivities `s` ≈ 20, 100, 1000, 10000,
100000. (Real subscription count `K` after the registry's own identical-expression dedup varied
768-3553 across trials - a real confound worth a cleaner re-run if this gets revisited, not
controlled for here.)

| target `s` | K (actual) | matches/event | `avg_match_us`* | `avg_fanout_us` | throughput (rows/s) |
|---:|---:|---:|---:|---:|---:|
| 20 | 3553 | 25.2 | 0.39µs | 22.38µs | 8,179 (not fully drained) |
| 100 | 768 | 5.24 | 0.13µs | 4.43µs | 11,514 |
| 1,000 | 2369 | 0.523 | 0.11µs | 31.64µs | 15,351 |
| 10,000 | 2958 | 0.052 | 0.10µs | 28.22µs | 11,510 |
| 100,000 | 3088 | 0.0052 | 0.10µs | 39.51µs | 11,513 |

*`avg_match_us` has since been removed (see "Stats output" above) - kept in this historical table
for the record, but do not trust its absolute value: a direct `perf` profile of this same regime
found it ~100-140x too low (see the correction below).

**Throughput plateaus at ~11,500-15,000 rows/s from `s`≈100 onward and never gets anywhere close
to 1M rows/s** - even at `s`=100,000, where only 0.5% of events match *anything*, throughput is no
better than at `s`=100. That finding is solid, confirmed by direct wall-clock measurement, not
dependent on `avg_match_us`/`avg_fanout_us` at all.

**What's *not* solid: the original claim here that matching+fan-out together explained "under 0.1%"
of the ~195-260µs per-event budget.** A follow-up `perf record`/`perf report` profile of this exact
regime found `PSTDynamic::matchEvent`+`PSTree::matchPoint`+`encodeValue`+`search()` together
consuming **~19% of real CPU cycles** - not negligible, contradicting the `avg_match_us`-based
estimate above by roughly 100-140x. That discrepancy is *why* `avg_match_us` was removed (see
"Stats output"): its 1-in-8-row timing sample was both inaccurate and, on its own, a real ~7.6% CPU
cost. The honest breakdown of that ~195-260µs floor, per direct `perf` measurement: ~50% deserialize/
decode (`zerialize::mp_skip`, string construction, `populate_event`, `match_columnar_batch`), ~19%
matching, the remainder split across allocation, hashing, and (before this fix) the timing
instrumentation itself.

**This still reframes the original question, just with the real numbers.** Subscription
selectivity stops being the lever past `s`≈100 - but deserialize/decode cost, not an unmeasured
mystery floor, is the largest real remaining contributor (~50%), with matching a real second
(~19%), not the "under 0.1%" this section originally claimed. Closing the gap to 1M rows/s would
mean optimizing the deserialize/populate path first, matching cost second - both real, `perf`-
measured targets now, not unmeasured territory. Not pursued past this point without new direction.

**Deserialize/decode fix - cache the redundant double `mp_skip` in zerialize's msgpack map/array
iterators** (`mrayva/zerialize@01144c9`, this repo's own pin bump `f386f8b`): reading the code
behind the ~50% deserialize/decode share above found `MsgPackDeserializer::KeysView`/
`EntriesView`/`ElementsView`'s own iterators called `mp_skip` in `operator*()` to learn the current
element's byte size, then called it *again* in `operator++()` to recompute that identical size just
to advance the offset - a real, avoidable 2x redundancy on the single most common access pattern
(dereference-then-increment), which `ColumnarRows::advance()` (called once per row) drives directly
through `ElementsView::iterator`. Fixed by caching `operator*()`'s computed size for `operator++()`
to reuse (still correct if a caller increments without ever dereferencing first). Verified: `perf`
on the same K~3088 regime before/after - `mp_skip`'s own total share dropped **19.41% → 13.34%**,
and `advance()`'s own slice of it dropped **8.24% → 5.34%** (~35% relative reduction), exactly the
targeted mechanism, confirmed by direct measurement rather than inferred. Full `sidecar_test` suite
green (plain/ASan+UBSan/ThreadSanitizer) and `zerialize`'s own test suite green throughout.

**End-to-end throughput impact: real per the profiler, but not cleanly separable from this
benchmark's own ceiling.** Two pre-fix and two post-fix trials of the identical K~3088 regime:
pre-fix spanned 10,948-15,352 rows/s, post-fix landed at 15,351-15,352 rows/s both times - at the
very top of the pre-fix range, not clearly above it. Two of the *pre-fix* trials independently hit
the exact same ~15,352 rows/s ceiling the post-fix trials did, which looks like a publisher-side
limit (`pub_workers=24`, `batch_size=500`) rather than a sidecar-processing one - once the sidecar
is fast enough to keep pace with the publisher in real time, making the sidecar faster still
doesn't raise measured throughput, because the publisher becomes the binding constraint instead.
This is the same honest category of result Phase 5 already hit: a real, `perf`-confirmed win in
isolation, whose contribution to *this specific benchmark's* end-to-end number is masked by a
different bottleneck at the fast end. Not chased further - the mechanism (real, targeted, verified)
is sound regardless of whether this particular publisher setup can expose it end to end.

**Confirmed directly: the publisher, not the sidecar, was the ~15,352 rows/s ceiling.**
`sidecar_pipeline_bench` (`benchmarks/sidecar_pipeline_bench.cpp`, gated behind
`SIDECAR_BUILD_BENCHMARKS`) drives the exact same production pipeline
(`deserialize_and_match_columnar` -> `populate_event` -> `matching_engine::search()` -> fan-out
resolution, via real `worker_pool::enqueue()`) with zero NATS I/O, zero Postgres, and no external
publisher process - real columnar msgpack payloads built in-process, a synthetic uniform `price`
distribution with an exact analytic threshold per target selectivity `s` (no percentile queries
needed), `io_context::run()` on its own dedicated thread (not a sleep-based poll loop, which would
impose its own artificial ceiling). Result, at the same K~3000-8000 / `s`=20-100000 shapes this
whole investigation has used throughout:

| K | `s` | matched (of 230,500 rows) | `avg_fanout_us` | true rows/s |
|---:|---:|---:|---:|---:|
| 3000 | 100,000 | 6 | 262.27µs | 17,041,032 |
| 3000 | 20 | 12,056 | 25.17µs | 1,158,126 |
| 8000 | 1,000 | 230 | 1,437.94µs | 1,795,402 |

**1.1-17 million rows/s, two to three orders of magnitude above the combined system's
publisher-bound ~15,352 rows/s** - decisive, direct confirmation that the sidecar itself was never
the bottleneck in any real-fleet trial this investigation ran at these K/`s` shapes; the external
publisher (`pub_workers=24`) was. This resolves the open question from the `mp_skip` fix's own
verification section and gives a clean, reusable, publish-independent ceiling to measure any future
sidecar-side fix against, instead of a real-fleet trial that can silently be measuring the
publisher's own speed instead (the same "obvious rate metric can lie to you" trap this document's
own "true sustained rate" methodology exists to catch - this benchmark sidesteps it by construction,
having no publisher to be limited by at all).

**Extended: Arrow input (default), and an optional real NATS *core* publish path.**
`sidecar_pipeline_bench` now defaults to `input_format=arrow` - exercising `ArrowColumnarRows`
(the other real production input-decode path, alongside `ColumnarRows<MsgPackDeserializer>`) with
msgpack as the required republish encoding (Arrow has no single-row encoder of its own) - `msgpack`
remains available and behaves identically to before. A new `publish=real` option (default: `fake`,
unchanged) connects to a real local NATS core server instead of the zero-I/O fake connection, so
real NATS write I/O for the output side can be measured without reintroducing the external-
publisher/Postgres confound this benchmark exists to remove - plain PUB/`write_raw`, no JetStream
consumer or KV bucket, nothing to clean up server-side. **First single-pair comparison (K=3000,
`s`=20) looked like a real ~6% cost for genuine NATS socket writes (`fake` ~1,893,393 rows/s vs.
`real` ~1,779,639 rows/s) - re-checked with 3 repeats each and retracted at that specific point**:
`fake` alone spans 1,469,960-1,912,476 rows/s run-to-run (mean ~1,734,598), `real` spans
1,633,205-1,877,342 (mean ~1,735,979) - at `s`=20-32 the two distributions overlap almost
completely and the means are effectively identical. The original "6%" was noise from a single
unrepeated pair at that one point, not a real effect there.

**But a full `s`=2^t (t=0..16) sweep of `fake` vs. `real` (same K=8000, 2-3 repeats each, arrow
input) shows the real-vs-fake gap is highly `s`-dependent, and at several points it is very real
and large - not noise:**

| t | `s` | matched | `fake` avg rows/s | `real` avg rows/s | real/fake |
|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 230,500 | 144,310 | 97,960 | 68% |
| 1 | 2 | 120,846 | 299,855 | 285,723 | 95% |
| 2 | 4 | 60,404 | 798,197 | 590,217 | 74% |
| 3 | 8 | 30,190 | 1,232,517 | 1,149,382 | 93% |
| 4 | 16 | 15,049 | 1,621,928 | 1,531,258 | 94% |
| 5 | 32 | 7,427 | 1,222,064 | 1,221,057 | 100% |
| 6 | 64 | 3,732 | 884,394 | 942,099 | 107% |
| 7 | 128 | 1,855 | 1,069,999 | 347,805 | **33%** |
| 8 | 256 | 954 | 1,316,629 | 438,226 | **33%** |
| 9 | 512 | 463 | 1,735,088 | 632,545 | **36%** |
| 10 | 1,024 | 228 | 3,057,081 | 2,134,340 | 70% |
| 11 | 2,048 | 109 | 5,282,050 | 2,102,119 | **40%** |
| 12 | 4,096 | 54 | 8,162,243 | 5,231,927 | 64% |
| 13 | 8,192 | 34 | 11,270,265 | 9,304,979 | 83% |
| 14 | 16,384 | 17 | 14,594,185 | 13,128,719 | 90% |
| 15 | 32,768 | 10 | 15,469,209 | 14,898,715 | 96% |
| 16 | 65,536 | 6 | 17,188,037 | 14,900,652 | 87% |

**`s`=128-512 in particular drops to real-publish throughput just 33-36% of the fake-connection
number - confirmed 3x independently (not the earlier single-pair mistake).** `avg_fanout_us` at
these points is itself 2-3x higher under `real` than `fake` (e.g. `s`=128: 1,017µs real vs. 368µs
fake) - genuine NATS-server-side cost (protocol parsing, subject routing, Go GC - see this
document's own earlier `perf` breakdown of `nats-server`'s share) leaking back into the sidecar's
own timed fan-out-resolution window, likely via connection backpressure (`wait_for_drain()`) once
enough PUB frames are in flight. Why the effect is small at `s`=20-64 and `s`=8,192+ but large in
the `s`=128-2,048 band specifically is not yet understood - this may be the same unexplained
mechanism behind the `fake`-only sweep's own `s`=16-128 dip (both regions overlap), or a separate
effect; not chased further here. The honest correction to the earlier retraction: "no detectable
cost from real NATS-core publish" was true *at the specific point tested* (`s`=20), not a general
finding - the fuller sweep shows the real cost is real, large, and `s`-dependent.

Arrow-input throughput at the default shape (K=3000, `s`=100,000) came in close to msgpack's own
number (~18.4M vs. ~17.7M rows/s) - both comfortably in the same "nowhere near the bottleneck"
territory.

**The selectivity sweep, redone with the publish-independent harness, extended down to `s`=1.**
The original K-scaling investigation's own selectivity sweep (`s`=20 to `s`=100,000) ran on the
combined real-fleet system - actual K varied 768-3553 due to run-to-run subscription dedup, and
throughput was confounded by the publisher-bound ~15,352 rows/s ceiling this whole section exists
to explain away. Redone with `sidecar_pipeline_bench` at a fixed requested K=8000 (arrow input,
fake publish, 2 repeats per point), swept as `s`=2^t for t=0..16 - the full range from `s`=1
(every subscription matches essentially every row) to `s`=65,536:

| t | `s` | K (actual) | matched (of 230,500) | avg rows/s | range across repeats |
|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 8000 | 230,500 | 144,310 | 142,474 - 146,146 |
| 1 | 2 | 8000 | 120,846 | 299,855 | 288,729 - 310,982 |
| 2 | 4 | 8000 | 60,404 | 798,197 | 767,161 - 829,234 |
| 3 | 8 | 8000 | 30,190 | 1,232,517 | 1,116,125 - 1,348,908 |
| 4 | 16 | 8000 | 15,049 | 1,621,928 | 1,556,252 - 1,687,603 |
| 5 | 32 | 8000 | 7,427 | 1,222,064 | 1,151,601 - 1,292,527 |
| 6 | 64 | 8000 | 3,732 | 884,394 | 865,269 - 903,519 |
| 7 | 128 | 8000 | 1,855 | 1,069,999 | 1,036,140 - 1,103,857 |
| 8 | 256 | 7,998 | 954 | 1,316,629 | 1,290,911 - 1,342,348 |
| 9 | 512 | 7,998 | 463 | 1,735,088 | 1,645,878 - 1,824,299 |
| 10 | 1,024 | 7,994 | 228 | 3,057,081 | 3,057,006 - 3,057,155 |
| 11 | 2,048 | 7,991 | 109 | 5,282,050 | 5,033,438 - 5,530,662 |
| 12 | 4,096 | 7,986 | 54 | 8,162,243 | 8,020,803 - 8,303,684 |
| 13 | 8,192 | 7,965 | 34 | 11,270,265 | 10,607,270 - 11,933,261 |
| 14 | 16,384 | 7,953 | 17 | 14,594,185 | 14,448,430 - 14,739,941 |
| 15 | 32,768 | 7,894 | 10 | 15,469,209 | 15,109,615 - 15,828,802 |
| 16 | 65,536 | 7,783 | 6 | 17,188,037 | 16,946,760 - 17,429,314 |

**A real, reproducible dip, not just a single anomalous point.** Throughput climbs monotonically
from `s`=1 to `s`=16 (144K -> 1.62M rows/s, exactly as intuition predicts - fewer matches, less
fan-out work, more throughput), then genuinely **dips** from `s`=16 down through `s`=64 (1.62M ->
1.22M -> 884K rows/s - each point's own repeat range is tight and non-overlapping with its
neighbors, so this is a real valley, not noise), before resuming a monotonic climb all the way to
`s`=65,536. The original 5-point sweep's own isolated `s`=100 dip (previous version of this
section) was this same valley, just under-sampled - the full 17-point sweep shows its actual
shape: a real trough spanning roughly `s`=16-128, not one anomalous value. The likely mechanism
(not fully confirmed): `avg_fanout_us` is a *per-matching-row* average, but the
`output_subjects`-resolution work it times runs once per *batch* (`worker_pool.cpp`) - a batch with
few matching rows divides whatever fixed per-batch overhead exists across fewer rows than a batch
with many, inflating the per-row average without necessarily meaning more real work happened; why
this specific effect would produce a *valley* rather than a monotonic trend as matches/batch keeps
shrinking is not yet explained. The raw wall-clock/throughput numbers above are unaffected by this
averaging question either way and are what to trust directly - noted honestly as a real, open
question, not smoothed over, matching this whole investigation's own standing practice.

## A "hot symbol" that wasn't a real win: `to_string` at extreme fan-out, verified and reverted

Re-profiling `sidecar_pipeline_bench` at `s`=1 (K=8000, near-total overlap - `published` shows
average fan-out is actually ~986 matched subscriptions/row here, not the full 8000 initially
assumed) surfaced `std::to_string(unsigned long)` as a standalone hot symbol (11.5-17.4% self-time
across two profiling runs) - traced to two call sites: `subscription_manager::output_subject()`
rebuilding `output_prefix + "." + to_string(id)` on every call (deduped per-batch, not per-row, but
still up to K times per batch), and `worker_pool.cpp`'s `append_pub_frame()` recomputing
`to_string(payload.size())` on *every* matched subscription for a row even though the payload (and
its size) is identical across all of them.

Both looked like unambiguous wins: cache `output_subject`'s result once at subscribe()/restore()
time instead of rebuilding it per call, and hoist `to_string(payload.size())` out of the
per-subscription loop into a once-per-row local. Both were implemented, and both passed the full
`sidecar_test` suite (plain/ASan+UBSan/ThreadSanitizer) with the existing exact-wire-output tests
unchanged. A follow-up `perf` profile confirmed `to_string` disappeared entirely from the hot list.

**But the throughput measurement told a different story.** A first interleaved A/B/A/B wall-clock
comparison (3 pairs) showed a consistent ~5-6% *regression*, not a win. Isolating each change
separately (stashing one source file at a time to build BEFORE/FIX1-ONLY/FIX2-ONLY/BOTH binaries)
and a follow-up wall-clock re-test came back murkier - inconsistent, sometimes near-parity - so the
call was settled with `perf stat -e cycles:u` (a hardware counter, immune to the sampling/dwarf-skid
concerns and much less sensitive to this shared host's own scheduling noise than wall-clock): 5
strictly-interleaved BEFORE/AFTER pairs, same command, same inputs. AFTER used *more* total cycles
than BEFORE in **5 out of 5** pairs (+0.36% to +1.37%, mean ~+0.9%) - small, but completely
one-sided. Instruction count *did* drop as expected (confirming the redundant computation really
was eliminated), but IPC dropped too (2.87 vs. a tight 2.98-3.02 for BEFORE, 3-for-3 no overlap) -
consistent with trading a cheap, register/stack-only computation (both `to_string` calls only ever
needed inputs already resident: `m_output_prefix`/`id`, or `payload.size()`) for an extra random
memory access into `subscription_info` (a fairly large struct - `expression` string +
`unordered_set<string> lease_holders` + the new cached field - sitting in a scattered
`unordered_map` node, likely cache-cold for any given subscription id at K=8000). The eliminated
"waste" was real but apparently cheap enough (short-string-optimized, register-bound) that removing
it didn't pay for the cache-miss cost of fetching it from a colder cache line instead.

**Reverted both changes** (`subscription_manager.cpp`/`.hpp`, `worker_pool.cpp`) rather than ship an
"optimization" that measured as a net-neutral-to-slightly-negative change. Kept one harmless,
genuinely useful byproduct: `sidecar_pipeline_bench` now also prints `published` (total pub frames
written), which is what revealed the real average fan-out at `s`=1 in the first place and remains
useful for any future investigation at this end of the sweep. This is the same "verify before
trusting a perf-profile hypothesis" discipline this document has needed more than once already
(the `avg_match_us` sampling-bias removal, the retracted-then-corrected real-vs-fake publish
finding) - a hot symbol in a sampling profile is a lead, not a proof, and the fix it suggests still
has to earn its keep against a real, repeated, low-noise measurement before shipping.

**A second attempt, same result: a faster `std::to_string` algorithm didn't help either.**
Reasoning that the caching attempt's failure was about *where* the value was fetched from
(cache-cold memory) rather than the `std::to_string` computation itself being cheap, a follow-up
tried swapping the algorithm in place instead: `jeaiii::to_text_from_integer`
(https://github.com/jeaiii/itoa, MIT) writing digits directly into the destination buffer at the
exact same two call sites, same call frequency, no restructuring - a `simditoa/benchmarks` run
showed it as the fastest realistic contender for short (1-8 digit) integers specifically, well
ahead of `std::to_chars`/`fmt`/SIMD converters at that end of the range. Vendored via
`FetchContent`, full `sidecar_test` suite green (plain/ASan+UBSan/ThreadSanitizer, byte-identical
wire output). The same `perf stat -e cycles:u` interleaved methodology - 8 pairs this time, not 5 -
came back just as one-sided: **8 out of 8** pairs used *more* cycles with jeaiii, +0.42% to +9.00%
(mean ~+3.5%, noisier and larger than the caching attempt's regression, not smaller). Reverted
(CMake FetchContent block, `src/fast_itoa.hpp`, both call sites) rather than carry an unused
dependency for a change that didn't pay off.

The likely explanation: modern libstdc++ (GCC 13/14, what this host and CI both build with) already
implements `std::to_string` for integers via a table-driven fast path broadly similar in spirit to
jeaiii's own approach, not the `snprintf`-based implementation older libstdc++ versions used - so
there was less headroom to win back than the external benchmark (comparing against `std::to_chars`,
not `std::to_string`, and on different hardware) suggested. Two independent, rigorously-verified
attempts at this exact hot symbol - one via caching, one via a faster algorithm - both came back
negative. Per this project's own standing pattern elsewhere (see pstree's own hot-path
investigation history), that's a reasonable point to stop chasing this specific symbol rather than
try a third approach.

**What `PSTDynamic::matchEvent()` itself actually spends time on, once inside it:** a `perf
annotate` source-line breakdown (RelWithDebInfo build, same `s`=1/K=8000 benchmark) attributes
**~82% of matchEvent's own time to two adjacent lines** - the capacity check
(`if (this->_M_impl._M_finish != this->_M_impl._M_end_of_storage)`, 43.3%) and pointer increment
(`++this->_M_impl._M_finish;`, 38.5%) inside `std::vector<uint64_t>::push_back()`'s inline fast
path, appending each matched subscription id to the result vector - not the boolean-expression
evaluation logic itself. The `GroupEntry` fast-path checks from Phase 4
(`entry.accIdxSkippable && entry.onlyPredicateIsAccess`) account for another ~7.8%. This points at
a concrete, not-yet-tried next lead: the result vector isn't reserve()'d ahead of the match loop, so
at high fan-out (hundreds to low thousands of matches per row at this end of the sweep) it likely
reallocates and regrows repeatedly - pre-reserving based on a size hint could turn this dominant
cost into a plain, branch-free pointer bump. Not yet attempted or measured; noted here as the next
candidate, not a finding in its own right - given how the last two "obvious" fixes on this same
function turned out under real measurement, it would need the same rigor before trusting it.

**Third attempt at the `std::to_string` hotspot, and this one actually worked.** The first two
attempts (caching, then a faster itoa algorithm) both kept the same underlying shape: build the
output string via several separate `+=`/`append()` calls, each paying its own capacity-check/grow
overhead - the exact same class of cost the matchEvent finding above just identified in
`std::vector`, just on `std::string` instead. A third attempt targeted that shape directly instead
of the integer conversion: `fmt::format("{}.{}", m_output_prefix, id)`
(`subscription_manager.cpp::output_subject()`) and `fmt::format_to(std::back_inserter(wire), "PUB {} {}\r\n", subject, payload.size())`
(`worker_pool.cpp::append_pub_frame()`) each replace 4-5 separate append calls with one `fmt` call
that composes the whole piece in a single pass - `fmt` was already a linked dependency, no new
FetchContent needed. Full `sidecar_test` suite green (plain/ASan+UBSan/ThreadSanitizer), byte-
identical wire output (existing exact-wire-format tests unchanged).

This time `perf stat -e cycles:u` came back one-sided in the *right* direction: **8/8** interleaved
pairs at `s`=1/K=8000 used *fewer* cycles (-1.2% to -5.65%, mean ~-3.5%), and **4/4** pairs at a
more realistic `s`=100/K=8000 did even better (-12.1% to -13.3%, mean ~-12.5% - larger, not smaller,
at the more realistic selectivity, since fan-out/string-building work is a bigger fraction of
total per-row cost there than at `s`=1's matching-dominated extreme). Committed
(`subscription_manager.cpp`, `worker_pool.cpp`). The distinguishing factor across all three
attempts: fixing *what kind of operation* was redundant (many small appends instead of one
composed write) paid off; fixing *which algorithm* computed an individual value did not - a useful
data point for the next hot-symbol chase on this codebase.

**The `matchEvent()` vector-reserve lead paid off too.** Restructured into two passes over the
same tree walk (`matchPoint()` itself is not repeated - doubling that real tree traversal would
cost more than the `push_back()`s it saves): a first pass collects every candidate group pointer
plus a running upper bound on the total candidate count (`ids.size()`, already known per group, no
extra work), then the result vector is `reserve()`'d to that bound before the real per-candidate
check runs, so it never reallocates mid-scan (pstree@3093b3f, pin bumped here). Verified: pstree's
own full suite green (plain, ASan+UBSan, and a manual ThreadSanitizer run of its concurrent stress
test), `sidecar_test` green under all three configs against the bumped pin, output unchanged
(candidateGroups preserves original iteration order).

Benchmarked the same way as the two reverted `to_string` attempts, at three selectivity points
instead of one this time, since a fix that only pays off at an extreme, unrealistic selectivity
wouldn't be worth much on its own: **8/8** pairs at `s`=1/K=8000 (near-total overlap) mean **-9.1%**
fewer cycles, `s`=20 (the high end of this project's own realistic 1/20-1/100 selectivity range)
mean **-3.9%** (3/4 pairs favorable, noisier), and `s`=100 (the low end of that range) essentially a
wash at **-0.3%** (2/4 favorable, 2/4 not - within noise). A real, fan-out-scaling win: substantial
at extreme overlap, a real but smaller win across the realistic range, never a clear loss anywhere
tested. Unlike the two `to_string` attempts, this is the first of the three "obvious hot symbol"
leads from this investigation that held up as a genuine win under the same rigor that sank the
other two.

**A follow-up profile after both real fixes landed found `fmt`'s own machinery as the new
second-biggest cost.** With `memmove`/`to_string` gone and `matchEvent` reserve()'d, a fresh `perf`
profile at `s`=1/K=8000 (now 90,543 rows/s, up from the original 81,593) showed `matchEvent` at
41.3% and `fmt::detail::parse_format_string` + `copy_noinline` + `container_buffer::grow` +
`vformat_to` together at **~40%** - `fmt`'s own runtime format-string parsing and buffer-growth
machinery, replacing the eliminated costs almost proportionally. `parse_format_string` alone
(18.5%) suggested the literal `"PUB {} {}\r\n"`/`"{}.{}"` format strings were being re-parsed at
runtime on every call rather than at compile time.

Wrapping both format strings in `FMT_COMPILE(...)` (`<fmt/compile.h>`, already available - fmt
12.2.0 here) parses and checks them once at compile time instead. Full `sidecar_test` suite green
(plain/ASan+UBSan/ThreadSanitizer), byte-identical output. Measured impact was real but far more
modest than the symbol's raw 18.5% share suggested: **11/12** interleaved `perf stat -e cycles:u`
pairs across `s`=1 and `s`=20 favored `FMT_COMPILE` (mean -1.9% and -1.3% respectively) - a small,
consistent win, not the double-digit reduction a naive read of the flat profile would predict
(some of that 18.5% was very likely inlined formatting/copy work misattributed to
`parse_format_string`'s own debug-info symbol, a known hazard of reading `perf`'s flat percentages
too literally on heavily-templated, aggressively-inlined library code). `container_buffer::grow`
itself (6%) is untouched by this change - it's a buffer-growth cost, not a parsing one - and
remains an open, not-yet-tried lead if `fmt`'s own hot path gets revisited again.

**Pushing matchEvent's optimization further - resize()+index instead of reserve()+push_back() -
looked promising but didn't hold up.** A follow-up `perf annotate` after the `reserve()` fix found
push_back()'s own capacity-check-and-increment *still* ~82.8% of matchEvent's self-time - `reserve()`
eliminates reallocation but not the per-call branch, since `std::vector` has no "I already know
this fits" mode. The natural next step: `resize(idUpperBound)` up front, write through a plain
index (`matchingSubs[writeIdx++] = id`) instead of `push_back()`, then `resize(writeIdx)` down to
the real count - no capacity check anywhere in the hot loop. Verified: pstree's full suite green
(plain, ASan+UBSan, manual ThreadSanitizer), `sidecar_test` green under all three configs
(byte-identical match results, differential tests included).

Measured impact was far smaller than the 82.8% figure suggested, and mixed rather than a clean
win: at `s`=1/K=8000, only **5/8** interleaved `perf stat -e cycles:u` pairs favored it (mean
-1.2%, essentially noise-level); at the more realistic `s`=20, it was a clear **regression**,
**4/4** pairs worse (mean **+4.6%**). The likely mechanism: `resize()` zero-fills the *entire*
upper bound up front (cheap per byte, but the upper bound is a worst-case estimate - every
candidate matching - not the real count), while `push_back()` only ever touches as many elements
as actually match. At lower selectivity, more candidates get rejected by their predicate check, so
the gap between "upper bound sized" and "real match count" widens, and the upfront zero-fill cost
outgrows the per-element check it was meant to avoid. Reverted (pstree left at 3093b3f, the
`reserve()`+`push_back()` version - no pin change needed since nothing was committed past that
point). Filed alongside the two `to_string` reverts as the same lesson again: a hot symbol's raw
percentage in a sampling profile - even from `perf annotate`'s source-line view, not just flat
`perf report` - is a lead to verify, not a size estimate of the fix's payoff.

## Record width: does trimming non-predicate fields actually help? Yes, strongly - at most selectivities

A different theory from the ones above: instead of chasing a specific hot line, test whether the
*shape of the input record itself* matters - does publishing only the fields any subscription
could reference (`price`, `exchange`, `symbol`, `trade_volume`) outperform publishing a
realistically wide record that also carries fields no subscription ever touches?

`sidecar_pipeline_bench` gained a `record_shape` argument (`narrow` default, `wide`) and two new
always-present predicate fields (`symbol`, `trade_volume`, alongside the existing `price`/
`exchange`) to make this testable. `record_shape=wide` adds the same 11 non-predicate columns the
real NYSE trade fixture table has beyond those 4 (see `pgnats/scripts/nyse_trade_short_cols_view.sql`'s
own column list: `time`, `sale_condition`, `trade_stop_indicator`, `trade_correction_indicator`,
`sequence_number`, `trade_id`, `source_of_trade`, `trade_reporting_facility`,
`participant_timestamp`, `trf_timestamp`, `trade_through_exempt_indicator` - realistically-shaped
synthetic values, short codes for the indicator/id columns matching the real table's own 1-4
character values). Both msgpack and Arrow input paths support it. The mechanism this isolates:
`populate_event()`'s per-row key walk (`event_bridge.hpp`) has to parse/skip every column present
in the payload via `mapKeys()`'s `mp_skip`-driven iteration (`msgpack.hpp`) even for columns the
schema doesn't recognize (`schema.lookup()` returns nullopt, so it's a wasted walk, not a wasted
match) - confirmed directly by reading that code before running anything, not assumed. The payload
is also bigger, meaning more bytes `memmove`'d per matched subscription during fan-out
(`append_pub_frame`).

Verified via the same interleaved `perf stat -e cycles:u` methodology as every other finding in
this section, at K=8000 across three selectivities:

| `s` | pairs favoring narrow | mean effect |
|---|---|---|
| 1 | 8/8 | **+13.76%** more cycles for `wide` |
| 20 | 4/4 | **+22.79%** more cycles for `wide` |
| 100 | 2/8 | -1.55% (noise - no real effect either way) |

A real, strong, one-sided effect at `s`=1 and `s`=20 - the theory holds. `s`=100's near-zero,
mixed-direction result is reported honestly rather than smoothed over: `s`=100 sits inside the
already-documented, still-unexplained `s`≈16-128 throughput "valley" from earlier in this
investigation (see the selectivity-sweep section above) - plausible that whatever drives that
valley interacts with this effect in a way not chased down here, not evidence the theory is wrong
at low fan-out generally (`s`=20, arguably more representative of "low fan-out" than `s`=100 is,
shows the *largest* effect of the three points tested, consistent with per-row decode cost being a
bigger fraction of a smaller per-row total as matching work shrinks). Committed
(`benchmarks/sidecar_pipeline_bench.cpp`) - a genuinely actionable finding: production schemas
that only declare the attributes actually used in expressions, rather than mirroring an entire
upstream table, have a real, measurable throughput reason to do so, not just a tidiness one.

## "Push-style" matching: an isolated microbenchmark validated it, the real fusion opportunity didn't

Prompted by asking whether `matching_engine::search()` could return individual matched ids as
found (a SAX-style streaming callback) instead of collecting a complete `std::vector<uint64_t>`
first. The real change - touching the shared interface across all three engines, including
a-tree's Rust FFI boundary - was too large to attempt speculatively, so the underlying mechanism
was validated in isolation first: a throwaway microbenchmark compared "collect N ids into a
`reserve()`'d vector, then iterate once to do real per-id work" against "invoke an inlinable
template callback per id, doing the same work directly, no intermediate vector at all" (same
64MB-distractor cache-eviction methodology as the earlier `uint32_t` test). Checksums matched
(correctness), and the fused version won cleanly: **6/6** pairs at N=986 (mean **-8.16%**), **5/6**
at N=400 (mean **-9.17%**) - the first idea in this whole hot-path investigation to pass this cheap
validation stage, unlike the four before it.

That result was real, but specific to what it actually tested: eliminating an *expensive*
intermediate vector (standing in for `matchEvent`'s own `matchingSubs`, independently confirmed at
~82.8% of that function's self-time). The engine interface itself couldn't be touched without a
much larger redesign, so the real attempt scoped down to what `worker_pool.cpp` *could* safely
fuse on its own: the two-pass shape where `output_subjects` (a `sub_id -> subject string` dedup
map) is built on the worker thread, then the async publish coroutine separately re-`find()`s into
that same map once per match. Restructured so the worker-thread pass also records a resolved
`const std::string*` per (row, matched_id) - eliminating the coroutine's redundant lookup - full
`sidecar_test` suite green under all three configs (this touches cross-thread pointer lifetime, so
ThreadSanitizer mattered here specifically), byte-identical wire output.

Measured as a clear regression, growing worse at lower selectivity: **7/8** pairs unfavorable at
`s`=1/K=8000 (mean **+4.7%** more cycles), **4/4** at `s`=20 (mean **+13.2%**). Root cause,
understood after the fact: `output_subjects.find()`'s own cost had already shown as negligible
(under 0.3%) in every profile taken this session - it was never the expensive thing the
microbenchmark's "N" represented. The real change didn't eliminate `matchEvent`'s own vector (still
can't, without the engine-interface redesign) - it added a *new* one (the resolved-pointer list)
to save a lookup that was already cheap, and that new vector pays the same cache-cold-allocation
tax the whole hot-path investigation keeps running into. Reverted. The isolated test's positive
result stands as real and correctly diagnosed the general principle; it just wasn't reachable
without the bigger interface change this attempt deliberately avoided - a useful, concrete data
point for whether that larger redesign would be worth attempting for real.

## `subscription_manager`'s hash-map storage replaced with array-indexed storage - a real win, this time

A different lever on the same `output_subject()`/dedup hot path: `perf annotate`'s source-line
view of that region attributed the overwhelming majority of its cost to inlined libstdc++
hashtable machinery (`_M_key_equals`, bucket-chain walking, hashing) - the same class of cost as
every prior attempt here, but this time targeting the *lookup mechanism itself* rather than what
gets cached. `subscription_registry::resolve_id()` (`src/subscription_registry.cpp:147-165`)
confirmed subscription ids are dense, monotonic, permanent integers in real use (NATS JetStream KV
bucket revision numbers - entries are never rewritten), so a hash map isn't actually required for
`subscription_manager`'s own `m_subscriptions` storage - direct array indexing is available.

Validated in isolation first (same methodology as every other idea here): a microbenchmark
comparing a K=8000-entry `std::unordered_map` (persistent record store + a fresh per-batch dedup
map, matching the real two-lookup shape) against array indexing (a dense `std::vector` + a reused,
generation-counter-based per-batch array, avoiding both the hashing *and* the per-batch
reallocation) - **6/6 pairs favorable at both a `s`=1-like and `s`=20-like fan-out, mean -51.2%
and -54.3%** respectively. The strongest, cleanest isolated result of this whole investigation.

**Real implementation, scoped to `subscription_manager` only this round** (the per-batch
`output_subjects` map in `worker_pool.cpp` is a separate follow-up, see below): replaced the
single `std::unordered_map<uint64_t, subscription_info> m_subscriptions` with a hybrid design -
`m_subscriptions_by_id` (a `std::vector<std::optional<subscription_info>>`, indexed directly by
id) for ids below a 1-million cap, falling back to `m_subscriptions_overflow` (the original
`unordered_map`, functionally unchanged) above it. The cap exists because `restore()` is a public
API that must stay correct for an arbitrary caller-supplied `uint64_t`, not just the registry's own
dense sequence - confirmed by an existing test (`independent_instances_restoring_same_id_agree_on_output_topic`)
that deliberately restores a `~0x9F3A7B2...`-shaped id; a pure flat vector would have tried to
allocate ~1.4 exabytes for that one test and correctly threw instead. One important correctness
point worth stating explicitly: **ids are never reused for a different expression** even after a
subscription's slot is cleared - `subscription_registry`'s own permanence guarantee already commits
to "the same expression always resolves to the same id, forever," so recycling a dead slot's id
for something else would break that. A dead slot's *contents* (expression string, lease-holder set)
do get reclaimed via `.reset()`, exactly like `unordered_map::erase()` already did - `restore()`
repopulates a revived id from the caller's own persisted data, never from anything remembered
locally. Six call sites (`subscribe`/`restore`/`remove_lease`/`remove_subscription`/
`get_subscription`/`output_subject`, plus `rebuild_tree_locked`'s iteration) now share this split
via `find_locked()`/`insert_locked()`/`erase_locked()`/`for_each_locked()` instead of repeating the
array-vs-overflow branch six times over. Verified: full `sidecar_test` suite green under all three
configs (ThreadSanitizer specifically relevant here - concurrent readers/writers over the same
storage), byte-identical behavior including the huge-id edge case.

Measured on the real pipeline via `sidecar_pipeline_bench` + interleaved `perf stat -e cycles:u`:
**4/4 pairs favorable at `s`=20 (mean -18.6%)** - a real, clean win - but only **5/8 at `s`=1 (mean
-0.93%)**, mostly noise. Understood, not just observed: this implementation only replaced *one* of
the two hash-map lookups the isolated microbenchmark's win came from - `worker_pool.cpp`'s own
per-batch `output_subjects` dedup map (checked on *every* `(row, id)` pair, not just each id's
first occurrence within a batch) is still the original, untouched hash map, and at `s`=1's dense
repeat rate that's the far more frequently-hit half. `s`=1 is also the selectivity where this
document's own earlier finding - ~94% of matched rows get dropped by backpressure before ever
reaching the dedup loop at all - means dedup was already a small share of `s`=1's total cost
regardless of how much this change improved it. `s`=20 loses less to that and has a lower average
fan-out, so the array win shows through cleanly. Committed
(`src/subscription_manager.hpp`/`.cpp`) on the strength of the real `s`=20 result and full
correctness verification; the natural next step - applying the same array/generation-counter
technique to `worker_pool.cpp`'s own per-batch map - is a follow-up, not yet done as of this
commit.

## The follow-up: array-indexing `worker_pool.cpp`'s own per-batch dedup map

Picking up the follow-up flagged above: `worker_pool.cpp`'s `output_subjects` map is rebuilt fresh
for every message, and its `.count(sub_id)` check runs once per `(row, matched_id)` pair - not just
once per *unique* id, unlike `subscription_manager`'s persistent storage (which is looked up once
per id total, ever). At high fan-out this is the more frequently-hit hashtable, so it's the more
plausible home for whatever win the previous section's real-pipeline result didn't fully capture.

Same mechanism as `subscription_manager`'s array, applied differently: a `std::vector<uint64_t>
dedup_generation` plus a `uint64_t dedup_current_generation` counter, both declared once per
worker thread (locals in `worker_loop()`, alongside the existing `queued_message qm;`) and reused
across every message that one thread ever processes - not rebuilt per message. Bumping the
generation counter once per message turns the array into an O(1) "have I already resolved this id
*this* message" check with no clear step between messages: a slot only counts as current if its
stored generation matches `dedup_current_generation` right now. Ids below a 1,000,000 cap (mirrors
`subscription_manager::kArrayIndexCap` - same dense/monotonic-id assumption, duplicated as
`kDedupArrayCap` rather than exposed just for this) use the array; the rare id above it falls back
to the original map's own `.count()`, unchanged. The map itself (`output_subjects`) still gets
built and handed to the publish coroutine exactly as before - only the per-pair *existence check*
that gated whether to bother calling `output_subject()` and inserting into it changed to the array.
This mechanism itself isn't new: it's the *other* half of what `dedup_bench.cpp`'s isolated
microbenchmark validated together in the previous section (that test modeled the persistent store
and the per-message dedup as one combined array design) - no new isolated benchmark was needed to
justify trying it here.

Verified: full `sidecar_test` suite green under all three configs (316/316 - plain, ASan+UBSan,
ThreadSanitizer; TSan relevant even though this state is worker-thread-local, not shared, as a
sign-off on the new per-thread persistent scratch pattern itself).

Measured on the real pipeline the same way as every other change in this investigation -
interleaved `perf stat -e cycles:u`, alternating start order across repeats to rule out the
run-order confound this document found earlier - at three selectivities:

- **`s`=1: 8/8 pairs favorable, mean -5.93% (stdev 2.26%)** - a real, clean win.
- **`s`=20: 4/8 favorable, mean +1.34% (stdev 12.65%)** - noise, no signal, once extended past an
  initially-misleading 4-pair sample (3/4 favorable) that didn't survive a larger run.
- **`s`=100: 2/8 favorable, mean +1.51%** - no signal either, slightly unfavorable if anything.

Understood, not just observed, and it's the mirror image of the previous section's result:
`s`=1 is this benchmark's *highest* per-row fan-out (selectivity 1/`s` = 1, so most subscriptions
match most rows - average fan-out approaches K itself), while `s`=20 and `s`=100 have
progressively smaller per-row fan-out (roughly K/20 and K/100 on average). The per-*message* dedup
array wins specifically where a single message's own dedup pass has to walk a very large number of
ids at once - exactly the large-N regime `dedup_bench.cpp` validated (`avg_fanout` 986 and 400) -
and shows no benefit where per-row fan-out is small enough that the hash map's own overhead was
already negligible relative to everything else the row does. Combined with the previous section's
finding (persistent-storage half wins clearly at `s`=20, not `s`=1), the two halves of the same
array/generation-counter idea end up winning at *different* selectivities, depending on which
access pattern - many distinct ids looked up once each over time, vs. one message repeatedly
re-checking a huge shared id set - is actually the bottleneck at that fan-out level. Committed
(`src/worker_pool.cpp`) on the strength of the real, low-variance `s`=1 result.

## Backpressure-adaptive count-then-emit matching for pstree

With the dedup-side hashtable costs cleared out (the two sections above), `PSTDynamic::matchEvent()`
itself became the largest remaining item in the profile - 56% of total CPU self-time at K=8000/`s`=1,
almost entirely the cold-memory-write cost of `push_back()`ing every match into a
`std::vector<uint64_t>` (already `reserve()`'d - see `pst_dynamic.hpp`'s own `perf annotate` comment,
no reallocation cost left). Separately, this document's own earlier K-scaling section already proved
that at high fan-out, most matched rows never get published at all: `worker_pool.cpp`'s backpressure
check drops ~94% of matched rows at `s`=1 and ~82% at `s`=20, ~0% at `s`=100. Critically, `matchEvent()`
already runs - and pays its full write cost - for *every* row, *before* that check runs, because the
check needs `matched_ids.size()` per row, which today only exists once the ids vector is already fully
built. So for ~94%/82% of rows at `s`=1/`s`=20, the expensive vector write happens for nothing: built,
then immediately discarded, unread.

**Isolated validation first, as always.** A microbenchmark (`countemit_bench.cpp`) modeled splitting
the candidate walk into a cheap COUNT-only pass (evaluate each candidate, just increment a counter, no
vector write - feeds the backpressure decision) plus a second EMIT pass (evaluate again, this time
writing ids) that only runs for rows surviving backpressure, against real-measured kept/dropped ratios:
kept-fraction 6% (`s`=1-like): **-27.7% mean**; kept-fraction 17.4% (`s`=20-like): **-21.8% mean**;
kept-fraction 99.9% (`s`=100-like): **+21.6%** - a real *regression*, since kept rows now pay for
evaluating every candidate twice. That crossover is why the real design is adaptive, not unconditional:
peek at current backpressure state before matching, and only take the two-phase path when the system
already looks under pressure - otherwise use today's unchanged single-pass path, so low-pressure
conditions never regress.

**A design review caught a load-bearing bug before any of this was implemented.** `pstree_matching_engine`'s
own `m_hasSyntheticClauses` flag (set once an OR'd/multi-clause subscription is ever inserted, and
monotonic thereafter) is realistically `true` at K=8000 (~10% of this project's own benchmark
subscriptions are OR'd, per `insert()`'s own comment) - a naive `search_count()` slow path that just
called `matchEvent()`/`search()` and counted the result would have silently paid the exact cost this
whole feature exists to eliminate, making it worth ~0% on the actual traffic shape it was built for.
Fixed by giving `PSTDynamic` a genuine callback-based primitive (`matchEventEach()`) that visits
matches one at a time without ever building a vector, and having `search_count()`'s own OR-clause
dedup logic (the same `m_clause_to_sub` translation + `seen`-set dedup `search()` already has) run
incrementally against that callback instead.

**The `pstree` refactor itself caught a real bug before being committed.** Splitting `matchEvent()`
into `scanCandidates()` (interning + candidate collection) and `walkCandidates()` (the per-candidate
check, now callback-based) is a pure move with zero intended behavior change - but an early version
made `internedEvent` a `scanCandidates()`-local variable, while the `indexed` span it returns holds
raw pointers directly into it. Since `walkCandidates()` reads through `indexed` from the *caller's*
frame, after `scanCandidates()` has already returned, every one of those pointers was left dangling
the moment the function returned - a real, silent memory-corruption bug. Caught immediately: the
existing `pstree` test suite failed with a nonsensical "matchValue type mismatch" error the moment it
was built, before anything here was committed. Fixed by making `internedEvent` a caller-owned
out-param too, exactly like `indexedStorage`/`indexed` already were - all three now share the calling
frame's lifetime, through both `matchEvent()`'s existing use and the new `matchEventEach()`.

**Architecture** (full detail in each file's own comments): `matching_engine.hpp` gains two new
virtuals with safe defaults - `supports_count()` (false unless overridden) and `search_count()`
(throws `matching_engine_error` unless overridden) - zero code changes to a-tree/be-tree, which
inherit both defaults untouched. `pstree_matching_engine::search_count()` mirrors `search()`'s own
fast-path/slow-path shape exactly, via `PSTDynamic::matchEventCount()`/`matchEventEach()`.
`event_bridge.hpp`/`.cpp` gain `count_match_columnar_batch()`, a count-only sibling of
`deserialize_and_match_columnar()` with the same format dispatch (Arrow included) but no
`OutProtocol`/`serialize_row()` at all. Its `columnar_count_estimate::estimated_bytes` field uses
`total_match_count * (payload.size()/row_count + 64)` - a uniform per-row average, confirmed (by
reading both in full) to be the only cheap size proxy available, since neither `zerialize::ColumnarRows<V>`
nor `ArrowColumnarRows` tracks any per-row byte range. This affects only the *accuracy* of the
byte-based backpressure *estimate* on the count-only path - "estimated_bytes" was already documented
as an approximation before this existed - never the decision logic, and never what ultimately
publishes: a kept message's real ids/payloads always come from an unchanged, exact
`deserialize_and_match_columnar()` call.

`worker_pool.cpp` gains a new `m_engine` field (set from `cfg.engine` at construction - guaranteed to
match `subscription_manager`'s own live tree, since `sidecar.cpp` constructs both from the same `cfg`)
and three shared helpers (`try_reserve_publish_capacity()`/`adjust_reserved_publish_bytes()`/
`release_reserved_publish_capacity()`) so the reserve/check/rollback atomic dance can never drift
between the direct and adaptive call sites. `worker_loop()`'s dispatch: only `qm.columnar &&
m_engine==pstree` is ever eligible; a non-mutating peek at current `publish_inflight`/
`publish_inflight_bytes` decides whether to run the count-only pass first. A gate-reject means the real
match never ran at all for that message - the actual win. A gate-accept (a misprediction) falls
through to the unchanged, real `deserialize_and_match_columnar()` call, truing up the provisional
reservation instead of reserving twice. Two correctness edges got dedicated handling rather than being
left implicit: `m_matched`/`m_processed` stay path-independent (a gate-dropped message still counts as
"processed"/"matched", exactly like the direct path's own drop branch already did - a gap caught and
fixed before any test was written for it), and `release_reserved_publish_capacity()` exists specifically
for the "kept but the real match then came back empty/malformed after all" edge, so a repeated
misprediction can never leak reserved capacity. That specific edge is structurally very hard to trigger
even deliberately (the count pass and the eventual real pass run against the *same* frozen tree
snapshot under one `tree_guard`, so a disagreement between them would itself be a bug the count-parity
tests below already guard against) - covered by code review and full-suite TSan coverage rather than a
dedicated unit test forcing it.

Given the mathematically-provable relationship between the peek's `current >= cap` threshold and the
real check's `current + estimate > cap` threshold, "the peek predicts pressure" *implies* "the real
check also rejects" for any message with a positive estimate on the bytes clause (and the two
thresholds read the identical quantity on the task-count clause) - meaning the "predicted pressure but
kept anyway" fallback is a genuine, race-dependent edge in real concurrent operation (a competing
worker's publish coroutine releasing capacity between the peek and the count pass), not one
constructible on demand in a controlled single-threaded test. `test_worker_pool.cpp`'s own new tests
cover what *is* deterministic instead: normal end-to-end publish when pstree+columnar is adaptive-eligible
but not under pressure, and the gate-drop branch's full observable contract (no publish, no write, but
`processed`/`matched` stats reflect the count pass that genuinely ran).

**Verified**: `pstree`'s own `ctest` suite green (12/12, new `test_pst_dynamic_count.cpp` differential-
tests `matchEventCount()`/`matchEventEach()` against `matchEvent()` itself - fig3 fixtures, edge cases,
and a randomized insert/delete/match stress loop). `sidecar_test` green under all three configs
(328/328 - plain/ASan+UBSan/ThreadSanitizer; TSan specifically relevant for `search_count()`'s own
`seen_count` thread_local scratch set, mirroring `search()`'s own earlier, real data-race fix).

**Measured on the real pipeline** via `sidecar_pipeline_bench` + interleaved `perf stat -e cycles:u`
(alternating start order, 8 pairs per selectivity): **`s`=1 is a real win - 7/8 favorable, mean -4.43%**
- smaller than the isolated microbenchmark's -27.7% prediction, consistent with dilution from
everything else in the real pipeline (same pattern as every other real-vs-isolated result in this
document). **`s`=20 shows no clear signal** (mean +3.48%, 3/8 favorable, noisy) - unlike the isolated
prediction; not yet understood in detail, plausibly because `s`=20's lower per-row fan-out means fewer
messages accumulate enough `publish_inflight_bytes` to trip the peek at all, diluting any win with
direct-path-only traffic. **`s`=100 shows no regression** (mean -0.35%, 4/8 favorable, noise) - this is
the actual point of choosing *adaptive* over *unconditional*: the isolated microbenchmark predicted a
real +21.6% regression at `s`=100-like kept fractions for an unconditional two-phase design, and the
adaptive gate successfully avoids it by almost never engaging when the system isn't genuinely under
pressure. Committed on the strength of the real `s`=1 result and the confirmed absence of an `s`=100
regression; `s`=20's own lack of signal is reported honestly rather than smoothed over.

## High-churn-under-load: subscribe/unsubscribe cost while the data plane is under sustained load

Every performance investigation in this document up to this point assumed a *static* subscription
set - K fixed, no subscribes/unsubscribes happening while the data plane is under load. That's a
real, unexamined gap: `subscription_manager::rebuild_tree_locked()` is O(current subscription
count) and runs on every fully-removed subscription (none of a-tree/be-tree/pstree expose a delete
primitive) *while holding `m_mutex`'s exclusive lock* - the same lock every worker thread's
`acquire_tree()` needs (as a shared lock) before it can match a single row.

**Tool**: `tests/churn_load_test.py`, built on `tests/integration_sidecar.py`'s own `NatsClient`/
process-lifecycle machinery (a real `nats-server` + a real `nats_sidecar` binary, not a fake
connection) rather than a from-scratch harness. Three concurrent actors, each its own NATS
connection/Python thread: a raw msgpack data-plane publisher; a subscribe/unsubscribe churn driver
holding a steady-state target K, in two modes - "warm" (a fixed pool of K expressions, reused
across many client ids - no `subscription_registry` KV round trips once each pool entry is known)
and "cold" (every cycle a brand-new expression - a real KV round trip every time); and a listener
subscribed directly to a dedicated probe subscription that matches every published row, measuring
real end-to-end publish-to-delivery latency (see the follow-up below for why this third actor was
added). After each run, `sidecar.list_subscriptions` (this document's own "admin-facing
subscription listing" section) cross-checks the churn driver's own bookkeeping against the
server's real state - a real, direct use of that endpoint's own stated purpose, and something with
no verification path before it existed.

**Two real bugs in the tool itself, found and fixed while first using it** (kept here, not scrubbed
- matching this document's own practice everywhere else): (1) `build/bin/nats_sidecar` was stale
(only `sidecar_lib`/`sidecar_test` had been rebuilt that session, not the actual executable target)
- the first run hung indefinitely on `sidecar.list_subscriptions` because the running binary
predated that endpoint entirely. (2) The warm-mode add/remove balance decision originally compared
lease-*pair* count against target K, conflated with a `target_k / 4` pool size - since most warm-mode
adds are reuses (not new distinct subscriptions), lease count could take far longer to reach target
K than the run's own duration allowed, so warm mode could run for its entire duration in pure "add"
bias, never reaching steady-state churn at all. Fixed by keying the balance decision on *distinct*
id count consistently, and by round-robining through the warm pool on first coverage instead of
sampling it randomly (plain random sampling is a coupon-collector process - covering all K pool
entries at least once takes an *expected* K·ln(K) draws, ~6900 for K=1000, not K). (3), found while
adding the latency probe: `"temperature >= 0"` was rejected outright (`matching_engine_error`,
mismatched types) - `temperature` is a `float` attribute and the bare integer literal `0` isn't
auto-promoted the way it is at some other call sites; needed `"temperature >= 0.0"`.

**Findings** (K=100 vs. K=1000, `cold` mode, `--churn-rate 50 --data-rate 2000 --selectivity 10`,
60s runs with the first 25s excluded from percentiles as ramp-up):

- **Unsubscribe (removal) cost scales with K, confirmed cleanly**: mean **1.40ms at K=100** →
  mean **13.53ms at K=1000** (~10x, tracking the ~10x K increase) - and this holds across the
  *whole* distribution, not just a tail (K=1000: p50=13.44ms, p99=15.48ms, p999=18.21ms - mean,
  median, and p99 are all close together, meaning *every* removal pays this cost, not just a rare
  unlucky few). This is `rebuild_tree_locked()`'s O(K) cost, real and directly measurable for the
  first time in this document.
- **Subscribe (insert) cost stays cheap regardless of K**: ~1.0-1.3ms at both K=100 and K=1000, in
  both warm and cold mode - confirms `subscribe()`'s own true incremental `insert()` (this
  document's own earlier fix for the old O(K²) bulk-rebuild pattern) still holds under sustained
  churn, not just at insert time in isolation.
- **Warm vs. cold, isolated per side**: on *subscribe*, cold pays a real but modest premium over
  warm at matched K=1000 (mean 1.31ms cold vs. 0.96ms warm) - the registry KV round trip's own
  cost, smaller than the K-scaling effect above. On *unsubscribe*, warm and cold cost the
  essentially the same when a full removal happens (warm's own few sampled removals: mean 4.29ms,
  max 13.77ms - in the same range as cold's 13.53ms mean) - consistent with `rebuild_tree_locked()`
  being entirely registry-independent, exactly as the architecture would predict (the registry is
  only ever consulted on the *insert* side).

**Follow-up: does this actually stall worker-thread matching, not just the churn request itself?**
The findings above measure the churn *request's own* round-trip latency, which necessarily
includes the synchronous rebuild cost by construction - they don't, on their own, show whether
*worker-thread matching* latency also stalls during that same window (the original concern:
`m_mutex`'s exclusive lock blocking every worker's `acquire_tree()` while a rebuild runs). Extended
the tool to answer this directly rather than by proxy: the data-plane publisher now embeds a
`seq`/`send_ts` in every row (silently ignored for matching - `populate_event()` already skips any
key outside the configured schema - but preserved verbatim in the republished output, since
row-mode forwards the client's own original raw bytes, not a re-serialized copy), and a dedicated
listener subscribes directly to a probe subscription matching every row, recording real end-to-end
publish-to-delivery latency. Cross-referencing each sample's own `[send_ts, recv_ts]` interval
against the churn thread's recorded full-removal rebuild windows splits every message into
"isolated" or "overlapped a rebuild" - the actual causal test, not an inference from aggregate
averages.

**Confirmed, cleanly, at K=1000** (same run shape as above; `unsubscribe_full_removal` is 871
samples, `subscribe` 870): **median data-plane latency shifts from 0.48ms (isolated, n=37,699) to
9.37ms (overlapped a rebuild, n=22,815)** - not a tail effect, the *typical* experience for a
message whose processing window overlapped one. Full picture: isolated mean=1.00ms/p99=5.31ms/
max=12.72ms vs. overlapped mean=9.28ms/p99=21.43ms/max=45.71ms. Across this one 60s run, 871
full-removal rebuilds accumulated **12.2 seconds of cumulative exclusive-lock hold time - roughly
35% of the run's own steady-state wall-clock window** - at a churn rate (50 cycles/sec, ~25
removes/sec) and K (1000) that aren't exotic for a real fleet. Hypothesis #1 is confirmed, not just
plausible: `rebuild_tree_locked()`'s exclusive lock is a real, measurable, and substantial tax on
data-plane matching latency under concurrent churn, at a scale this project's own benchmarks
already treat as ordinary.

(One correction to the earlier findings above, made while building this extension: the original
`unsubscribe` percentile bucket blended full removals - the only kind that actually calls
`rebuild_tree_locked()` - with partial removals - a lease dropped but others remain, which never
touch the tree at all. Warm mode's own earlier-reported "mean=4.29ms, max=13.77ms, n=11" was this
blend; splitting into `unsubscribe_full_removal`/`unsubscribe_partial` makes the two costs directly
visible instead of diluting one into the other. This doesn't change the cold-mode K=100-vs-K=1000
comparison above - cold mode's expressions are never reused, so every one of its removals was
already a full removal by construction.)

**A second, more consequential correction, found while verifying the fix below**: every number
above was measured against **a-tree**, not pstree - `config/example.yaml` (this tool's own default
`--config`) sets `engine: atree`, and the tool had no way to override it until the fix below added
one. The *finding itself* is still completely valid (`rebuild_tree_locked()` doesn't care which
engine it's rebuilding - the cost is inherent to the "discard and re-insert everything" strategy,
not to a-tree specifically), but every reference to "this project's own flagship/most-benchmarked
engine" in the paragraphs above was an unverified assumption, not a checked fact. Corrected here
rather than silently fixed, matching this document's own standing practice.

## Fixing it: true incremental delete for pstree, replacing rebuild_tree_locked() on removal

The class-level comment `rebuild_tree_locked()` cites for its own existence - "none of a-tree/
be-tree/pstree expose a delete primitive" - turned out to be **factually wrong for pstree
specifically**. Reading `pst_dynamic.hpp` directly: `PSTDynamic::deleteSubscription(subId)`
(Algorithm 6) is a genuinely incremental delete - it touches only the leaf nodes reachable from
the *one* affected subscription's own access predicate, not the whole tree, and already includes
space reclamation. It was already used safely, just narrowly: `pstree_matching_engine::insert()`'s
own rollback-on-failure path already called it to undo a partially-completed insert. It had simply
never been wired up for the general "remove a live subscription" case `subscription_manager`
actually needs on every real unsubscribe.

**Design**: `matching_engine.hpp` gains `supports_remove()`/`remove(id)`, the same safe-default
pattern as `supports_count()`/`search_count()` from the earlier adaptive-dispatch work - zero code
changes to a-tree/be-tree, which simply inherit the defaults (`false` / throws).
`pstree_matching_engine` gets a real override backed directly by `deleteSubscription()`, plus one
new piece of bookkeeping `insert()` didn't previously need to keep: a forward map from a
caller-facing subscription id to the (possibly several, for an OR'd expression) underlying
PSTDynamic clause ids it expanded into at insert time - the reverse of the `m_clause_to_sub` map
`search()` already maintains. `subscription_manager`'s own two removal call sites
(`remove_lease()`'s full-removal branch, `remove_subscription()`) now check
`supports_remove()` and call the true `remove()` when available, falling back to the unchanged
`rebuild_tree_locked()` otherwise - a-tree/be-tree keep today's behavior exactly, at zero risk.

One real test-design lesson from building this: an early version of the concurrency test called
`remove()` and `search()` directly on a raw `matching_engine`, with no external locking - and it
crashed (`std::bad_alloc`, real internal-state corruption) almost immediately. That wasn't a
production bug; it was testing an invariant this layer never promised. `subscription_manager::
m_mutex` is what actually serializes writers against readers in this codebase (its own
"deliberately NOT lock-free for readers" doc comment says so directly) - every *other* existing
concurrent test at the `matching_engine.cpp` layer only exercises concurrent reads for exactly this
reason. The real concurrency test belongs at the `subscription_manager` layer instead, where the
locking actually lives - see `pstree_remove_lease_concurrent_with_search_does_not_race`.

**Verified, with a real pstree-specific before/after** (the tool needed a new `--engine` flag
first, itself a fix for the mix-up above - `churn_load_test.py --engine pstree`, same K=1000/`cold`/
60s configuration as the original finding):

| | subscribe | unsubscribe (full removal) | cumulative lock-hold | data-plane isolated vs. overlapped | dropped messages |
|---|---|---|---|---|---|
| **before** (pstree, rebuild-based) | mean 10.08ms | mean **37.19ms**, p50 33.45ms | **22.6s of 35s steady-state (~65%)** | mean 3.27ms vs. **26.37ms** | **41 of 103,917** |
| **after** (pstree, true `remove()`) | mean 1.48ms | mean **1.09ms**, p50 1.00ms | **952ms of 35s (~2.7%)** | mean 0.61ms vs. **0.82ms** | **0 of 103,985** |

Every one of the plan's own verification criteria held, and more strongly than expected - pstree's
own rebuild cost was actually *worse* than a-tree's at the same K (37.19ms vs. a-tree's own
13.95ms mean), so pstree had more to gain from this fix, not less. `unsubscribe` now costs about
the same as `subscribe` (both cheap, incremental operations, as they should be) instead of ~34x
more. The data-plane isolated-vs-overlapped gap - the actual causal signal for "does this stall
worker-thread matching" - shrinks from a ~8x mean difference to a difference so small it's within
this measurement's own noise floor. And a real, previously-unreported consequence surfaces in the
"before" row that latency percentiles alone don't capture: **41 real dropped messages** - not just
slow, actually lost, on a plain core-NATS input subject with no redelivery - a direct
consequence of the rebuild stalling matching long enough to exceed whatever timeout or
backpressure mechanism was in the path. The fix eliminates this too.

Full `sidecar_test` suite green under all three configs (348/348 - plain/ASan+UBSan/
ThreadSanitizer) both before and after this change.

## License

See LICENSE file.
