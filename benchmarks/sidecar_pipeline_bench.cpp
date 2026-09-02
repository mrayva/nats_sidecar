// Standalone, publish-independent sidecar throughput benchmark: drives the REAL production
// pipeline (deserialize_and_match_columnar -> populate_event -> matching_engine::search() ->
// fan-out resolution, exactly as worker_pool::worker_loop() does for a real inbound NATS message)
// with zero real NATS I/O, zero Postgres, zero external publisher process.
//
// Why this exists: every real-fleet trial this project's own benchmark history has run measures
// the *combined* system - an external Postgres-backed publisher plus real NATS transport plus the
// sidecar's own processing. A real, `perf`-confirmed sidecar-side fix (caching a redundant
// mp_skip call in zerialize's msgpack iterators) still showed no clean end-to-end throughput win
// in that combined setup: pre-fix and post-fix trials all landed at the same ~15,352 rows/s
// ceiling, consistent with the *publisher* (pub_workers=24) being the binding constraint at the
// fast end, not the sidecar. This benchmark removes that confound entirely so further sidecar-side
// fixes can be measured against a clean, publish-independent ceiling.
//
// Unlike matching_engine_bench.cpp (which calls event_sink::with_float() etc. directly, bypassing
// the real wire-format decode path), this benchmark builds real columnar msgpack payloads via
// zerialize's own serializer (the same DSL test_worker_pool.cpp's columnar_payload() helper uses)
// and feeds them through worker_pool::enqueue() - the actual code path a real inbound NATS message
// takes, including deserialize_and_match_columnar()/populate_event()/ColumnarRows (where the
// mp_skip fix lives) and the fan-out-resolution stage timed by avg_fanout_us.
//
// I/O is eliminated via sidecar_test::fake_connection (tests/fake_connection.hpp) - its
// write_raw() and everything else is a true no-op unless a test/benchmark hooks it, already
// proven correct by every test_worker_pool.cpp test that publishes through it.
//
// Deliberately does NOT use tests/asio_test_helpers.hpp's drive_until(): it polls with a
// hardcoded 2ms sleep_for per iteration, fine for correctness tests where wall-clock doesn't
// matter, but it would impose an artificial ~500/s polling ceiling on a throughput measurement.
// Instead ioc.run() runs on its own dedicated thread for the benchmark's duration (matching how
// sidecar_engine actually runs in production), and completion is polled with a tight busy-loop.
//
// Self-contained, no external dependencies: `price` is synthetic, uniform in [0, kPriceMax] -
// for a uniform distribution, P(price > X) = (kPriceMax - X) / kPriceMax, so the threshold for a
// target selectivity `s` (matched-fraction = 1/s, this project's own established convention from
// the real-fleet selectivity sweep) is exactly `kPriceMax * (1 - 1/s)` - no percentile queries or
// real data needed.
//
// `input_format=arrow` (the default) exercises ArrowColumnarRows (arrow_columnar_rows.hpp) -
// the *other* real production input-decode path, distinct from ColumnarRows<MsgPackDeserializer>
// - with msgpack as the republish encoding (Arrow has no single-row encoder of its own, so
// output_format decoupling is required whenever format==arrow - see config::output_format's own
// comment). Batches are built via arrow::DoubleBuilder/StringBuilder -> arrow::RecordBatch::Make
// -> arrow::ipc::MakeStreamWriter, the same construction tests/test_arrow_columnar.cpp's own
// build_arrow_ipc_stream()/double_array()/string_array() helpers use (reimplemented here without
// their EXPECT_TRUE gtest macros, since this is a standalone binary).
//
// `publish=real` (default: fake, zero I/O) connects to a real local NATS *core* server
// (127.0.0.1:4222, via nats_asio::connect() - a plain PUB/write_raw connection, no JetStream
// consumer or KV bucket, nothing to clean up server-side afterward) instead of
// sidecar_test::fake_connection, so real NATS write I/O for the output side can be measured
// against the fully-isolated number without reintroducing the external-publisher/Postgres
// confound this whole benchmark exists to remove.
//
// Usage: sidecar_pipeline_bench [K] [s] [total_rows] [worker_threads] [engine: atree|betree|pstree]
//                                [input_format: arrow|msgpack] [publish: fake|real]

#include "worker_pool.hpp"
#include "subscription_manager.hpp"
#include "fake_connection.hpp"

#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <spdlog/sinks/null_sink.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr double kPriceMax = 1000.0;
constexpr std::size_t kBatchSize = 500; // matches the real publisher's own --batch-size 500

std::vector<std::string> exchange_pool() {
    return {"NYSE", "NASDAQ", "ARCA", "BATS"};
}

// K distinct "price > X" expressions, X jittered in a narrow band around the analytic threshold
// for target selectivity s - mirrors nyse-matrix/gen_price_threshold_subs_realistic.py's own
// narrow-band approach. subscription_manager's own identical-expression dedup (m_expr_to_id)
// would otherwise collapse textually-identical subscriptions, exactly the "K (actual) < K
// (requested)" effect this project's own real-fleet sweep already observed and reported honestly.
std::vector<std::string> generate_subscriptions(std::size_t k, double s, unsigned seed) {
    // Jitter in SELECTIVITY-space (a narrow band of 1/s values), not threshold-space directly:
    // jittering the threshold itself risks pushing some subscriptions' thresholds above
    // kPriceMax, making them mathematically unsatisfiable (price can never exceed kPriceMax) and
    // silently skewing the real achieved selectivity well below the requested `s` - caught by
    // comparing this benchmark's own reported `matched` count against the naive expectation
    // (~k/s per row) and finding it far lower for the default s=100000 case before this fix.
    std::mt19937 rng(seed);
    double lo_selectivity = 1.0 / s * 0.95;
    double hi_selectivity = 1.0 / s * 1.05;
    std::uniform_real_distribution<double> selectivity(lo_selectivity, hi_selectivity);

    std::vector<std::string> exprs;
    exprs.reserve(k);
    char buf[64];
    for (std::size_t i = 0; i < k; ++i) {
        double x = kPriceMax * (1.0 - selectivity(rng));
        std::snprintf(buf, sizeof(buf), "price > %.8f", x);
        exprs.emplace_back(buf);
    }
    return exprs;
}

// One real columnar msgpack batch: {"price": [...], "exchange": [...]}, kBatchSize rows - the
// same wire shape ColumnarRows/populate_event parse in production, built via the same DSL
// test_worker_pool.cpp's columnar_payload() helper uses.
std::vector<char> generate_batch(std::mt19937& rng) {
    auto exchanges = exchange_pool();
    std::uniform_real_distribution<double> price(0.0, kPriceMax);
    std::uniform_int_distribution<std::size_t> exch_idx(0, exchanges.size() - 1);

    zerialize::dyn::Value::Array prices;
    zerialize::dyn::Value::Array exchange_vals;
    prices.reserve(kBatchSize);
    exchange_vals.reserve(kBatchSize);
    for (std::size_t i = 0; i < kBatchSize; ++i) {
        prices.push_back(zerialize::dyn::Value(price(rng)));
        exchange_vals.push_back(zerialize::dyn::Value(exchanges[exch_idx(rng)].c_str()));
    }
    auto buf = zerialize::serialize<zerialize::MsgPack>(zerialize::dyn::Value::map({
        {"price", zerialize::dyn::Value::array(std::move(prices))},
        {"exchange", zerialize::dyn::Value::array(std::move(exchange_vals))},
    }));
    return std::vector<char>(reinterpret_cast<const char*>(buf.data()),
                             reinterpret_cast<const char*>(buf.data()) + buf.size());
}

void check_arrow(const arrow::Status& st, const char* what) {
    if (!st.ok()) throw std::runtime_error(std::string("arrow: ") + what + ": " + st.ToString());
}

// One real Arrow IPC-stream batch: a single RecordBatch with the same {price: double, exchange:
// string} two-column shape as generate_batch()'s msgpack payload, and the same value
// distributions - the shape pg_arrow's own rows_to_arrow() produces in production (per
// build_arrow_ipc_stream's own comment in test_arrow_columnar.cpp, already verified against
// pg_arrow.cpp there). MakeStreamWriter, never NewFileWriter - no footer/magic bytes, matching
// ArrowColumnarRows' own constructor expectations.
std::vector<char> generate_arrow_batch(std::mt19937& rng) {
    auto exchanges = exchange_pool();
    std::uniform_real_distribution<double> price(0.0, kPriceMax);
    std::uniform_int_distribution<std::size_t> exch_idx(0, exchanges.size() - 1);

    arrow::DoubleBuilder price_builder;
    arrow::StringBuilder exchange_builder;
    for (std::size_t i = 0; i < kBatchSize; ++i) {
        check_arrow(price_builder.Append(price(rng)), "append price");
        check_arrow(exchange_builder.Append(exchanges[exch_idx(rng)]), "append exchange");
    }
    std::shared_ptr<arrow::Array> price_arr;
    std::shared_ptr<arrow::Array> exchange_arr;
    check_arrow(price_builder.Finish(&price_arr), "finish price array");
    check_arrow(exchange_builder.Finish(&exchange_arr), "finish exchange array");

    auto arrow_schema = arrow::schema({
        arrow::field("price", price_arr->type(), /*nullable=*/true),
        arrow::field("exchange", exchange_arr->type(), /*nullable=*/true),
    });
    auto batch = arrow::RecordBatch::Make(
        arrow_schema, static_cast<int64_t>(kBatchSize), {price_arr, exchange_arr});

    auto sink_result = arrow::io::BufferOutputStream::Create();
    check_arrow(sink_result.status(), "create output stream");
    auto sink = *sink_result;
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, arrow_schema);
    check_arrow(writer_result.status(), "create stream writer");
    auto writer = *writer_result;
    check_arrow(writer->WriteRecordBatch(*batch), "write record batch");
    check_arrow(writer->Close(), "close writer");
    auto buffer_result = sink->Finish();
    check_arrow(buffer_result.status(), "finish output stream");
    auto buffer = *buffer_result;

    return std::vector<char>(reinterpret_cast<const char*>(buffer->data()),
                             reinterpret_cast<const char*>(buffer->data()) + buffer->size());
}

sidecar::engine_type parse_engine(const std::string& s) {
    if (s == "atree") return sidecar::engine_type::atree;
    if (s == "betree") return sidecar::engine_type::betree;
    if (s == "pstree") return sidecar::engine_type::pstree;
    std::fprintf(stderr, "unknown engine '%s', defaulting to pstree\n", s.c_str());
    return sidecar::engine_type::pstree;
}

const char* engine_name(sidecar::engine_type e) {
    switch (e) {
        case sidecar::engine_type::atree:  return "atree";
        case sidecar::engine_type::betree: return "betree";
        case sidecar::engine_type::pstree: return "pstree";
    }
    return "?";
}

} // namespace

int main(int argc, char** argv) {
    std::size_t k = argc > 1 ? std::stoul(argv[1]) : 3000;
    double s = argc > 2 ? std::stod(argv[2]) : 100000.0;
    std::size_t total_rows = argc > 3 ? std::stoul(argv[3]) : 230276;
    unsigned worker_threads = argc > 4 ? static_cast<unsigned>(std::stoul(argv[4])) : 3;
    sidecar::engine_type engine = argc > 5 ? parse_engine(argv[5]) : sidecar::engine_type::pstree;
    std::string input_format = argc > 6 ? argv[6] : "arrow";
    std::string publish = argc > 7 ? argv[7] : "fake";

    if (input_format != "arrow" && input_format != "msgpack") {
        std::fprintf(stderr, "unknown input_format '%s' (expected arrow|msgpack)\n",
                     input_format.c_str());
        return 1;
    }
    if (publish != "fake" && publish != "real") {
        std::fprintf(stderr, "unknown publish '%s' (expected fake|real)\n", publish.c_str());
        return 1;
    }
    bool use_arrow = (input_format == "arrow");

    auto log = std::make_shared<spdlog::logger>(
        "sidecar_pipeline_bench", std::make_shared<spdlog::sinks::null_sink_mt>());

    std::vector<sidecar::attribute_def> attrs = {
        {"price", sidecar::attribute_type::float_val},
        {"exchange", sidecar::attribute_type::string},
    };
    sidecar::attribute_schema schema(attrs);
    sidecar::subscription_manager subscriptions(attrs, "output", log, engine);

    constexpr unsigned kSubSeed = 42;
    auto exprs = generate_subscriptions(k, s, kSubSeed);
    std::size_t rejected = 0;
    for (std::size_t i = 0; i < exprs.size(); ++i) {
        try {
            subscriptions.subscribe(exprs[i], "bench-client-" + std::to_string(i));
        } catch (const std::exception& e) {
            ++rejected;
        }
    }
    std::size_t actual_k = subscriptions.active_count();

    std::size_t total_batches = (total_rows + kBatchSize - 1) / kBatchSize;
    total_rows = total_batches * kBatchSize; // round up to a whole number of batches

    constexpr unsigned kRowSeed = 1337;
    std::mt19937 row_rng(kRowSeed);
    std::vector<std::vector<char>> batches;
    batches.reserve(total_batches);
    for (std::size_t i = 0; i < total_batches; ++i) {
        batches.push_back(use_arrow ? generate_arrow_batch(row_rng) : generate_batch(row_rng));
    }

    sidecar::config cfg;
    cfg.input_subjects = {"bench.input"};
    cfg.output_prefix = "bench.output";
    cfg.worker_threads = worker_threads;
    cfg.attributes = attrs;
    // Arrow has no single-row encoder of its own (config::output_format's own comment) - msgpack
    // is required as the republish encoding whenever format==arrow. Every other format keeps
    // output_format unset (== format), matching production's own v1-scope constraint.
    cfg.format = use_arrow ? sidecar::binary_format::arrow : sidecar::binary_format::msgpack;
    cfg.output_format = use_arrow ? std::optional(sidecar::binary_format::msgpack) : std::nullopt;
    // Generous enough that enqueue() never legitimately drops during feeding - this benchmark
    // measures processing throughput, not backpressure behavior.
    cfg.input_queue_max_messages = total_batches + 100;
    cfg.input_queue_max_bytes = 2ull * 1024 * 1024 * 1024;
    cfg.publish_max_inflight = 100000;
    cfg.publish_max_inflight_bytes = 2ull * 1024 * 1024 * 1024;

    asio::io_context ioc;

    // ioc.run() on its own dedicated thread for the whole benchmark, matching how sidecar_engine
    // actually runs in production - not a sleep-based poll loop, which would impose an artificial
    // ceiling on a throughput measurement. Started before the connection is created: a real
    // connection's handshake is scheduled on `ioc` and only actually progresses once something is
    // running it.
    auto work_guard = asio::make_work_guard(ioc);
    std::thread io_thread([&ioc] { ioc.run(); });

    nats_asio::iconnection_sptr conn;
    if (publish == "real") {
        // Plain NATS *core* connection (write_raw/PUB frames, the same wire mechanism
        // worker_pool's own publish coroutine already uses) - no JetStream consumer, no KV
        // bucket, nothing to clean up server-side afterward.
        conn = nats_asio::connect(ioc, "127.0.0.1", 4222);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!conn->is_connected() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        if (!conn->is_connected()) {
            std::fprintf(stderr,
                "could not connect to NATS core server at 127.0.0.1:4222 within 5s - "
                "is a local NATS server running?\n");
            work_guard.reset();
            ioc.stop();
            io_thread.join();
            return 1;
        }
    } else {
        conn = std::make_shared<sidecar_test::fake_connection>();
    }
    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, conn, log);

    pool.start();

    std::printf("K requested=%zu actual=%zu (rejected=%zu)  total_rows=%zu (%zu batches of %zu)  "
                "worker_threads=%u  engine=%s  input_format=%s  publish=%s\n",
               k, actual_k, rejected, total_rows, total_batches, kBatchSize, worker_threads,
               engine_name(engine), input_format.c_str(), publish.c_str());

    auto t0 = std::chrono::steady_clock::now();
    std::size_t enqueued = 0;
    for (auto& batch : batches) {
        if (pool.enqueue(std::move(batch), /*columnar=*/true)) ++enqueued;
    }
    // Tight busy-loop, no sleep - the whole point of not reusing drive_until() here.
    while (pool.get_stats().processed < enqueued) {
        std::this_thread::yield();
    }
    auto t1 = std::chrono::steady_clock::now();
    double seconds = std::chrono::duration<double>(t1 - t0).count();

    auto stats = pool.get_stats();
    double rows_per_sec = seconds > 0 ? static_cast<double>(enqueued * kBatchSize) / seconds : 0.0;
    double avg_fanout_us = stats.fanout_time_count > 0
        ? (double(stats.fanout_time_ns_total) / double(stats.fanout_time_count)) / 1000.0
        : 0.0;

    std::printf("\nenqueued=%zu (of %zu batches) processed=%llu matched=%llu\n",
               enqueued, total_batches, (unsigned long long)stats.processed,
               (unsigned long long)stats.matched);
    std::printf("wall_clock=%.3fs  true_rows_per_sec=%.1f  avg_fanout_us=%.3f\n",
               seconds, rows_per_sec, avg_fanout_us);

    pool.stop();
    work_guard.reset();
    ioc.stop();
    io_thread.join();

    return 0;
}
