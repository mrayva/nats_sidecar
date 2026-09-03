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
// `record_shape=narrow` (default) publishes only the 4 fields any subscription in this benchmark
// could actually reference (price, exchange, symbol, trade_volume). `record_shape=wide` adds the
// same 11 extra columns the real NYSE trade fixture table has beyond those 4 (see
// pgnats/scripts/nyse_trade_short_cols_view.sql's own column list: Time, Sale Condition, Trade
// Stop Stock Indicator, Trade Correction Indicator, Sequence Number, Trade Id, Source of Trade,
// Trade Reporting Facility, Participant Timestamp, Trade Reporting Facility TRF Timestamp, Trade
// Through Exempt Indicator) - present in the payload but never referenced by the schema, so
// populate_event()'s per-row key walk (event_bridge.hpp) has to parse/skip each one via
// mapKeys()'s mp_skip-driven iteration (msgpack.hpp) without ever matching it, and the raw
// payload itself is bigger, meaning more bytes memmove'd per matched subscription during fan-out
// (worker_pool.cpp's append_pub_frame). Tests the theory that shorter records improve throughput
// by isolating exactly that one variable - same subscriptions, same K/s/selectivity, only the
// record's own extra-field count differs.
//
// Usage: sidecar_pipeline_bench [K] [s] [total_rows] [worker_threads] [engine: atree|betree|pstree]
//                                [input_format: arrow|msgpack] [publish: fake|real]
//                                [record_shape: narrow|wide]

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

std::vector<std::string> symbol_pool() {
    return {"AAPL", "MSFT", "TSLA", "NVDA", "AMZN", "GOOGL", "META", "SPY"};
}

// The 11 non-predicate columns the real NYSE trade fixture table has beyond the 4 predicate
// fields (see this file's own top comment) - only added when record_shape=wide. Values are
// synthetic but realistically shaped (short codes for the indicator/id columns, matching the
// real table's own 1-4 character values, large monotonic-looking integers for the timestamp/
// sequence columns). Batches are COLUMNAR (one array per column, kBatchSize entries each - same
// shape as generate_batch()'s own price/exchange arrays), so this builds one array per extra
// column, not one map per row.
void append_wide_extra_columns(zerialize::dyn::Value::Map& columns, std::mt19937& rng) {
    static const std::vector<std::string> sale_conditions = {"@", "F", "T", "@FI", "@FT"};
    static const std::vector<std::string> trade_stop_indicators = {" ", "T"};
    static const std::vector<std::string> trade_correction_indicators = {"00", "01", "02"};
    static const std::vector<std::string> sources_of_trade = {"C", "N", "E"};
    static const std::vector<std::string> trade_reporting_facilities = {" ", "D", "Q"};
    static const std::vector<std::string> trade_through_exempt_indicators = {"0", "1"};

    std::uniform_int_distribution<std::size_t> sc(0, sale_conditions.size() - 1);
    std::uniform_int_distribution<std::size_t> ts(0, trade_stop_indicators.size() - 1);
    std::uniform_int_distribution<std::size_t> tc(0, trade_correction_indicators.size() - 1);
    std::uniform_int_distribution<std::size_t> so(0, sources_of_trade.size() - 1);
    std::uniform_int_distribution<std::size_t> tf(0, trade_reporting_facilities.size() - 1);
    std::uniform_int_distribution<std::size_t> te(0, trade_through_exempt_indicators.size() - 1);

    zerialize::dyn::Value::Array time_col, sale_condition_col, trade_stop_col, trade_correction_col,
        sequence_number_col, trade_id_col, source_of_trade_col, trade_reporting_facility_col,
        participant_timestamp_col, trf_timestamp_col, trade_through_exempt_col;
    for (auto* col : {&time_col, &sale_condition_col, &trade_stop_col, &trade_correction_col,
                      &sequence_number_col, &trade_id_col, &source_of_trade_col,
                      &trade_reporting_facility_col, &participant_timestamp_col,
                      &trf_timestamp_col, &trade_through_exempt_col}) {
        col->reserve(kBatchSize);
    }

    for (std::size_t i = 0; i < kBatchSize; ++i) {
        std::int64_t base_ts = 34200000000000LL + static_cast<std::int64_t>(i) * 1000;
        time_col.push_back(zerialize::dyn::Value(base_ts));
        sale_condition_col.push_back(zerialize::dyn::Value(sale_conditions[sc(rng)].c_str()));
        trade_stop_col.push_back(zerialize::dyn::Value(trade_stop_indicators[ts(rng)].c_str()));
        trade_correction_col.push_back(zerialize::dyn::Value(trade_correction_indicators[tc(rng)].c_str()));
        sequence_number_col.push_back(zerialize::dyn::Value(static_cast<std::int64_t>(i)));
        trade_id_col.push_back(zerialize::dyn::Value(base_ts + static_cast<std::int64_t>(i)));
        source_of_trade_col.push_back(zerialize::dyn::Value(sources_of_trade[so(rng)].c_str()));
        trade_reporting_facility_col.push_back(zerialize::dyn::Value(trade_reporting_facilities[tf(rng)].c_str()));
        participant_timestamp_col.push_back(zerialize::dyn::Value(base_ts));
        trf_timestamp_col.push_back(zerialize::dyn::Value(base_ts));
        trade_through_exempt_col.push_back(zerialize::dyn::Value(trade_through_exempt_indicators[te(rng)].c_str()));
    }

    columns.emplace_back("time", zerialize::dyn::Value::array(std::move(time_col)));
    columns.emplace_back("sale_condition", zerialize::dyn::Value::array(std::move(sale_condition_col)));
    columns.emplace_back("trade_stop_indicator", zerialize::dyn::Value::array(std::move(trade_stop_col)));
    columns.emplace_back("trade_correction_indicator", zerialize::dyn::Value::array(std::move(trade_correction_col)));
    columns.emplace_back("sequence_number", zerialize::dyn::Value::array(std::move(sequence_number_col)));
    columns.emplace_back("trade_id", zerialize::dyn::Value::array(std::move(trade_id_col)));
    columns.emplace_back("source_of_trade", zerialize::dyn::Value::array(std::move(source_of_trade_col)));
    columns.emplace_back("trade_reporting_facility", zerialize::dyn::Value::array(std::move(trade_reporting_facility_col)));
    columns.emplace_back("participant_timestamp", zerialize::dyn::Value::array(std::move(participant_timestamp_col)));
    columns.emplace_back("trf_timestamp", zerialize::dyn::Value::array(std::move(trf_timestamp_col)));
    columns.emplace_back("trade_through_exempt_indicator", zerialize::dyn::Value::array(std::move(trade_through_exempt_col)));
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

// One real columnar msgpack batch: {"price": [...], "exchange": [...], "symbol": [...],
// "trade_volume": [...]} (record_shape=narrow) or the same 4 plus the 11 non-predicate columns
// from append_wide_extra_columns() (record_shape=wide), kBatchSize rows each - the same wire
// shape ColumnarRows/populate_event parse in production, built via the same DSL
// test_worker_pool.cpp's columnar_payload() helper uses.
std::vector<char> generate_batch(std::mt19937& rng, bool wide) {
    auto exchanges = exchange_pool();
    auto symbols = symbol_pool();
    std::uniform_real_distribution<double> price(0.0, kPriceMax);
    std::uniform_int_distribution<std::size_t> exch_idx(0, exchanges.size() - 1);
    std::uniform_int_distribution<std::size_t> sym_idx(0, symbols.size() - 1);
    std::uniform_int_distribution<std::int64_t> volume(1, 5000);

    zerialize::dyn::Value::Array prices, exchange_vals, symbol_vals, volume_vals;
    prices.reserve(kBatchSize);
    exchange_vals.reserve(kBatchSize);
    symbol_vals.reserve(kBatchSize);
    volume_vals.reserve(kBatchSize);
    for (std::size_t i = 0; i < kBatchSize; ++i) {
        prices.push_back(zerialize::dyn::Value(price(rng)));
        exchange_vals.push_back(zerialize::dyn::Value(exchanges[exch_idx(rng)].c_str()));
        symbol_vals.push_back(zerialize::dyn::Value(symbols[sym_idx(rng)].c_str()));
        volume_vals.push_back(zerialize::dyn::Value(volume(rng)));
    }

    zerialize::dyn::Value::Map columns = {
        {"price", zerialize::dyn::Value::array(std::move(prices))},
        {"exchange", zerialize::dyn::Value::array(std::move(exchange_vals))},
        {"symbol", zerialize::dyn::Value::array(std::move(symbol_vals))},
        {"trade_volume", zerialize::dyn::Value::array(std::move(volume_vals))},
    };
    if (wide) append_wide_extra_columns(columns, rng);

    auto buf = zerialize::serialize<zerialize::MsgPack>(zerialize::dyn::Value::map(std::move(columns)));
    return std::vector<char>(reinterpret_cast<const char*>(buf.data()),
                             reinterpret_cast<const char*>(buf.data()) + buf.size());
}

void check_arrow(const arrow::Status& st, const char* what) {
    if (!st.ok()) throw std::runtime_error(std::string("arrow: ") + what + ": " + st.ToString());
}

// One real Arrow IPC-stream batch: a single RecordBatch with the same 4 (narrow) or 15 (wide)
// columns as generate_batch()'s msgpack payload, and the same value distributions - the shape
// pg_arrow's own rows_to_arrow() produces in production (per build_arrow_ipc_stream's own comment
// in test_arrow_columnar.cpp, already verified against pg_arrow.cpp there). MakeStreamWriter,
// never NewFileWriter - no footer/magic bytes, matching ArrowColumnarRows' own constructor
// expectations. The 11 extra wide-mode columns use Int64Builder (timestamps/ids) or StringBuilder
// (short indicator codes), matching their msgpack-side types.
std::vector<char> generate_arrow_batch(std::mt19937& rng, bool wide) {
    auto exchanges = exchange_pool();
    auto symbols = symbol_pool();
    std::uniform_real_distribution<double> price(0.0, kPriceMax);
    std::uniform_int_distribution<std::size_t> exch_idx(0, exchanges.size() - 1);
    std::uniform_int_distribution<std::size_t> sym_idx(0, symbols.size() - 1);
    std::uniform_int_distribution<std::int64_t> volume(1, 5000);

    arrow::DoubleBuilder price_builder;
    arrow::StringBuilder exchange_builder;
    arrow::StringBuilder symbol_builder;
    arrow::Int64Builder volume_builder;
    for (std::size_t i = 0; i < kBatchSize; ++i) {
        check_arrow(price_builder.Append(price(rng)), "append price");
        check_arrow(exchange_builder.Append(exchanges[exch_idx(rng)]), "append exchange");
        check_arrow(symbol_builder.Append(symbols[sym_idx(rng)]), "append symbol");
        check_arrow(volume_builder.Append(volume(rng)), "append trade_volume");
    }
    std::shared_ptr<arrow::Array> price_arr, exchange_arr, symbol_arr, volume_arr;
    check_arrow(price_builder.Finish(&price_arr), "finish price array");
    check_arrow(exchange_builder.Finish(&exchange_arr), "finish exchange array");
    check_arrow(symbol_builder.Finish(&symbol_arr), "finish symbol array");
    check_arrow(volume_builder.Finish(&volume_arr), "finish trade_volume array");

    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("price", price_arr->type(), /*nullable=*/true),
        arrow::field("exchange", exchange_arr->type(), /*nullable=*/true),
        arrow::field("symbol", symbol_arr->type(), /*nullable=*/true),
        arrow::field("trade_volume", volume_arr->type(), /*nullable=*/true),
    };
    std::vector<std::shared_ptr<arrow::Array>> columns = {price_arr, exchange_arr, symbol_arr, volume_arr};

    if (wide) {
        static const std::vector<std::string> sale_conditions = {"@", "F", "T", "@FI", "@FT"};
        static const std::vector<std::string> trade_stop_indicators = {" ", "T"};
        static const std::vector<std::string> trade_correction_indicators = {"00", "01", "02"};
        static const std::vector<std::string> sources_of_trade = {"C", "N", "E"};
        static const std::vector<std::string> trade_reporting_facilities = {" ", "D", "Q"};
        static const std::vector<std::string> trade_through_exempt_indicators = {"0", "1"};
        std::uniform_int_distribution<std::size_t> sc(0, sale_conditions.size() - 1);
        std::uniform_int_distribution<std::size_t> ts(0, trade_stop_indicators.size() - 1);
        std::uniform_int_distribution<std::size_t> tc(0, trade_correction_indicators.size() - 1);
        std::uniform_int_distribution<std::size_t> so(0, sources_of_trade.size() - 1);
        std::uniform_int_distribution<std::size_t> tf(0, trade_reporting_facilities.size() - 1);
        std::uniform_int_distribution<std::size_t> te(0, trade_through_exempt_indicators.size() - 1);

        arrow::Int64Builder time_b, seq_b, trade_id_b, participant_ts_b, trf_ts_b;
        arrow::StringBuilder sale_cond_b, trade_stop_b, trade_correction_b, source_b, trf_facility_b, exempt_b;
        for (std::size_t i = 0; i < kBatchSize; ++i) {
            std::int64_t base_ts = 34200000000000LL + static_cast<std::int64_t>(i) * 1000;
            check_arrow(time_b.Append(base_ts), "append time");
            check_arrow(sale_cond_b.Append(sale_conditions[sc(rng)]), "append sale_condition");
            check_arrow(trade_stop_b.Append(trade_stop_indicators[ts(rng)]), "append trade_stop_indicator");
            check_arrow(trade_correction_b.Append(trade_correction_indicators[tc(rng)]), "append trade_correction_indicator");
            check_arrow(seq_b.Append(static_cast<std::int64_t>(i)), "append sequence_number");
            check_arrow(trade_id_b.Append(base_ts + static_cast<std::int64_t>(i)), "append trade_id");
            check_arrow(source_b.Append(sources_of_trade[so(rng)]), "append source_of_trade");
            check_arrow(trf_facility_b.Append(trade_reporting_facilities[tf(rng)]), "append trade_reporting_facility");
            check_arrow(participant_ts_b.Append(base_ts), "append participant_timestamp");
            check_arrow(trf_ts_b.Append(base_ts), "append trf_timestamp");
            check_arrow(exempt_b.Append(trade_through_exempt_indicators[te(rng)]), "append trade_through_exempt_indicator");
        }
        struct Extra { const char* name; arrow::ArrayBuilder* builder; };
        Extra extras[] = {
            {"time", &time_b}, {"sale_condition", &sale_cond_b},
            {"trade_stop_indicator", &trade_stop_b}, {"trade_correction_indicator", &trade_correction_b},
            {"sequence_number", &seq_b}, {"trade_id", &trade_id_b},
            {"source_of_trade", &source_b}, {"trade_reporting_facility", &trf_facility_b},
            {"participant_timestamp", &participant_ts_b}, {"trf_timestamp", &trf_ts_b},
            {"trade_through_exempt_indicator", &exempt_b},
        };
        for (auto& extra : extras) {
            std::shared_ptr<arrow::Array> arr;
            check_arrow(extra.builder->Finish(&arr), "finish extra column");
            fields.push_back(arrow::field(extra.name, arr->type(), /*nullable=*/true));
            columns.push_back(arr);
        }
    }

    auto arrow_schema = arrow::schema(fields);
    auto batch = arrow::RecordBatch::Make(arrow_schema, static_cast<int64_t>(kBatchSize), columns);

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
    std::string record_shape = argc > 8 ? argv[8] : "narrow";

    if (input_format != "arrow" && input_format != "msgpack") {
        std::fprintf(stderr, "unknown input_format '%s' (expected arrow|msgpack)\n",
                     input_format.c_str());
        return 1;
    }
    if (publish != "fake" && publish != "real") {
        std::fprintf(stderr, "unknown publish '%s' (expected fake|real)\n", publish.c_str());
        return 1;
    }
    if (record_shape != "narrow" && record_shape != "wide") {
        std::fprintf(stderr, "unknown record_shape '%s' (expected narrow|wide)\n",
                     record_shape.c_str());
        return 1;
    }
    bool use_arrow = (input_format == "arrow");
    bool wide = (record_shape == "wide");

    auto log = std::make_shared<spdlog::logger>(
        "sidecar_pipeline_bench", std::make_shared<spdlog::sinks::null_sink_mt>());

    std::vector<sidecar::attribute_def> attrs = {
        {"price", sidecar::attribute_type::float_val},
        {"exchange", sidecar::attribute_type::string},
        {"symbol", sidecar::attribute_type::string},
        {"trade_volume", sidecar::attribute_type::integer},
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
        batches.push_back(use_arrow ? generate_arrow_batch(row_rng, wide) : generate_batch(row_rng, wide));
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
                "worker_threads=%u  engine=%s  input_format=%s  publish=%s  record_shape=%s\n",
               k, actual_k, rejected, total_rows, total_batches, kBatchSize, worker_threads,
               engine_name(engine), input_format.c_str(), publish.c_str(), record_shape.c_str());

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
    double avg_match_us = stats.match_time_count > 0
        ? (double(stats.match_time_cycles_total) / sidecar::cycles_per_microsecond())
              / double(stats.match_time_count)
        : 0.0;

    std::printf("\nenqueued=%zu (of %zu batches) processed=%llu matched=%llu published=%llu\n",
               enqueued, total_batches, (unsigned long long)stats.processed,
               (unsigned long long)stats.matched, (unsigned long long)stats.published);
    std::printf("wall_clock=%.3fs  true_rows_per_sec=%.1f  avg_fanout_us=%.3f  avg_match_us=%.3f\n",
               seconds, rows_per_sec, avg_fanout_us, avg_match_us);

    pool.stop();
    work_guard.reset();
    ioc.stop();
    io_thread.join();

    return 0;
}
