#include "event_bridge.hpp"

namespace sidecar {

std::optional<std::vector<uint64_t>> deserialize_and_match(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out)
{
    try {
        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        switch (format) {
            case binary_format::msgpack: {
                zerialize::MsgPack::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::cbor: {
                zerialize::CBOR::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::flexbuffers: {
                zerialize::Flex::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::zera: {
                zerialize::Zera::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::ion: {
                zerialize::Ion::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::bson: {
                zerialize::Bson::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
            case binary_format::beve: {
                zerialize::Beve::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log, search_time_out);
            }
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

namespace {

template <typename Protocol>
std::optional<std::vector<row_match>> match_columnar_batch(
    const matching_engine& tree,
    const attribute_schema& schema,
    std::span<const uint8_t> bytes,
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out,
    std::size_t* rows_searched_out)
{
    typename Protocol::Deserializer reader(bytes);

    // columnar_rows() walks `reader` (the columnar-shaped payload) directly,
    // one row at a time - it does not build the row-major document
    // expand_columnar() used to (a Writer pass, then a fresh Deserializer
    // parsing exactly what was just written straight back in). That
    // write-then-read round trip was itself the single largest CPU cost in
    // this whole request path once the O(n^2) row/column-indexing bugs on
    // both sides of it were fixed (confirmed via perf) - bigger than
    // matching_engine::search(), populate_event(), and everything else in
    // this function combined. See zerialize/columnar.hpp's columnar_rows()
    // doc comment for how it gets the same values without materializing.
    auto rows = zerialize::columnar_rows(reader);
    const std::size_t n = rows.size();
    if (rows_searched_out) *rows_searched_out = n;

    std::vector<row_match> result;
    std::chrono::nanoseconds total_search_time{0};
    bool any_search_time = false;

    // Timing every row's tree.search() call (std::chrono::steady_clock::now(),
    // twice per row) is itself real, measurable overhead on this workload -
    // confirmed via perf, ~4% of total CPU - because individual searches run
    // well under a microsecond here, so the act of timing competes with the
    // thing being timed. Sample instead: only request timing for 1 in
    // kSearchTimeSampleStride rows, then scale the summed sample up by that
    // stride below to estimate the batch's total - worker_pool's avg_match_us
    // (computed from this against the batch's *true*, un-sampled row count,
    // via rows_searched_out) becomes a statistical estimate rather than an
    // exact average, a standard and worthwhile trade here given how large a
    // fraction of the measured quantity the measurement itself was.
    constexpr std::size_t kSearchTimeSampleStride = 8;

    // One event_sink for the whole batch, reused across every row instead
    // of a fresh one per row, when the engine safely supports it
    // (matching_engine::reuses_events() - true for a-tree, false for
    // be-tree; see that method's own comment for why). search() recycles
    // a reused event_sink in place rather than reallocating - a real,
    // separately-measured per-row cost this batch's rows no longer each
    // pay on their own. Engines that don't support reuse keep getting a
    // fresh event_sink every row via match_message()'s own make_event()
    // overload, exactly as before this change.
    const bool reuse_event = tree.reuses_events();
    std::unique_ptr<event_sink> reused_event = reuse_event ? tree.make_event() : nullptr;

    auto process_row = [&](std::size_t i, auto&& row) -> bool {
        std::optional<std::chrono::nanoseconds> row_search_time;
        const bool sample_timing = search_time_out && (i % kSearchTimeSampleStride == 0);
        auto* timing_out = sample_timing ? &row_search_time : nullptr;
        auto matches = reuse_event
            ? match_message(tree, schema, row, *reused_event, log, timing_out)
            : match_message(tree, schema, row, log, timing_out);
        if (row_search_time) {
            total_search_time += *row_search_time;
            any_search_time = true;
        }
        if (!matches) {
            // One row's match failed outright (not just "no match") - treat
            // the whole batch as poisoned, matching deserialize_and_match's
            // existing per-message (not per-row) failure granularity.
            if (log) {
                log->warn("event_bridge: row {} of columnar batch failed to match; "
                          "poisoning whole batch", i);
            }
            return false;
        }
        if (!matches->empty()) {
            result.push_back({std::move(*matches), serialize_row<Protocol>(row)});
        }
        return true;
    };

    bool ok = true;
    std::size_t i = 0;
    for (auto&& row : rows) {
        if (!process_row(i++, row)) { ok = false; break; }
    }
    if (!ok) return std::nullopt;

    if (search_time_out && any_search_time) {
        *search_time_out = total_search_time * kSearchTimeSampleStride;
    }
    return result;
}

} // namespace

std::optional<std::vector<row_match>> deserialize_and_match_columnar(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out,
    std::size_t* rows_searched_out)
{
    try {
        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        switch (format) {
            case binary_format::msgpack:
                return match_columnar_batch<zerialize::MsgPack>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::cbor:
                return match_columnar_batch<zerialize::CBOR>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::flexbuffers:
                return match_columnar_batch<zerialize::Flex>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::zera:
                return match_columnar_batch<zerialize::Zera>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::ion:
                return match_columnar_batch<zerialize::Ion>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::beve:
                return match_columnar_batch<zerialize::Beve>(
                    tree, schema, bytes, log, search_time_out, rows_searched_out);
            case binary_format::bson:
                // Rejected at config-validation time (finalize_and_validate_config) -
                // a bson+columnar connection should never reach this call at all.
                // See event_bridge.hpp's deserialize_and_match_columnar doc comment
                // for why (root-level-array round-trip is not safe for BSON).
                if (log) {
                    log->error("event_bridge: columnar batching is not supported "
                              "for format=bson");
                }
                return std::nullopt;
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: columnar deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

} // namespace sidecar
