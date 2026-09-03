#include "event_bridge.hpp"

namespace sidecar {

std::optional<std::vector<uint64_t>> deserialize_and_match(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log)
{
    try {
        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        switch (format) {
            case binary_format::msgpack: {
                zerialize::MsgPack::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::cbor: {
                zerialize::CBOR::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::flexbuffers: {
                zerialize::Flex::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::zera: {
                zerialize::Zera::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::ion: {
                zerialize::Ion::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::bson: {
                zerialize::Bson::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::beve: {
                zerialize::Beve::Deserializer reader(bytes);
                return match_message(tree, schema, reader, log);
            }
            case binary_format::arrow:
                // Arrow has no row-mode reader at all (it's inherently columnar - see
                // arrow_columnar_rows.hpp's own file-level comment). Rejected at
                // config-validation time (finalize_and_validate_config requires every
                // connection to be columnar when format=arrow) - a row-mode arrow message
                // should never reach this call at all.
                if (log) {
                    log->error("event_bridge: row-mode matching is not supported for "
                               "format=arrow (Arrow is columnar-only)");
                }
                return std::nullopt;
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

namespace {

// `Rows` is deduced from the `rows` argument (either a zerialize::ColumnarRows<Protocol::
// Deserializer> for the 6 existing columnar-capable formats, or an ArrowColumnarRows - see
// arrow_columnar_rows.hpp); `OutProtocol` is always explicit at the call site, since nothing
// about `rows`'s own type determines what format matched rows should republish in. This split
// (originally `template<typename Protocol>` deriving both the reader AND the writer from one
// type) is what lets input and output format decouple - see config::output_format's own
// comment for why (Arrow has no single-row encoder, so it can never be its own OutProtocol).
template <typename OutProtocol, typename Rows>
std::optional<std::vector<row_match>> match_columnar_batch(
    const matching_engine& tree,
    const attribute_schema& schema,
    Rows& rows,
    const std::shared_ptr<spdlog::logger>& log)
{
    std::vector<row_match> result;

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
        auto matches = reuse_event
            ? match_message(tree, schema, row, *reused_event, log)
            : match_message(tree, schema, row, log);
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
            result.push_back({std::move(*matches), serialize_row<OutProtocol>(row)});
        }
        return true;
    };

    bool ok = true;
    std::size_t i = 0;
    for (auto&& row : rows) {
        if (!process_row(i++, row)) { ok = false; break; }
    }
    if (!ok) return std::nullopt;

    return result;
}

// Dispatches on `out_fmt` to pick OutProtocol and call match_columnar_batch - the second half
// of the double-dispatch that only Arrow input actually needs (see deserialize_and_match_columnar
// below: non-arrow input uses `format`'s own Protocol directly, without going through this).
// `arrow` itself is never a valid `out_fmt` - config-validation (cli.cpp) rejects it at startup;
// reachable here only if that guard is bypassed.
template <typename Rows>
std::optional<std::vector<row_match>> dispatch_columnar_output(
    const matching_engine& tree,
    const attribute_schema& schema,
    Rows& rows,
    binary_format out_fmt,
    const std::shared_ptr<spdlog::logger>& log)
{
    switch (out_fmt) {
        case binary_format::msgpack:
            return match_columnar_batch<zerialize::MsgPack>(tree, schema, rows, log);
        case binary_format::cbor:
            return match_columnar_batch<zerialize::CBOR>(tree, schema, rows, log);
        case binary_format::flexbuffers:
            return match_columnar_batch<zerialize::Flex>(tree, schema, rows, log);
        case binary_format::zera:
            return match_columnar_batch<zerialize::Zera>(tree, schema, rows, log);
        case binary_format::ion:
            return match_columnar_batch<zerialize::Ion>(tree, schema, rows, log);
        case binary_format::beve:
            return match_columnar_batch<zerialize::Beve>(tree, schema, rows, log);
        case binary_format::bson:
            // Valid as an OUTPUT format even for a columnar connection, unlike bson as an
            // INPUT format (rejected below): serialize_row() only ever encodes one row (a
            // document), never the batch's own root-level array, so BSON's root-array
            // limitation (see the input-side rejection's own comment) never applies here.
            return match_columnar_batch<zerialize::Bson>(tree, schema, rows, log);
        case binary_format::arrow:
            if (log) log->error("event_bridge: output_format=arrow is not supported (Arrow is read-only)");
            return std::nullopt;
    }
    return std::nullopt;
}

// Count-only sibling of match_columnar_batch<OutProtocol, Rows>() above - same reused-event_sink
// shape, but calls count_message() (search_count()) instead of match_message() (search()), and
// never calls serialize_row() at all, so no OutProtocol template parameter is needed here.
// `total_payload_bytes` is the caller's own payload.size() (the whole batch's wire size, cheaply
// known upfront) - used only to compute columnar_count_estimate::estimated_bytes's own
// avg_row_payload_bytes proxy (see that field's own doc comment in event_bridge.hpp).
template <typename Rows>
std::optional<columnar_count_estimate> count_columnar_rows_batch(
    const matching_engine& tree,
    const attribute_schema& schema,
    Rows& rows,
    std::size_t total_payload_bytes,
    const std::shared_ptr<spdlog::logger>& log)
{
    const bool reuse_event = tree.reuses_events();
    std::unique_ptr<event_sink> reused_event = reuse_event ? tree.make_event() : nullptr;

    columnar_count_estimate estimate;
    std::size_t row_count = 0;

    auto process_row = [&](std::size_t i, auto&& row) -> bool {
        ++row_count;
        auto count = reuse_event
            ? count_message(tree, schema, row, *reused_event, log)
            : count_message(tree, schema, row, log);
        if (!count) {
            // Mirrors match_columnar_batch()'s own "poison the whole batch" contract on a
            // row-level failure - same per-message (not per-row) failure granularity as every
            // other entry point in this file.
            if (log) {
                log->warn("event_bridge: row {} of columnar batch failed to count; "
                          "poisoning whole batch", i);
            }
            return false;
        }
        if (*count > 0) {
            estimate.matched_row_count += 1;
            estimate.total_match_count += *count;
        }
        return true;
    };

    bool ok = true;
    std::size_t i = 0;
    for (auto&& row : rows) {
        if (!process_row(i++, row)) { ok = false; break; }
    }
    if (!ok) return std::nullopt;

    // Uniform per-row average, the only cheap size proxy available (see
    // columnar_count_estimate::estimated_bytes's own doc comment) - guard row_count == 0 (an
    // empty batch) to avoid a division by zero; there is nothing to estimate in that case anyway
    // (total_match_count is already 0).
    std::size_t avg_row_payload_bytes = row_count > 0 ? total_payload_bytes / row_count : 0;
    estimate.estimated_bytes = estimate.total_match_count * (avg_row_payload_bytes + 64);

    return estimate;
}

} // namespace

std::optional<columnar_count_estimate> count_match_columnar_batch(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log)
{
    try {
        // Arrow input: same ArrowColumnarRows reader deserialize_and_match_columnar() uses, but
        // no output-format dispatch is needed here at all (unlike that function's own Arrow
        // branch) - this path never re-encodes a row, so there is no OutProtocol to pick.
        if (format == binary_format::arrow) {
            ArrowColumnarRows rows(payload);
            return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
        }

        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        switch (format) {
            case binary_format::msgpack: {
                zerialize::MsgPack::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::cbor: {
                zerialize::CBOR::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::flexbuffers: {
                zerialize::Flex::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::zera: {
                zerialize::Zera::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::ion: {
                zerialize::Ion::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::beve: {
                zerialize::Beve::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return count_columnar_rows_batch(tree, schema, rows, payload.size(), log);
            }
            case binary_format::bson:
                // Same restriction as deserialize_and_match_columnar()'s own bson-as-input
                // rejection - see that function's own comment.
                if (log) {
                    log->error("event_bridge: columnar batching is not supported "
                              "for format=bson");
                }
                return std::nullopt;
            case binary_format::arrow:
                // Unreachable - handled at the top of this function.
                return std::nullopt;
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: columnar count-pass deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

std::optional<std::vector<row_match>> deserialize_and_match_columnar(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log,
    std::optional<binary_format> output_format)
{
    try {
        // Arrow input is handled entirely separately from the other 6 formats: it has its own
        // reader type (ArrowColumnarRows, not a zerialize columnar_rows() view), and - unlike
        // every other format, which is required to keep output_format == format (v1 scope, see
        // config::output_format's own comment) - it always needs the second output dispatch,
        // since output_format is required to be set and different whenever format == arrow
        // (enforced by finalize_and_validate_config()).
        if (format == binary_format::arrow) {
            ArrowColumnarRows rows(payload);
            if (!output_format) {
                // finalize_and_validate_config() (cli.cpp) requires output_format to be set
                // whenever format=arrow (Arrow has no single-row encoder of its own) -
                // reachable here only if that guard is bypassed.
                if (log) log->error("event_bridge: format=arrow requires output_format to be set");
                return std::nullopt;
            }
            return dispatch_columnar_output(tree, schema, rows, *output_format, log);
        }

        auto bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        // Every non-arrow format keeps output_format == format (v1 scope, enforced at
        // config-validation time) - OutProtocol is `format`'s own Protocol directly, the same
        // single-dispatch shape (and the same template instantiations) as before output_format
        // existed at all. columnar_rows() walks `reader` (the columnar-shaped payload) directly,
        // one row at a time - it does not build the row-major document expand_columnar() used
        // to (a Writer pass, then a fresh Deserializer parsing exactly what was just written
        // straight back in). That write-then-read round trip was itself the single largest CPU
        // cost in this whole request path once the O(n^2) row/column-indexing bugs on both
        // sides of it were fixed (confirmed via perf) - bigger than matching_engine::search(),
        // populate_event(), and everything else in this function combined. See
        // zerialize/columnar.hpp's columnar_rows() doc comment for how it gets the same values
        // without materializing.
        switch (format) {
            case binary_format::msgpack: {
                zerialize::MsgPack::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::MsgPack>(tree, schema, rows, log);
            }
            case binary_format::cbor: {
                zerialize::CBOR::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::CBOR>(tree, schema, rows, log);
            }
            case binary_format::flexbuffers: {
                zerialize::Flex::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::Flex>(tree, schema, rows, log);
            }
            case binary_format::zera: {
                zerialize::Zera::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::Zera>(tree, schema, rows, log);
            }
            case binary_format::ion: {
                zerialize::Ion::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::Ion>(tree, schema, rows, log);
            }
            case binary_format::beve: {
                zerialize::Beve::Deserializer reader(bytes);
                auto rows = zerialize::columnar_rows(reader);
                return match_columnar_batch<zerialize::Beve>(tree, schema, rows, log);
            }
            case binary_format::bson:
                // Rejected at config-validation time (finalize_and_validate_config) -
                // a bson+columnar connection should never reach this call at all.
                // See event_bridge.hpp's deserialize_and_match_columnar doc comment
                // for why (root-level-array round-trip is not safe for BSON as an INPUT
                // format - note this does NOT apply to bson as an *output* format, see
                // dispatch_columnar_output's own bson case above).
                if (log) {
                    log->error("event_bridge: columnar batching is not supported "
                              "for format=bson");
                }
                return std::nullopt;
            case binary_format::arrow:
                // Unreachable - handled at the top of this function. The switch must stay
                // exhaustive regardless.
                return std::nullopt;
        }
    } catch (const std::exception& e) {
        if (log) log->debug("event_bridge: columnar deserialization failed: {}", e.what());
    }

    return std::nullopt;
}

} // namespace sidecar
