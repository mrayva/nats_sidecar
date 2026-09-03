#pragma once

#include "arrow_columnar_rows.hpp"
#include "config.hpp"
#include "match_timing.hpp"
#include "matching_engine.hpp"
#include <chrono>
#include <limits>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/zera.hpp>
#include <zerialize/protocols/ion.hpp>
#include <zerialize/protocols/bson.hpp>
#include <zerialize/protocols/beve.hpp>
#include <spdlog/spdlog.h>
#include <span>
#include <string>
#include <vector>
#include <optional>
#include <unordered_set>

namespace sidecar {

// Precomputed lookup: attribute name -> schema definition. Looked up by
// string_view_lookup_map<> (matching_engine.hpp) - heterogeneous find() by
// std::string_view, not requiring a caller to already have an owned
// std::string just to look a name up (see populate_event() below, which
// looks this up once per (row, attribute) pair).
struct attribute_schema {
    string_view_lookup_map<attribute_type> types;
    // Meaningful only for attribute_type::decimal entries - the one canonical scale (see
    // pstree/pst_dynamic.hpp's AttrSchema::decimalScale) every value reaching that attribute
    // gets rescaled to, event or subscription literal alike. Kept as its own lookup rather than
    // folded into `types` since every other attribute_type has no equivalent per-attribute
    // config to carry.
    string_view_lookup_map<std::int32_t> decimalScales;

    explicit attribute_schema(const std::vector<attribute_def>& defs) {
        for (const auto& d : defs) {
            types[d.name] = d.type;
            if (d.type == attribute_type::decimal && d.decimal_scale) {
                decimalScales[d.name] = *d.decimal_scale;
            }
        }
    }

    std::optional<attribute_type> lookup(std::string_view name) const {
        auto it = types.find(name);
        if (it != types.end()) return it->second;
        return std::nullopt;
    }

    std::optional<std::int32_t> decimalScaleFor(std::string_view name) const {
        auto it = decimalScales.find(name);
        if (it != decimalScales.end()) return it->second;
        return std::nullopt;
    }
};

// Populate an event_sink from a zerialize reader using the schema.
template <typename Reader>
bool populate_event(
    event_sink& builder,
    const attribute_schema& schema,
    Reader& reader,
    const std::shared_ptr<spdlog::logger>& log)
{
    if (!reader.isMap()) {
        if (log) log->debug("event_bridge: payload is not a map");
        return false;
    }

    auto keys = reader.mapKeys();
    for (auto key_sv : keys) {
        // key_sv is used directly as the lookup key and the event_sink name
        // argument below - no owned std::string needed for either: schema
        // lookup() takes string_view via string_view_lookup_map<>'s
        // heterogeneous find(), and event_sink's with_*() methods all take
        // string_view too (see matching_engine.hpp) - constructing a
        // std::string here would just be an allocation neither consumer
        // needs.
        auto type_opt = schema.lookup(key_sv);
        if (!type_opt) continue;

        auto value = reader[key_sv];

        try {
            switch (*type_opt) {
                case attribute_type::boolean:
                    if (value.isBool()) {
                        builder.with_boolean(key_sv, value.asBool());
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::integer:
                    if (value.isInt() || value.isUInt()) {
                        builder.with_integer(key_sv, value.asInt64());
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::float_val:
                    if (value.isFloat()) {
                        builder.with_float(key_sv, value.asDouble());
                    } else if (value.isInt() || value.isUInt()) {
                        builder.with_float(key_sv, static_cast<double>(value.asInt64()));
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::string:
                    if (value.isString()) {
                        builder.with_string(key_sv, value.asStringView());
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::decimal:
                    // Only ArrowColumnarRows's own cell view has isDecimal()/asDecimal() - no
                    // other Reader (every pg_zerialize-backed format) has a native decimal
                    // concept at all (see arrow_columnar_rows.hpp's own header comment). This
                    // populate_event() template is instantiated once per Reader type, so the
                    // `if constexpr` guard is load-bearing, not decorative: without it, this
                    // switch case would fail to COMPILE for every non-Arrow Reader, not just
                    // fail at runtime - same idiom zerialize's own translate.hpp already uses
                    // for raw_copy_safe()/mapEntries()/elements(), confirmed before relying on
                    // it here too.
                    if constexpr (requires (std::int32_t s) { value.isDecimal(); value.asDecimal(s); }) {
                        auto scale = schema.decimalScaleFor(key_sv);
                        if (scale && value.isDecimal()) {
                            builder.with_decimal(key_sv, value.asDecimal(*scale));
                        } else {
                            builder.with_undefined(key_sv);
                        }
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::string_list:
                    if (value.isArray()) {
                        std::vector<std::string> list;
                        auto sz = value.arraySize();
                        list.reserve(sz);
                        for (size_t i = 0; i < sz; ++i) {
                            auto elem = value[i];
                            if (elem.isString()) list.emplace_back(elem.asString());
                        }
                        builder.with_string_list(key_sv, list);
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;

                case attribute_type::integer_list:
                    if (value.isArray()) {
                        std::vector<int64_t> list;
                        auto sz = value.arraySize();
                        list.reserve(sz);
                        for (size_t i = 0; i < sz; ++i) {
                            auto elem = value[i];
                            if (elem.isInt() || elem.isUInt()) list.push_back(elem.asInt64());
                        }
                        builder.with_integer_list(key_sv, list);
                    } else {
                        builder.with_undefined(key_sv);
                    }
                    break;
            }
        } catch (const std::exception& e) {
            if (log) log->debug("event_bridge: failed to extract field '{}': {}", key_sv, e.what());
            try { builder.with_undefined(key_sv); } catch (...) {}
        }
    }

    return true;
}

// Match a deserialized message against all active subscriptions, using a
// caller-supplied, already-`tree.make_event()`d event_sink. Exists
// separately from the convenience overload below so a caller processing
// many rows against the same tree (a columnar batch) can supply *one*
// event_sink reused across all of them (when matching_engine::reuses_events()
// says that's safe for this engine - see matching_engine.hpp) instead of a
// fresh one per row.
template <typename Reader>
std::optional<std::vector<uint64_t>> match_message(
    const matching_engine& tree,
    const attribute_schema& schema,
    Reader& reader,
    event_sink& event,
    const std::shared_ptr<spdlog::logger>& log)
{
    if (!populate_event(event, schema, reader, log)) {
        return std::nullopt;
    }

    const std::uint64_t cycles_start = read_cycles();
    try {
        auto result = tree.search(event);
        record_match_cycles(read_cycles() - cycles_start);
        return result;
    } catch (const std::exception& e) {
        record_match_cycles(read_cycles() - cycles_start);
        if (log) log->warn("event_bridge: matching engine search failed: {}", e.what());
        return std::nullopt;
    }
}

// Convenience overload: makes its own one-shot event_sink. Fine for a
// caller that only matches a single message against `tree` (row-mode
// deserialize_and_match() below, called once per input message) - no
// reuse to benefit from there.
template <typename Reader>
std::optional<std::vector<uint64_t>> match_message(
    const matching_engine& tree,
    const attribute_schema& schema,
    Reader& reader,
    const std::shared_ptr<spdlog::logger>& log)
{
    auto event = tree.make_event();
    return match_message(tree, schema, reader, *event, log);
}

// Count-only sibling of match_message() above - calls matching_engine::search_count() instead
// of search(), so no std::vector<uint64_t> of matched ids is ever built. Precondition:
// tree.supports_count() must be true (only pstree does today) - a caller violating this gets
// matching_engine_error thrown from search_count()'s own base-class default, which this function
// folds into its usual nullopt/"row failed" return, same as any other row-level failure -
// see count_match_columnar_batch()'s own comment for why a caller-side gating bug here fails
// safe rather than silently miscounting. Deliberately does NOT record RDTSC match-timing cycles
// the way match_message() does (see match_timing.hpp) - folding a cheap count-only pass into the
// same avg_match_us stat as a real search() would quietly change what that stat measures
// specifically under the high-pressure conditions this path exists for; left as an explicit,
// separate follow-up rather than done as a side effect of this change.
template <typename Reader>
std::optional<std::size_t> count_message(
    const matching_engine& tree,
    const attribute_schema& schema,
    Reader& reader,
    event_sink& event,
    const std::shared_ptr<spdlog::logger>& log)
{
    if (!populate_event(event, schema, reader, log)) {
        return std::nullopt;
    }

    try {
        return tree.search_count(event);
    } catch (const std::exception& e) {
        if (log) log->warn("event_bridge: matching engine search_count failed: {}", e.what());
        return std::nullopt;
    }
}

// Convenience overload, mirrors match_message()'s own: makes its own one-shot event_sink.
template <typename Reader>
std::optional<std::size_t> count_message(
    const matching_engine& tree,
    const attribute_schema& schema,
    Reader& reader,
    const std::shared_ptr<spdlog::logger>& log)
{
    auto event = tree.make_event();
    return count_message(tree, schema, reader, *event, log);
}

// Top-level entry: deserialize raw bytes according to format, then match.
std::optional<std::vector<uint64_t>> deserialize_and_match(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log);

// One row of a columnar batch that matched at least one subscription.
struct row_match {
    std::vector<uint64_t> matched_ids;
    std::vector<char> payload;   // this row's own standalone bytes, in the connection's output_format
                                  // (config::output_format if set, else the same format as the input
                                  // batch - see deserialize_and_match_columnar's own comment)
};

// Writes `row` (one row of a zerialize::columnar_rows() view - a
// normal row-shaped map reader) into its own standalone document of the
// same protocol. Mirrors zerialize::translate<DstP>()'s internal body
// (RootSerializer + Writer + write_value + finish()) but returns raw bytes
// instead of wrapping them in a Deserializer, since worker_pool publishes
// raw spans, not Deserializer objects.
template <typename Protocol, typename Reader>
std::vector<char> serialize_row(const Reader& row) {
    typename Protocol::RootSerializer rs{};
    typename Protocol::Serializer w{rs};
    zerialize::write_value(row, w);
    zerialize::ZBuffer out = rs.finish();
    const char* data = reinterpret_cast<const char*>(out.data());
    return std::vector<char>(data, data + out.size());
}

// Top-level entry for a columnar-batched connection: unpacks `payload` (a
// zerialize columnar record - see zerialize/columnar.hpp) into its N rows
// and matches each row independently. nullopt means the whole batch is
// malformed (poison, same contract as deserialize_and_match); an empty
// vector means the batch was well-formed but no row matched anything.
//
// BSON is not supported here (format == binary_format::bson is rejected at
// config-validation time before this function is ever called, and has no
// case below). Historically this was because expand_columnar() materialized
// a root-level array, which BSON's wire format can't round-trip (a document
// and an array are byte-identical on the wire; only a *parent* element's
// header records which one a value is, and the root has no parent - see
// zerialize/protocols/bson.hpp). match_columnar_batch() no longer calls
// expand_columnar() (see its own comment), so that specific blocker no
// longer applies - every root value this path produces is a document (the
// source columnar record, and each matched row's own standalone payload),
// never an array. BSON support could plausibly be added now, but that's a
// deliberate scope decision someone still needs to make (new test coverage,
// a config-validation change, a README update) - not a side effect of this
// perf change, so the exclusion stays as-is for now.
// `output_format` selects the republish encoding for matched rows, decoupled from `format`
// (the input encoding) - see config::output_format's own comment. `nullopt` (the default) means
// "same as `format`" - today's behavior, zero change for every caller that doesn't care about
// the distinction. Only `format == binary_format::arrow` may pass a genuinely different,
// explicit `output_format` - finalize_and_validate_config() (cli.cpp) enforces this at startup,
// but this function does not re-validate it, so a caller bypassing that (e.g. a future direct
// programmatic use) that passes a mismatched pair for a non-arrow `format` gets
// undefined-but-safe behavior: the explicit `output_format` wins, silently diverging from
// `format`, not a crash - validate before calling if that matters.
std::optional<std::vector<row_match>> deserialize_and_match_columnar(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log,
    std::optional<binary_format> output_format = std::nullopt);

// Aggregate result of a count-only pass over a columnar batch, via count_match_columnar_batch()
// below - deliberately NOT a per-row vector (unlike row_match): nothing downstream needs per-row
// granularity past this sum, since this path never retains ids or payloads. A count-only pass and
// the eventual real pass, if one ever happens, are two SEPARATE walks over the same raw bytes -
// there is no way to "keep" a count-pass's populated event_sink around for later reuse (a batch's
// rows all share ONE reused event_sink, repopulated in place row by row - see
// match_columnar_batch()'s own comment in event_bridge.cpp - so by the time row N+1 has been
// counted, row N's populated data is already gone).
struct columnar_count_estimate {
    // Number of rows with search_count() > 0 - mirrors row_match's own "only non-empty rows get
    // an entry" convention, and lets a caller (worker_pool.cpp) feed its own `matched` stat the
    // same value it would have gotten from the real path.
    std::size_t matched_row_count = 0;
    // Sum, across matched rows, of search_count()'s own real (exact) per-row count.
    std::size_t total_match_count = 0;
    // sum over matched rows of (that row's search_count() * (avg_row_payload_bytes + 64)), the
    // same formula/+64-per-frame-overhead constant worker_pool's own real estimated_bytes
    // calculation already uses for the direct path - just fed by avg_row_payload_bytes
    // (payload.size() / row_count, a uniform per-row average) instead of each row's own real,
    // exact serialize_row() size, which neither zerialize::ColumnarRows<V> nor ArrowColumnarRows
    // can report without actually re-encoding the row (verified by reading both in full) - the
    // exact cost a count-only pass exists to avoid paying for a row that may never get
    // published. This affects ONLY the ACCURACY of the byte-based backpressure ESTIMATE on the
    // count-only path - "estimated_bytes" was already documented as an approximation before this
    // struct existed (see worker_pool.cpp's own comment on its direct-path computation) - it
    // never changes the backpressure decision LOGIC (identical comparison/atomics/thresholds on
    // both paths), and it never changes what ultimately publishes: once a message is confirmed
    // kept, its real ids/payloads always come from an unchanged, exact
    // deserialize_and_match_columnar() call, never from this estimate.
    std::size_t estimated_bytes = 0;
};

// Count-only sibling of deserialize_and_match_columnar() above: same format dispatch (including
// the Arrow special case) and the same malformed/nullopt vs. well-formed contract, but calls
// matching_engine::search_count() per row instead of search(), and never calls serialize_row() at
// all - no OutProtocol/output_format needed here (unlike deserialize_and_match_columnar), since
// this path never re-encodes a row. Precondition: tree.supports_count() must be true - a caller
// (worker_pool.cpp) is expected to have already checked this (via the engine_type it knows it
// configured) before calling; violating it surfaces as matching_engine_error from
// search_count()'s own base-class default, caught here like any other row-match failure and
// folded into the usual nullopt/"poison batch" return - a caller-side gating bug here fails safe
// (the caller falls back to its own unchanged direct path) rather than silently miscounting.
std::optional<columnar_count_estimate> count_match_columnar_batch(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log);

} // namespace sidecar
