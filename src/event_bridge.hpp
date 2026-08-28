#pragma once

#include "arrow_columnar_rows.hpp"
#include "config.hpp"
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

    explicit attribute_schema(const std::vector<attribute_def>& defs) {
        for (const auto& d : defs) {
            types[d.name] = d.type;
        }
    }

    std::optional<attribute_type> lookup(std::string_view name) const {
        auto it = types.find(name);
        if (it != types.end()) return it->second;
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
//
// search_time_out, if non-null, is set to the wall-clock duration of the
// matching_engine::search() call alone (not deserialize/populate) whenever
// search actually runs - left untouched (still nullopt) if populate_event
// bails out first or search throws, so callers can distinguish "no search
// happened" from "search took some time" without a sentinel duration.
template <typename Reader>
std::optional<std::vector<uint64_t>> match_message(
    const matching_engine& tree,
    const attribute_schema& schema,
    Reader& reader,
    event_sink& event,
    const std::shared_ptr<spdlog::logger>& log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr)
{
    if (!populate_event(event, schema, reader, log)) {
        return std::nullopt;
    }

    try {
        auto t0 = std::chrono::steady_clock::now();
        auto result = tree.search(event);
        auto t1 = std::chrono::steady_clock::now();
        if (search_time_out) {
            *search_time_out = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0);
        }
        return result;
    } catch (const std::exception& e) {
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
    const std::shared_ptr<spdlog::logger>& log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr)
{
    auto event = tree.make_event();
    return match_message(tree, schema, reader, *event, log, search_time_out);
}

// Top-level entry: deserialize raw bytes according to format, then match.
// See match_message() for search_time_out's semantics.
std::optional<std::vector<uint64_t>> deserialize_and_match(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    const std::shared_ptr<spdlog::logger>& log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr);

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
// rows_searched_out, if non-null, is set to the batch's row count (for
// worker_pool's avg_match_us accounting - one message can now represent many
// searches). search_time_out accumulates total nanoseconds across every
// row's tree.search() call.
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
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr,
    std::size_t* rows_searched_out = nullptr,
    std::optional<binary_format> output_format = std::nullopt);

} // namespace sidecar
