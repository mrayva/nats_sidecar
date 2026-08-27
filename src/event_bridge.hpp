#pragma once

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
#include <unordered_map>
#include <unordered_set>

namespace sidecar {

// Precomputed lookup: attribute name -> schema definition
struct attribute_schema {
    std::unordered_map<std::string, attribute_type> types;

    explicit attribute_schema(const std::vector<attribute_def>& defs) {
        for (const auto& d : defs) {
            types[d.name] = d.type;
        }
    }

    std::optional<attribute_type> lookup(const std::string& name) const {
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
    std::shared_ptr<spdlog::logger> log)
{
    if (!reader.isMap()) {
        if (log) log->debug("event_bridge: payload is not a map");
        return false;
    }

    auto keys = reader.mapKeys();
    for (auto key_sv : keys) {
        std::string key(key_sv);

        auto type_opt = schema.lookup(key);
        if (!type_opt) continue;

        auto value = reader[key_sv];

        try {
            switch (*type_opt) {
                case attribute_type::boolean:
                    if (value.isBool()) {
                        builder.with_boolean(key, value.asBool());
                    } else {
                        builder.with_undefined(key);
                    }
                    break;

                case attribute_type::integer:
                    if (value.isInt() || value.isUInt()) {
                        builder.with_integer(key, value.asInt64());
                    } else {
                        builder.with_undefined(key);
                    }
                    break;

                case attribute_type::float_val:
                    if (value.isFloat()) {
                        builder.with_float(key, value.asDouble());
                    } else if (value.isInt() || value.isUInt()) {
                        builder.with_float(key, static_cast<double>(value.asInt64()));
                    } else {
                        builder.with_undefined(key);
                    }
                    break;

                case attribute_type::string:
                    if (value.isString()) {
                        builder.with_string(key, std::string(value.asStringView()));
                    } else {
                        builder.with_undefined(key);
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
                        builder.with_string_list(key, list);
                    } else {
                        builder.with_undefined(key);
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
                        builder.with_integer_list(key, list);
                    } else {
                        builder.with_undefined(key);
                    }
                    break;
            }
        } catch (const std::exception& e) {
            if (log) log->debug("event_bridge: failed to extract field '{}': {}", key, e.what());
            try { builder.with_undefined(key); } catch (...) {}
        }
    }

    return true;
}

// Match a deserialized message against all active subscriptions.
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
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr)
{
    auto event = tree.make_event();

    if (!populate_event(*event, schema, reader, log)) {
        return std::nullopt;
    }

    try {
        auto t0 = std::chrono::steady_clock::now();
        auto result = tree.search(*event);
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

// Top-level entry: deserialize raw bytes according to format, then match.
// See match_message() for search_time_out's semantics.
std::optional<std::vector<uint64_t>> deserialize_and_match(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr);

// One row of a columnar batch that matched at least one subscription.
struct row_match {
    std::vector<uint64_t> matched_ids;
    std::vector<char> payload;   // this row's own standalone bytes, same format as the input batch
};

// Writes `row` (one element of a zerialize::expand_columnar() result - a
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
// config-validation time before this function is ever called): expand_columnar
// materializes a root-level array, and BSON's wire format cannot round-trip
// a root-level array (a document and an array are byte-identical on the
// wire; only a *parent* element's header records which one a value is, and
// the root has no parent - see zerialize/protocols/bson.hpp and
// zerialize/test/test_zerialize.cpp's test_bson_specific() for why BSON is
// excluded from zerialize's own generic protocol test harness for the same
// reason).
std::optional<std::vector<row_match>> deserialize_and_match_columnar(
    const matching_engine& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log,
    std::optional<std::chrono::nanoseconds>* search_time_out = nullptr,
    std::size_t* rows_searched_out = nullptr);

} // namespace sidecar
