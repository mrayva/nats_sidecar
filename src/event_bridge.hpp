#pragma once

#include "config.hpp"
#include <atree.hpp>
#include <limits>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/zera.hpp>
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

// Populate an EventBuilder from a zerialize reader using the schema.
template <typename Reader>
bool populate_event(
    atree::EventBuilder& builder,
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
template <typename Reader>
std::optional<std::vector<uint64_t>> match_message(
    const atree::Tree& tree,
    const attribute_schema& schema,
    Reader& reader,
    std::shared_ptr<spdlog::logger> log)
{
    auto event = tree.make_event();

    if (!populate_event(event, schema, reader, log)) {
        return std::nullopt;
    }

    try {
        return tree.search(std::move(event));
    } catch (const std::exception& e) {
        if (log) log->warn("event_bridge: a-tree search failed: {}", e.what());
        return std::nullopt;
    }
}

// Top-level entry: deserialize raw bytes according to format, then match.
std::optional<std::vector<uint64_t>> deserialize_and_match(
    const atree::Tree& tree,
    const attribute_schema& schema,
    binary_format format,
    std::span<const char> payload,
    std::shared_ptr<spdlog::logger> log);

} // namespace sidecar
