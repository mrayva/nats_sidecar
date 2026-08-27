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
    auto rows = zerialize::expand_columnar<Protocol>(reader);
    const std::size_t n = rows.arraySize();
    if (rows_searched_out) *rows_searched_out = n;

    std::vector<row_match> result;
    std::chrono::nanoseconds total_search_time{0};
    bool any_search_time = false;

    for (std::size_t i = 0; i < n; ++i) {
        auto row = rows[i];
        std::optional<std::chrono::nanoseconds> row_search_time;
        auto matches = match_message(tree, schema, row, log, &row_search_time);
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
            return std::nullopt;
        }
        if (!matches->empty()) {
            result.push_back({std::move(*matches), serialize_row<Protocol>(row)});
        }
    }

    if (search_time_out && any_search_time) *search_time_out = total_search_time;
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
