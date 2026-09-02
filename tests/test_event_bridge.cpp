#include "event_bridge.hpp"
#include "config.hpp"
#include "subscription_manager.hpp"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/zera.hpp>
#include <zerialize/protocols/ion.hpp>
#include <zerialize/protocols/bson.hpp>
#include <zerialize/protocols/beve.hpp>
#include <algorithm>

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>("test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

std::span<const char> as_char_span(const zerialize::ZBuffer& buf) {
    return std::span<const char>(reinterpret_cast<const char*>(buf.data()), buf.size());
}

bool contains(const std::vector<uint64_t>& ids, uint64_t id) {
    return std::find(ids.begin(), ids.end(), id) != ids.end();
}

} // namespace

TEST(attribute_schema, lookup_known_attribute) {
    std::vector<sidecar::attribute_def> defs = {
        {"temperature", sidecar::attribute_type::float_val},
        {"location",    sidecar::attribute_type::string},
    };

    sidecar::attribute_schema schema(defs);

    auto t = schema.lookup("temperature");
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(*t, sidecar::attribute_type::float_val);

    auto l = schema.lookup("location");
    ASSERT_TRUE(l.has_value());
    EXPECT_EQ(*l, sidecar::attribute_type::string);
}

TEST(attribute_schema, lookup_unknown_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {
        {"temperature", sidecar::attribute_type::float_val},
    };

    sidecar::attribute_schema schema(defs);

    auto r = schema.lookup("nonexistent");
    EXPECT_FALSE(r.has_value());
}

TEST(config_parsing, parse_format) {
    EXPECT_EQ(sidecar::parse_format("msgpack"),     sidecar::binary_format::msgpack);
    EXPECT_EQ(sidecar::parse_format("cbor"),        sidecar::binary_format::cbor);
    EXPECT_EQ(sidecar::parse_format("flexbuffers"), sidecar::binary_format::flexbuffers);
    EXPECT_EQ(sidecar::parse_format("zera"),        sidecar::binary_format::zera);
    EXPECT_EQ(sidecar::parse_format("ion"),         sidecar::binary_format::ion);
    EXPECT_EQ(sidecar::parse_format("bson"),        sidecar::binary_format::bson);
    EXPECT_EQ(sidecar::parse_format("beve"),        sidecar::binary_format::beve);
    EXPECT_EQ(sidecar::parse_format("arrow"),       sidecar::binary_format::arrow);
    EXPECT_FALSE(sidecar::parse_format("invalid").has_value());
}

TEST(config_parsing, parse_attribute_type) {
    EXPECT_EQ(sidecar::parse_attribute_type("boolean"), sidecar::attribute_type::boolean);
    EXPECT_EQ(sidecar::parse_attribute_type("bool"),    sidecar::attribute_type::boolean);
    EXPECT_EQ(sidecar::parse_attribute_type("integer"), sidecar::attribute_type::integer);
    EXPECT_EQ(sidecar::parse_attribute_type("int"),     sidecar::attribute_type::integer);
    EXPECT_EQ(sidecar::parse_attribute_type("float"),   sidecar::attribute_type::float_val);
    EXPECT_EQ(sidecar::parse_attribute_type("string"),  sidecar::attribute_type::string);
    EXPECT_EQ(sidecar::parse_attribute_type("string_list"), sidecar::attribute_type::string_list);
    EXPECT_EQ(sidecar::parse_attribute_type("integer_list"), sidecar::attribute_type::integer_list);
    EXPECT_FALSE(sidecar::parse_attribute_type("invalid").has_value());
}

// Lease key parsing tests
#include "lease_manager.hpp"

TEST(lease_manager, make_and_parse_lease_key) {
    auto key = sidecar::lease_manager::make_lease_key(42, "client-abc");
    EXPECT_EQ(key, "42.client-abc");

    uint64_t id;
    std::string client;
    EXPECT_TRUE(sidecar::lease_manager::parse_lease_key(key, id, client));
    EXPECT_EQ(id, 42u);
    EXPECT_EQ(client, "client-abc");
}

TEST(lease_manager, parse_invalid_lease_key) {
    uint64_t id;
    std::string client;

    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key("", id, client));
    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key("noperiod", id, client));
    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key(".leading", id, client));
    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key("trailing.", id, client));
    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key("notanumber.client", id, client));
    EXPECT_FALSE(sidecar::lease_manager::parse_lease_key("42junk.client", id, client));
}

// --- Real message matching, across all seven binary formats ---

namespace {

template <typename Protocol>
std::optional<std::vector<uint64_t>> match_with(
    const sidecar::matching_engine& tree, const sidecar::attribute_schema& schema,
    sidecar::binary_format format, const zerialize::dyn::Value& payload,
    std::shared_ptr<spdlog::logger> log) {
    auto buf = zerialize::serialize<Protocol>(payload);
    return sidecar::deserialize_and_match(tree, schema, format, as_char_span(buf), std::move(log));
}

} // namespace

TEST(event_bridge_matching, matches_across_all_binary_formats) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    uint64_t id = mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto payload = zerialize::dyn::map({{"value", 42}});

    auto msgpack_result = match_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack, payload, make_log());
    ASSERT_TRUE(msgpack_result.has_value());
    EXPECT_TRUE(contains(*msgpack_result, id));

    auto cbor_result = match_with<zerialize::CBOR>(
        *snap, schema, sidecar::binary_format::cbor, payload, make_log());
    ASSERT_TRUE(cbor_result.has_value());
    EXPECT_TRUE(contains(*cbor_result, id));

    auto flex_result = match_with<zerialize::Flex>(
        *snap, schema, sidecar::binary_format::flexbuffers, payload, make_log());
    ASSERT_TRUE(flex_result.has_value());
    EXPECT_TRUE(contains(*flex_result, id));

    auto zera_result = match_with<zerialize::Zera>(
        *snap, schema, sidecar::binary_format::zera, payload, make_log());
    ASSERT_TRUE(zera_result.has_value());
    EXPECT_TRUE(contains(*zera_result, id));

    auto ion_result = match_with<zerialize::Ion>(
        *snap, schema, sidecar::binary_format::ion, payload, make_log());
    ASSERT_TRUE(ion_result.has_value());
    EXPECT_TRUE(contains(*ion_result, id));

    auto bson_result = match_with<zerialize::Bson>(
        *snap, schema, sidecar::binary_format::bson, payload, make_log());
    ASSERT_TRUE(bson_result.has_value());
    EXPECT_TRUE(contains(*bson_result, id));

    auto beve_result = match_with<zerialize::Beve>(
        *snap, schema, sidecar::binary_format::beve, payload, make_log());
    ASSERT_TRUE(beve_result.has_value());
    EXPECT_TRUE(contains(*beve_result, id));
}

TEST(event_bridge_matching, does_not_match_when_expression_false) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto buf = zerialize::serialize<zerialize::MsgPack>(zerialize::dyn::map({{"value", 5}}));
    auto result = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST(event_bridge_matching, type_mismatch_marks_undefined_and_does_not_match) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    // "value" is declared as an integer but sent as a string - populate_event
    // should mark it undefined rather than throwing, and undefined can't
    // satisfy a numeric comparison.
    auto buf = zerialize::serialize<zerialize::MsgPack>(
        zerialize::dyn::map({{"value", std::string("not-a-number")}}));
    auto result = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST(event_bridge_matching, matches_bool_string_and_list_attributes) {
    std::vector<sidecar::attribute_def> defs = {
        {"active",   sidecar::attribute_type::boolean},
        {"location", sidecar::attribute_type::string},
        {"tags",     sidecar::attribute_type::string_list},
    };
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    // Boolean attributes are tested by bare identifier (truthiness), not
    // `= true` - see a-tree's grammar.lalrpop Expression::"identifier" rule.
    uint64_t id = mgr.subscribe("active and location = \"warehouse\"", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto payload = zerialize::dyn::map({
        {"active", true},
        {"location", std::string("warehouse")},
        {"tags", zerialize::dyn::array({std::string("a"), std::string("b")})},
    });
    auto buf = zerialize::serialize<zerialize::MsgPack>(payload);
    auto result = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(contains(*result, id));
}

TEST(event_bridge_matching, matches_end_to_end_with_betree_engine) {
    std::vector<sidecar::attribute_def> defs = {
        {"value",  sidecar::attribute_type::integer},
        {"symbol", sidecar::attribute_type::string},
    };
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log(), sidecar::engine_type::betree);
    // Canonical dialect - proves the full real pipeline (subscribe ->
    // dialect translation -> be-tree parse -> deserialize -> match) works
    // end to end, not just the direct matching_engine API.
    uint64_t id = mgr.subscribe("value > 10 and symbol not in (\"GOOG\", \"TSLA\")", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto buf = zerialize::serialize<zerialize::MsgPack>(
        zerialize::dyn::map({{"value", 42}, {"symbol", std::string("AAPL")}}));
    auto result = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(contains(*result, id));

    auto non_matching = zerialize::serialize<zerialize::MsgPack>(
        zerialize::dyn::map({{"value", 42}, {"symbol", std::string("GOOG")}}));
    auto result2 = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack,
        as_char_span(non_matching), make_log());
    ASSERT_TRUE(result2.has_value());
    EXPECT_TRUE(result2->empty());
}

// --- Columnar batch matching (deserialize_and_match_columnar) ---
// BSON is intentionally not included here - columnar batching is rejected
// for format=bson at config-validation time (see test_cli.cpp/
// test_config_loading.cpp), not something deserialize_and_match_columnar
// itself needs to handle.

namespace {

template <typename Protocol>
std::optional<std::vector<sidecar::row_match>> match_columnar_with(
    const sidecar::matching_engine& tree, const sidecar::attribute_schema& schema,
    sidecar::binary_format format, const zerialize::dyn::Value& payload,
    std::shared_ptr<spdlog::logger> log) {
    auto buf = zerialize::serialize<Protocol>(payload);
    return sidecar::deserialize_and_match_columnar(
        tree, schema, format, as_char_span(buf), std::move(log));
}

template <typename Protocol>
void check_columnar_multi_row_match(
    const sidecar::matching_engine& tree, const sidecar::attribute_schema& schema,
    sidecar::binary_format format, uint64_t id) {
    // Row 0 (value=5) doesn't match, rows 1 and 2 (42, 100) do.
    auto payload = zerialize::dyn::map({
        {"value", zerialize::dyn::array({5, 42, 100})},
    });
    auto result = match_columnar_with<Protocol>(tree, schema, format, payload, make_log());
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 2u);
    EXPECT_TRUE(contains((*result)[0].matched_ids, id));
    EXPECT_TRUE(contains((*result)[1].matched_ids, id));

    typename Protocol::Deserializer row0(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>((*result)[0].payload.data()), (*result)[0].payload.size()));
    typename Protocol::Deserializer row1(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>((*result)[1].payload.data()), (*result)[1].payload.size()));
    EXPECT_EQ(row0["value"].asInt64(), 42);
    EXPECT_EQ(row1["value"].asInt64(), 100);
}

} // namespace

TEST(event_bridge_columnar, matches_multiple_rows_independently_across_all_supported_formats) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    uint64_t id = mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    check_columnar_multi_row_match<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack, id);
    check_columnar_multi_row_match<zerialize::CBOR>(
        *snap, schema, sidecar::binary_format::cbor, id);
    check_columnar_multi_row_match<zerialize::Flex>(
        *snap, schema, sidecar::binary_format::flexbuffers, id);
    check_columnar_multi_row_match<zerialize::Zera>(
        *snap, schema, sidecar::binary_format::zera, id);
    check_columnar_multi_row_match<zerialize::Ion>(
        *snap, schema, sidecar::binary_format::ion, id);
    check_columnar_multi_row_match<zerialize::Beve>(
        *snap, schema, sidecar::binary_format::beve, id);
}

TEST(event_bridge_columnar, empty_batch_returns_empty_result_not_malformed) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto result = match_columnar_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack,
        zerialize::dyn::map({}), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST(event_bridge_columnar, malformed_shape_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    // A normal (non-columnar) scalar row sent to the columnar path: "value"
    // is a bare int, not an array - expand_columnar rejects this outright.
    auto result = match_columnar_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack,
        zerialize::dyn::map({{"value", 42}}), make_log());
    EXPECT_FALSE(result.has_value());
}

TEST(event_bridge_columnar, mismatched_column_lengths_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {
        {"value", sidecar::attribute_type::integer},
        {"tag",   sidecar::attribute_type::string},
    };
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    auto payload = zerialize::dyn::map({
        {"value", zerialize::dyn::array({5, 42})},
        {"tag", zerialize::dyn::array({std::string("a")})},   // one fewer than "value"
    });
    auto result = match_columnar_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack, payload, make_log());
    EXPECT_FALSE(result.has_value());
}

TEST(event_bridge_columnar, list_attribute_extracts_own_per_row_value) {
    std::vector<sidecar::attribute_def> defs = {
        {"tags", sidecar::attribute_type::string_list},
    };
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    uint64_t id = mgr.subscribe("tags one of (\"a\")", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    // Each row's own "tags" value is itself an array (a list attribute) -
    // the columnar batch shape wraps that in one more array level, one
    // element per row.
    auto payload = zerialize::dyn::map({
        {"tags", zerialize::dyn::array({
            zerialize::dyn::array({std::string("a")}),
            zerialize::dyn::array({std::string("b")}),
        })},
    });
    auto result = match_columnar_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack, payload, make_log());
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1u);
    EXPECT_TRUE(contains((*result)[0].matched_ids, id));
}

TEST(event_bridge_columnar, null_composite_row_produces_all_undefined_event) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    // "is null" is the only expression that can match a fully-undefined row.
    uint64_t id = mgr.subscribe("value is null", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    // pg_zerialize represents a wholly-NULL composite row as null at that
    // index in every column's array.
    auto payload = zerialize::dyn::map({
        {"value", zerialize::dyn::array({zerialize::dyn::Value(), 42})},
    });
    auto result = match_columnar_with<zerialize::MsgPack>(
        *snap, schema, sidecar::binary_format::msgpack, payload, make_log());
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1u);
    EXPECT_TRUE(contains((*result)[0].matched_ids, id));
}

TEST(event_bridge_matching, unrecognized_binary_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.acquire_tree();
    ASSERT_TRUE(snap);

    // 0xc1 is reserved/invalid in MessagePack.
    std::vector<char> garbage{static_cast<char>(0xc1)};
    auto result = sidecar::deserialize_and_match(
        *snap, schema, sidecar::binary_format::msgpack,
        std::span<const char>(garbage.data(), garbage.size()), make_log());
    EXPECT_FALSE(result.has_value());
}
