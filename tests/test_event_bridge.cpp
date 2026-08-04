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

// --- Real message matching, across all four binary formats ---

namespace {

template <typename Protocol>
std::optional<std::vector<uint64_t>> match_with(
    const atree::Tree& tree, const sidecar::attribute_schema& schema,
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
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto payload = zerialize::dyn::map({{"value", 42}});

    auto msgpack_result = match_with<zerialize::MsgPack>(
        *snap->tree, schema, sidecar::binary_format::msgpack, payload, make_log());
    ASSERT_TRUE(msgpack_result.has_value());
    EXPECT_TRUE(contains(*msgpack_result, id));

    auto cbor_result = match_with<zerialize::CBOR>(
        *snap->tree, schema, sidecar::binary_format::cbor, payload, make_log());
    ASSERT_TRUE(cbor_result.has_value());
    EXPECT_TRUE(contains(*cbor_result, id));

    auto flex_result = match_with<zerialize::Flex>(
        *snap->tree, schema, sidecar::binary_format::flexbuffers, payload, make_log());
    ASSERT_TRUE(flex_result.has_value());
    EXPECT_TRUE(contains(*flex_result, id));

    auto zera_result = match_with<zerialize::Zera>(
        *snap->tree, schema, sidecar::binary_format::zera, payload, make_log());
    ASSERT_TRUE(zera_result.has_value());
    EXPECT_TRUE(contains(*zera_result, id));
}

TEST(event_bridge_matching, does_not_match_when_expression_false) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto buf = zerialize::serialize<zerialize::MsgPack>(zerialize::dyn::map({{"value", 5}}));
    auto result = sidecar::deserialize_and_match(
        *snap->tree, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->empty());
}

TEST(event_bridge_matching, type_mismatch_marks_undefined_and_does_not_match) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    // "value" is declared as an integer but sent as a string - populate_event
    // should mark it undefined rather than throwing, and undefined can't
    // satisfy a numeric comparison.
    auto buf = zerialize::serialize<zerialize::MsgPack>(
        zerialize::dyn::map({{"value", std::string("not-a-number")}}));
    auto result = sidecar::deserialize_and_match(
        *snap->tree, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
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
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    auto payload = zerialize::dyn::map({
        {"active", true},
        {"location", std::string("warehouse")},
        {"tags", zerialize::dyn::array({std::string("a"), std::string("b")})},
    });
    auto buf = zerialize::serialize<zerialize::MsgPack>(payload);
    auto result = sidecar::deserialize_and_match(
        *snap->tree, schema, sidecar::binary_format::msgpack, as_char_span(buf), make_log());
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(contains(*result, id));
}

TEST(event_bridge_matching, unrecognized_binary_returns_nullopt) {
    std::vector<sidecar::attribute_def> defs = {{"value", sidecar::attribute_type::integer}};
    sidecar::attribute_schema schema(defs);
    sidecar::subscription_manager mgr(defs, "test.output", make_log());
    mgr.subscribe("value > 10", "client-1");
    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap && snap->tree);

    // 0xc1 is reserved/invalid in MessagePack.
    std::vector<char> garbage{static_cast<char>(0xc1)};
    auto result = sidecar::deserialize_and_match(
        *snap->tree, schema, sidecar::binary_format::msgpack,
        std::span<const char>(garbage.data(), garbage.size()), make_log());
    EXPECT_FALSE(result.has_value());
}
