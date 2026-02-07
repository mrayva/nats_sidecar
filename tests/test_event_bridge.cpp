#include "event_bridge.hpp"
#include "config.hpp"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>

// These tests verify the attribute_schema lookup and lease_manager key parsing.
// Full event_bridge tests require a-tree + zerialize and will be integration tests.

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>("test", std::make_shared<spdlog::sinks::null_sink_mt>());
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
}
