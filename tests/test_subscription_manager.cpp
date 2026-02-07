#include "subscription_manager.hpp"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>("test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

std::vector<sidecar::attribute_def> sample_attributes() {
    return {
        {"temperature", sidecar::attribute_type::float_val},
        {"location",    sidecar::attribute_type::string},
        {"severity",    sidecar::attribute_type::integer},
        {"active",      sidecar::attribute_type::boolean},
    };
}

} // namespace

TEST(subscription_manager, subscribe_returns_id) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    EXPECT_GT(id, 0u);
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager, duplicate_expression_returns_same_id) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id1 = mgr.subscribe("temperature > 30.0", "client-1");
    uint64_t id2 = mgr.subscribe("temperature > 30.0", "client-2");

    EXPECT_EQ(id1, id2);
    EXPECT_EQ(mgr.active_count(), 1u);

    // Both clients are lease holders
    auto info = mgr.get_subscription(id1);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->lease_holders.size(), 2u);
}

TEST(subscription_manager, different_expressions_get_different_ids) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id1 = mgr.subscribe("temperature > 30.0", "client-1");
    uint64_t id2 = mgr.subscribe("severity = 5", "client-1");

    EXPECT_NE(id1, id2);
    EXPECT_EQ(mgr.active_count(), 2u);
}

TEST(subscription_manager, remove_lease_partial) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    mgr.subscribe("temperature > 30.0", "client-2");

    // Remove one lease - subscription should remain
    bool fully_removed = mgr.remove_lease(id, "client-1");
    EXPECT_FALSE(fully_removed);
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager, remove_lease_complete) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    bool fully_removed = mgr.remove_lease(id, "client-1");
    EXPECT_TRUE(fully_removed);
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager, find_by_expression) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    uint64_t id = mgr.subscribe("location = \"warehouse\"", "client-1");

    auto found = mgr.find_by_expression("location = \"warehouse\"");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, id);

    auto not_found = mgr.find_by_expression("location = \"office\"");
    EXPECT_FALSE(not_found.has_value());
}

TEST(subscription_manager, invalid_expression_throws) {
    sidecar::subscription_manager mgr(sample_attributes(), make_log());

    EXPECT_THROW(
        mgr.subscribe("this is not a valid expression !!!", "client-1"),
        atree::Error
    );
    EXPECT_EQ(mgr.active_count(), 0u);
}
