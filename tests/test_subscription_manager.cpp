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
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    EXPECT_GT(id, 0u);
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager, duplicate_expression_returns_same_id) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

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
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id1 = mgr.subscribe("temperature > 30.0", "client-1");
    uint64_t id2 = mgr.subscribe("severity = 5", "client-1");

    EXPECT_NE(id1, id2);
    EXPECT_EQ(mgr.active_count(), 2u);
}

TEST(subscription_manager, remove_lease_partial) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    mgr.subscribe("temperature > 30.0", "client-2");

    // Remove one lease - subscription should remain
    bool fully_removed = mgr.remove_lease(id, "client-1");
    EXPECT_FALSE(fully_removed);
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager, remove_lease_complete) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    bool fully_removed = mgr.remove_lease(id, "client-1");
    EXPECT_TRUE(fully_removed);
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager, find_by_expression) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("location = \"warehouse\"", "client-1");

    auto found = mgr.find_by_expression("location = \"warehouse\"");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, id);

    auto not_found = mgr.find_by_expression("location = \"office\"");
    EXPECT_FALSE(not_found.has_value());
}

TEST(subscription_manager, invalid_expression_throws) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    EXPECT_THROW(
        mgr.subscribe("this is not a valid expression !!!", "client-1"),
        atree::Error
    );
    EXPECT_EQ(mgr.active_count(), 0u);
}

// --- Snapshot-specific tests ---

TEST(subscription_manager, snapshot_valid_after_subscribe) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap);
    ASSERT_TRUE(snap->tree);
    EXPECT_EQ(snap->active_count, 1u);
    EXPECT_EQ(snap->output_subjects.size(), 1u);

    auto it = snap->output_subjects.find(id);
    ASSERT_NE(it, snap->output_subjects.end());
    EXPECT_EQ(it->second, "test.output." + std::to_string(id));
}

TEST(subscription_manager, snapshot_valid_after_remove) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    mgr.remove_lease(id, "client-1");

    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap);
    ASSERT_TRUE(snap->tree);
    EXPECT_EQ(snap->active_count, 0u);
    EXPECT_TRUE(snap->output_subjects.empty());
}

TEST(subscription_manager, old_snapshot_remains_valid_after_new_publish) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id1 = mgr.subscribe("temperature > 30.0", "client-1");
    auto old_snap = mgr.snapshot();

    // Add another subscription — triggers new snapshot
    uint64_t id2 = mgr.subscribe("severity = 5", "client-2");
    auto new_snap = mgr.snapshot();

    // Old snapshot still valid with 1 subscription
    ASSERT_TRUE(old_snap);
    ASSERT_TRUE(old_snap->tree);
    EXPECT_EQ(old_snap->active_count, 1u);
    EXPECT_EQ(old_snap->output_subjects.size(), 1u);
    EXPECT_NE(old_snap->output_subjects.find(id1), old_snap->output_subjects.end());

    // New snapshot has 2 subscriptions
    ASSERT_TRUE(new_snap);
    ASSERT_TRUE(new_snap->tree);
    EXPECT_EQ(new_snap->active_count, 2u);
    EXPECT_EQ(new_snap->output_subjects.size(), 2u);
    EXPECT_NE(new_snap->output_subjects.find(id1), new_snap->output_subjects.end());
    EXPECT_NE(new_snap->output_subjects.find(id2), new_snap->output_subjects.end());
}

TEST(subscription_manager, snapshot_empty_on_construction) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap);
    ASSERT_TRUE(snap->tree);
    EXPECT_EQ(snap->active_count, 0u);
    EXPECT_TRUE(snap->output_subjects.empty());
}
