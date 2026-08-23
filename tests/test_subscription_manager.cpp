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
    auto removal = mgr.remove_lease(id, "client-1");
    EXPECT_EQ(removal, sidecar::lease_removal::still_active);
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager, remove_lease_complete) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    auto removal = mgr.remove_lease(id, "client-1");
    EXPECT_EQ(removal, sidecar::lease_removal::fully_removed);
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager, remove_lease_not_found) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    auto removal = mgr.remove_lease(999, "client-1");
    EXPECT_EQ(removal, sidecar::lease_removal::not_found);
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
        sidecar::matching_engine_error
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

TEST(subscription_manager, restores_stable_id_and_advances_sequence) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    ASSERT_TRUE(mgr.restore(42, "temperature > 30.0", "client-1"));
    ASSERT_TRUE(mgr.restore(42, "temperature > 30.0", "client-2"));

    auto restored = mgr.get_subscription(42);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->lease_holders.size(), 2u);
    EXPECT_EQ(mgr.snapshot()->output_subjects.at(42), "test.output.42");

    EXPECT_EQ(mgr.subscribe("severity = 5", "client-3"), 43u);
}

TEST(subscription_manager, rejects_conflicting_restored_records) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    ASSERT_TRUE(mgr.restore(7, "temperature > 30.0", "client-1"));
    EXPECT_FALSE(mgr.restore(8, "temperature > 30.0", "client-2"));
    EXPECT_FALSE(mgr.restore(7, "severity = 5", "client-2"));
    EXPECT_EQ(mgr.active_count(), 1u);
}

// This is the property the whole client-assigned-ID control-plane redesign
// depends on: two entirely independent sidecar instances (modeled here as
// two independent subscription_manager objects, never talking to each
// other), each fed the SAME client-supplied ID+expression pair via
// restore() - as they would be by a shared, fan-out control subject - must
// land on the identical output topic without any coordination between them.
// This is what lets a client compute where matches will be published the
// instant it publishes the subscribe request, without waiting for any
// instance's reply.
TEST(subscription_manager, independent_instances_restoring_same_id_agree_on_output_topic) {
    sidecar::subscription_manager mgr_a(sample_attributes(), "sc.real.out", make_log());
    sidecar::subscription_manager mgr_b(sample_attributes(), "sc.real.out", make_log());

    constexpr uint64_t client_assigned_id = 0x9F3A7B21C4E60D18ull;
    ASSERT_TRUE(mgr_a.restore(client_assigned_id, "location = \"NYC\"", "client-1"));
    ASSERT_TRUE(mgr_b.restore(client_assigned_id, "location = \"NYC\"", "client-1"));

    const auto& topic_a = mgr_a.snapshot()->output_subjects.at(client_assigned_id);
    const auto& topic_b = mgr_b.snapshot()->output_subjects.at(client_assigned_id);
    EXPECT_EQ(topic_a, topic_b);
    EXPECT_EQ(topic_a, "sc.real.out." + std::to_string(client_assigned_id));

    // Order-independence: mgr_b sees an unrelated prior subscription first
    // (simulating divergent per-instance history - exactly the scenario
    // that makes uncoordinated auto-incrementing IDs unsafe), yet still
    // agrees with mgr_a once given the same explicit ID.
    sidecar::subscription_manager mgr_c(sample_attributes(), "sc.real.out", make_log());
    mgr_c.subscribe("severity = 5", "some-other-client");
    mgr_c.subscribe("active", "some-other-client");
    ASSERT_TRUE(mgr_c.restore(client_assigned_id, "location = \"NYC\"", "client-1"));
    EXPECT_EQ(mgr_c.snapshot()->output_subjects.at(client_assigned_id), topic_a);
}

// --- engine=betree: same subscribe/lease/snapshot behavior, different backing engine ---

TEST(subscription_manager_betree, subscribe_and_lease_lifecycle) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    EXPECT_GT(id, 0u);
    EXPECT_EQ(mgr.active_count(), 1u);

    auto removal = mgr.remove_lease(id, "client-1");
    EXPECT_EQ(removal, sidecar::lease_removal::fully_removed);
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager_betree, invalid_expression_throws) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    EXPECT_THROW(
        mgr.subscribe("this is not a valid expression !!!", "client-1"),
        sidecar::matching_engine_error
    );
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager_betree, snapshot_valid_after_subscribe) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    auto snap = mgr.snapshot();
    ASSERT_TRUE(snap);
    ASSERT_TRUE(snap->tree);
    EXPECT_EQ(snap->active_count, 1u);
    EXPECT_EQ(snap->output_subjects.at(id), "test.output." + std::to_string(id));
}

TEST(subscription_manager_betree, space_separated_keyword_works_natively) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    // a-tree and be-tree agree on "is not null"'s spelling verbatim - no
    // translation needed, just confirms the real pipeline accepts it.
    EXPECT_NO_THROW(mgr.subscribe("location is not null", "client-1"));
    EXPECT_EQ(mgr.active_count(), 1u);
}

TEST(subscription_manager_betree, is_not_empty_is_rejected) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    // be-tree's grammar has no rule for "is not empty" at all - this must
    // be caught before it reaches be-tree's own parser with a confusing
    // syntax error.
    EXPECT_THROW(
        mgr.subscribe("location is not empty", "client-1"),
        sidecar::matching_engine_error
    );
    EXPECT_EQ(mgr.active_count(), 0u);
}
