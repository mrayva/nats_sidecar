#include "subscription_manager.hpp"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/null_sink.h>
#include <chrono>

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

// --- Tree/output-subject tests (acquire_tree()/output_subject() - see subscription_manager.hpp's
// own comments for why these replaced the old snapshot()/tree_snapshot pair: the old RCU
// snapshot published a whole new tree_snapshot object on every write, which is exactly the
// O(K^2) rebuild-from-scratch cost this class was redesigned to eliminate. There's now one live
// tree mutated in place, so there's no more "old, still-immutable snapshot" concept to test -
// what these tests verify instead is that acquire_tree()/output_subject() correctly reflect
// current state as it evolves. ---

TEST(subscription_manager, tree_valid_after_subscribe) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    auto tree = mgr.acquire_tree();
    ASSERT_TRUE(tree);
    EXPECT_EQ(mgr.active_count(), 1u);

    auto subj = mgr.output_subject(id);
    ASSERT_TRUE(subj.has_value());
    EXPECT_EQ(*subj, "test.output." + std::to_string(id));
}

TEST(subscription_manager, output_subject_absent_after_remove) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");
    mgr.remove_lease(id, "client-1");

    auto tree = mgr.acquire_tree();
    ASSERT_TRUE(tree);
    EXPECT_EQ(mgr.active_count(), 0u);
    EXPECT_FALSE(mgr.output_subject(id).has_value());
}

TEST(subscription_manager, output_subject_reflects_growth_incrementally) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    uint64_t id1 = mgr.subscribe("temperature > 30.0", "client-1");
    EXPECT_EQ(mgr.active_count(), 1u);
    ASSERT_TRUE(mgr.output_subject(id1).has_value());

    // Add another subscription — the live tree grows in place (see this file's own top-of-block
    // comment); id1's own subject must remain unaffected by id2's insertion.
    uint64_t id2 = mgr.subscribe("severity = 5", "client-2");

    EXPECT_EQ(mgr.active_count(), 2u);
    auto subj1 = mgr.output_subject(id1);
    auto subj2 = mgr.output_subject(id2);
    ASSERT_TRUE(subj1.has_value());
    ASSERT_TRUE(subj2.has_value());
    EXPECT_NE(*subj1, *subj2);
}

TEST(subscription_manager, tree_empty_on_construction) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    auto tree = mgr.acquire_tree();
    ASSERT_TRUE(tree);
    EXPECT_EQ(mgr.active_count(), 0u);
}

TEST(subscription_manager, restores_stable_id_and_advances_sequence) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    ASSERT_TRUE(mgr.restore(42, "temperature > 30.0", "client-1"));
    ASSERT_TRUE(mgr.restore(42, "temperature > 30.0", "client-2"));

    auto restored = mgr.get_subscription(42);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->lease_holders.size(), 2u);
    EXPECT_EQ(mgr.output_subject(42).value(), "test.output.42");

    EXPECT_EQ(mgr.subscribe("severity = 5", "client-3"), 43u);
}

// Regression guard for the O(K^2) bulk-subscribe cost this class was redesigned to eliminate:
// subscribe() used to rebuild the whole matching tree from scratch (re-parsing every prior
// expression) on every single call, so the Nth subscribe cost O(N). With true incremental
// insert(), per-call cost should be roughly constant, so subscribing the second half of N should
// not take dramatically longer wall-clock time than the first half. A generous 3x ratio
// threshold (not e.g. 1.2x) keeps this robust against ordinary timing noise while still failing
// hard on an accidental reintroduction of the quadratic rebuild (which would make the second
// half take roughly N/2 times as long as the first, not a small constant factor).
TEST(subscription_manager, subscribe_cost_is_not_quadratic) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log());

    constexpr int kTotal = 2000;
    constexpr int kHalf = kTotal / 2;

    auto expr_for = [](int i) {
        return "severity = " + std::to_string(i);
    };

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kHalf; ++i) {
        mgr.subscribe(expr_for(i), "client-1");
    }
    auto t1 = std::chrono::steady_clock::now();
    for (int i = kHalf; i < kTotal; ++i) {
        mgr.subscribe(expr_for(i), "client-1");
    }
    auto t2 = std::chrono::steady_clock::now();

    EXPECT_EQ(mgr.active_count(), static_cast<std::size_t>(kTotal));

    auto first_half = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    auto second_half = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    // Both halves insert the same number of subscriptions into a tree of comparable size (the
    // second half's tree is at most 2x the first half's) - under the old O(K^2) rebuild, the
    // second half would take roughly (1.5x kHalf)/(0.5x kHalf) = 3x longer on average; under
    // true incremental insert, both halves should cost roughly the same.
    EXPECT_LT(second_half, first_half * 3 + 1000)
        << "first_half=" << first_half << "us second_half=" << second_half << "us "
        << "(a large second_half/first_half ratio suggests the O(K^2) rebuild regressed)";
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

    const auto topic_a = mgr_a.output_subject(client_assigned_id).value();
    const auto topic_b = mgr_b.output_subject(client_assigned_id).value();
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
    EXPECT_EQ(mgr_c.output_subject(client_assigned_id).value(), topic_a);
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

TEST(subscription_manager_betree, tree_valid_after_subscribe) {
    sidecar::subscription_manager mgr(sample_attributes(), "test.output", make_log(),
                                      sidecar::engine_type::betree);

    uint64_t id = mgr.subscribe("temperature > 30.0", "client-1");

    auto tree = mgr.acquire_tree();
    ASSERT_TRUE(tree);
    EXPECT_EQ(mgr.active_count(), 1u);
    EXPECT_EQ(mgr.output_subject(id).value(), "test.output." + std::to_string(id));
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
