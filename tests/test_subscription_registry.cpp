#include "subscription_registry.hpp"
#include "fake_connection.hpp"
#include "asio_test_helpers.hpp"
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/null_sink.h>
#include <chrono>
#include <string>

namespace {

using sidecar_test::run_to_completion;

auto make_log() {
    return std::make_shared<spdlog::logger>(
        "registry-test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

nats_asio::message json_message(const nlohmann::json& body) {
    nats_asio::message msg;
    auto s = body.dump();
    msg.payload.assign(s.begin(), s.end());
    return msg;
}

using id_result = std::pair<uint64_t, nats_asio::status>;

} // namespace

// --- resolve_id() ---

TEST(subscription_registry, resolve_id_creates_new_entry_for_fresh_expression) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id, status] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));

    EXPECT_TRUE(status.ok());
    EXPECT_NE(id, 0u);
    EXPECT_EQ(conn->kv_store.size(), 1u);
}

// Models both "a second instance in the fleet resolves the same brand-new
// expression" and "two coroutines on the same instance race the same
// brand-new expression" - either way, only one kv_create can win, and the
// loser must read back the winner's id rather than creating a duplicate.
TEST(subscription_registry, resolve_id_second_call_same_expression_returns_same_id) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg_a(conn, "test-registry", make_log());
    sidecar::subscription_registry reg_b(conn, "test-registry", make_log());

    auto [id_a, status_a] = run_to_completion<id_result>(
        ioc, reg_a.resolve_id("temperature > 30.0"));
    ASSERT_TRUE(status_a.ok());

    auto [id_b, status_b] = run_to_completion<id_result>(
        ioc, reg_b.resolve_id("temperature > 30.0"));
    ASSERT_TRUE(status_b.ok());

    EXPECT_EQ(id_a, id_b);
    EXPECT_EQ(conn->kv_store.size(), 1u);
}

TEST(subscription_registry, resolve_id_different_expressions_get_different_ids) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id1, status1] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));
    auto [id2, status2] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 50.0"));

    ASSERT_TRUE(status1.ok());
    ASSERT_TRUE(status2.ok());
    EXPECT_NE(id1, id2);
    EXPECT_EQ(conn->kv_store.size(), 2u);
}

// Direct proof of "why registry entries are permanent": a resubscribe to an
// expression that was fully torn down elsewhere (subscription_manager no
// longer has it locally) must still resolve to the SAME id it had before,
// not a fresh one - the split-brain bug the design review caught would
// reappear the moment two instances could disagree about this.
TEST(subscription_registry, resolve_id_is_stable_across_repeated_calls_no_deletion_ever_occurs) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id1, status1] = run_to_completion<id_result>(
        ioc, reg.resolve_id("location = \"NYC\""));
    ASSERT_TRUE(status1.ok());

    // Simulate the passage of time / a full unsubscribe-then-resubscribe
    // cycle elsewhere - nothing about the registry itself changes.
    auto [id2, status2] = run_to_completion<id_result>(
        ioc, reg.resolve_id("location = \"NYC\""));
    ASSERT_TRUE(status2.ok());

    EXPECT_EQ(id1, id2);
    EXPECT_EQ(conn->kv_store.size(), 1u);
}

TEST(subscription_registry, resolve_id_rejects_hash_collision) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id1, status1] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));
    ASSERT_TRUE(status1.ok());
    ASSERT_EQ(conn->kv_store.size(), 1u);

    // Corrupt the stored record to simulate two different expressions
    // hashing to the same key (astronomically unlikely in practice, but
    // must never silently misroute a client's filter if it ever happened).
    auto& record = conn->kv_store.begin()->second;
    nlohmann::json corrupted = {{"expression", "a completely different expression"}};
    auto s = corrupted.dump();
    record.value.assign(s.begin(), s.end());

    auto [id2, status2] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));

    EXPECT_TRUE(status2.failed());
    EXPECT_EQ(id2, 0u);
}

TEST(subscription_registry, resolve_id_treats_zero_revision_as_failure) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    // Forces fake_connection::kv_put (which kv_create delegates to on a
    // winning create) to hand back revision 0 for this one call - mirrors
    // lease_manager::persist_lease/delete_lease's existing defensive check
    // against the same real-world API surface.
    conn->next_revision = 0;
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id, status] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));

    EXPECT_TRUE(status.failed());
    EXPECT_EQ(id, 0u);
}

TEST(subscription_registry, resolve_id_reports_real_create_failure) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    conn->fail_kv_create = [](std::string_view, std::string_view) { return true; };
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    auto [id, status] = run_to_completion<id_result>(
        ioc, reg.resolve_id("temperature > 30.0"));

    EXPECT_TRUE(status.failed());
    EXPECT_EQ(id, 0u);
    EXPECT_TRUE(conn->kv_store.empty());
}

// --- ensure_bucket() ---

TEST(subscription_registry, ensure_bucket_creates_bucket_when_missing) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-registry") {
            co_return std::pair{json_message({{"error", {{"code", 404}}}}), nats_asio::status{}};
        }
        if (subject == "$JS.API.STREAM.CREATE.KV_test-registry") {
            co_return std::pair{json_message(nlohmann::json::object()), nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(ioc, reg.ensure_bucket()));
}

TEST(subscription_registry, ensure_bucket_accepts_existing_bucket_with_matching_config) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-registry") {
            co_return std::pair{
                json_message({{"config", {{"max_msgs_per_subject", 1},
                                          {"subjects", {"$KV.test-registry.>"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(ioc, reg.ensure_bucket()));
}

TEST(subscription_registry, ensure_bucket_refuses_mismatched_history) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_registry reg(conn, "test-registry", make_log());

    // history (max_msgs_per_subject) != 1 would let a later write silently
    // shadow an earlier one instead of failing outright - a real mismatch,
    // not cosmetic.
    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-registry") {
            co_return std::pair{
                json_message({{"config", {{"max_msgs_per_subject", 5},
                                          {"subjects", {"$KV.test-registry.>"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_FALSE(run_to_completion<bool>(ioc, reg.ensure_bucket()));
}
