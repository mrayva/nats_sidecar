#include "lease_manager.hpp"
#include "subscription_manager.hpp"
#include "fake_connection.hpp"
#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/null_sink.h>
#include <chrono>
#include <optional>
#include <string>

namespace sidecar {

// Mirrors sidecar_engine_test_access in test_sidecar_lifecycle.cpp: grants
// tests access to lease_manager's private reconcile_once(), which is the
// factored-out body of cleanup_loop()'s per-cycle reconciliation pass. This
// lets a test drive one reconciliation immediately instead of waiting on a
// real check-interval timer.
struct lease_manager_test_access {
    static asio::awaitable<void> reconcile_once(lease_manager& lm) {
        return lm.reconcile_once();
    }
};

} // namespace sidecar

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>(
        "lease-test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

std::vector<sidecar::attribute_def> sample_attributes() {
    return {{"temperature", sidecar::attribute_type::float_val}};
}

// Runs `awaitable_fn(...)` to completion on `ioc`, polling with short
// run_for() slices (a single run_for() could return immediately if nothing
// is queued yet on a fresh/restarted io_context - see the same pattern in
// test_worker_pool.cpp's drive_until()).
template <typename T, typename Awaitable>
T run_to_completion(asio::io_context& ioc, Awaitable&& awaitable) {
    std::optional<T> result;
    asio::co_spawn(ioc, std::forward<Awaitable>(awaitable), [&](std::exception_ptr ep, T value) {
        if (ep) std::rethrow_exception(ep);
        result = std::move(value);
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!result.has_value() && std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(20));
    }
    return std::move(result).value();
}

void run_void_to_completion(asio::io_context& ioc, asio::awaitable<void> awaitable) {
    bool done = false;
    asio::co_spawn(ioc, std::move(awaitable), [&](std::exception_ptr ep) {
        if (ep) std::rethrow_exception(ep);
        done = true;
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done && std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(done);
}

nats_asio::message json_message(const nlohmann::json& body) {
    nats_asio::message msg;
    auto s = body.dump();
    msg.payload.assign(s.begin(), s.end());
    return msg;
}

} // namespace

TEST(lease_manager, persist_lease_stores_record_in_kv_store) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "leases", /*ttl=*/60, /*check_interval=*/60,
                              make_log());

    bool ok = run_to_completion<bool>(
        ioc, lm.persist_lease(1, "temperature > 30.0", "client-1"));
    EXPECT_TRUE(ok);

    auto it = conn->kv_store.find(sidecar_test::fake_connection::kv_key("leases", "1.client-1"));
    ASSERT_NE(it, conn->kv_store.end());
    EXPECT_EQ(it->second.op, nats_asio::kv_entry::operation::put);
}

TEST(lease_manager, delete_lease_marks_record_deleted) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "leases", 60, 60, make_log());

    ASSERT_TRUE(run_to_completion<bool>(ioc, lm.persist_lease(1, "temperature > 30.0", "client-1")));
    bool deleted = run_to_completion<bool>(ioc, lm.delete_lease(1, "client-1"));
    EXPECT_TRUE(deleted);

    auto it = conn->kv_store.find(sidecar_test::fake_connection::kv_key("leases", "1.client-1"));
    ASSERT_NE(it, conn->kv_store.end());
    EXPECT_EQ(it->second.op, nats_asio::kv_entry::operation::del);
}

// This is the exact scenario broken by the nats_asio kv_get bug found during
// dependency review: a lease's KV entry is genuinely gone (expired/deleted
// server-side), and reconciliation must treat that as key_not_found and
// remove the in-memory subscription lease - not silently treat it as "still
// alive" and keep refreshing it forever.
TEST(lease_manager, reconcile_removes_subscription_when_lease_expired_on_server) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "leases", 60, 60, make_log());

    uint64_t sub_id = sub_mgr.subscribe("temperature > 30.0", "client-1");
    ASSERT_TRUE(run_to_completion<bool>(
        ioc, lm.persist_lease(sub_id, "temperature > 30.0", "client-1")));
    ASSERT_EQ(sub_mgr.active_count(), 1u);

    // Simulate server-side TTL expiry: the KV entry is just gone.
    conn->kv_store.erase(sidecar_test::fake_connection::kv_key("leases", "1.client-1"));

    run_void_to_completion(ioc, sidecar::lease_manager_test_access::reconcile_once(lm));

    EXPECT_EQ(sub_mgr.active_count(), 0u);
}

TEST(lease_manager, reconcile_keeps_subscription_when_lease_still_present) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "leases", 60, 60, make_log());

    uint64_t sub_id = sub_mgr.subscribe("temperature > 30.0", "client-1");
    ASSERT_TRUE(run_to_completion<bool>(
        ioc, lm.persist_lease(sub_id, "temperature > 30.0", "client-1")));
    ASSERT_EQ(sub_mgr.active_count(), 1u);

    // Lease is still present server-side - reconciliation must not remove it.
    run_void_to_completion(ioc, sidecar::lease_manager_test_access::reconcile_once(lm));

    EXPECT_EQ(sub_mgr.active_count(), 1u);
}

// --- start() / ensure_bucket() ---

TEST(lease_manager, start_creates_bucket_when_stream_is_missing) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "test-leases", 60, 60, make_log());

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-leases") {
            co_return std::pair{json_message({{"error", {{"code", 404}}}}), nats_asio::status{}};
        }
        if (subject == "$JS.API.STREAM.CREATE.KV_test-leases") {
            co_return std::pair{json_message(nlohmann::json::object()), nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(ioc, lm.start()));
}

TEST(lease_manager, start_accepts_existing_bucket_with_matching_config) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "test-leases", /*ttl=*/60, 60, make_log());

    const uint64_t expected_max_age = 60ULL * 1'000'000'000ULL;  // ttl_seconds -> ns
    conn->on_request = [expected_max_age](
                           std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-leases") {
            co_return std::pair{
                json_message({{"config", {{"max_age", expected_max_age},
                                          {"max_msgs_per_subject", 1},
                                          {"subjects", {"$KV.test-leases.>"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(ioc, lm.start()));
}

TEST(lease_manager, start_refuses_existing_bucket_with_mismatched_ttl) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::subscription_manager sub_mgr(sample_attributes(), "test.output", make_log());
    sidecar::lease_manager lm(ioc, conn, sub_mgr, "test-leases", /*ttl=*/60, 60, make_log());

    // Bucket exists, but its max_age (a prior, different TTL) doesn't match
    // this process's configured lease_ttl_seconds - startup must refuse
    // rather than silently operating against a mismatched bucket.
    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.KV_test-leases") {
            co_return std::pair{
                json_message({{"config", {{"max_age", 1},
                                          {"max_msgs_per_subject", 1},
                                          {"subjects", {"$KV.test-leases.>"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_FALSE(run_to_completion<bool>(ioc, lm.start()));
}
