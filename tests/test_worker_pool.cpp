#include "worker_pool.hpp"
#include "fake_connection.hpp"
#include "asio_test_helpers.hpp"
#include <asio/io_context.hpp>
#include <gtest/gtest.h>
#include <spdlog/sinks/null_sink.h>
#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <atomic>
#include <chrono>

namespace {

using sidecar_test::drive_until;

auto worker_log() {
    return std::make_shared<spdlog::logger>(
        "worker-test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

sidecar::config worker_config() {
    sidecar::config cfg;
    cfg.input_subject = "input";
    cfg.output_prefix = "output";
    cfg.worker_threads = 2;
    cfg.input_queue_max_messages = 1024;
    cfg.input_queue_max_bytes = 1024;
    cfg.publish_max_inflight = 4;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};
    return cfg;
}

std::vector<char> matching_payload(int value) {
    auto buf = zerialize::serialize<zerialize::MsgPack>(zerialize::dyn::map({{"value", value}}));
    return std::vector<char>(reinterpret_cast<const char*>(buf.data()),
                             reinterpret_cast<const char*>(buf.data()) + buf.size());
}

} // namespace

TEST(worker_pool, build_pub_frames_exact_wire_for_single_subscriber) {
    std::string payload = "hello";
    auto frames = sidecar::build_pub_frames(
        {1}, {{1, "output.1"}}, std::span<const char>(payload.data(), payload.size()));

    EXPECT_EQ(frames.count, 1u);
    EXPECT_EQ(frames.wire, "PUB output.1 5\r\nhello\r\n");
}

TEST(worker_pool, build_pub_frames_exact_wire_for_multiple_subscribers) {
    std::string payload = "hi";
    auto frames = sidecar::build_pub_frames(
        {1, 2}, {{1, "output.1"}, {2, "output.2"}},
        std::span<const char>(payload.data(), payload.size()));

    EXPECT_EQ(frames.count, 2u);
    EXPECT_EQ(frames.wire, "PUB output.1 2\r\nhi\r\nPUB output.2 2\r\nhi\r\n");
}

TEST(worker_pool, build_pub_frames_skips_ids_missing_from_output_subjects) {
    std::string payload = "x";
    // id 2 matched but has no known output subject (e.g. removed between
    // search and publish) - it must be silently skipped, not crash or emit
    // a malformed frame.
    auto frames = sidecar::build_pub_frames(
        {1, 2}, {{1, "output.1"}}, std::span<const char>(payload.data(), payload.size()));

    EXPECT_EQ(frames.count, 1u);
    EXPECT_EQ(frames.wire, "PUB output.1 1\r\nx\r\n");
}

TEST(worker_pool, build_pub_frames_empty_when_no_ids_match) {
    std::string payload = "x";
    auto frames = sidecar::build_pub_frames(
        {}, {}, std::span<const char>(payload.data(), payload.size()));

    EXPECT_EQ(frames.count, 0u);
    EXPECT_TRUE(frames.wire.empty());
}

TEST(worker_pool, rejects_payload_larger_than_byte_limit) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    cfg.input_queue_max_bytes = 8;
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, nullptr, worker_log());

    pool.start();
    EXPECT_FALSE(pool.enqueue(std::vector<char>(9, 'x')));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.input_dropped, 1u);
    EXPECT_EQ(stats.processed, 0u);
}

TEST(worker_pool, stop_drains_every_accepted_input) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, nullptr, worker_log());

    pool.start();
    std::size_t accepted = 0;
    for (std::size_t i = 0; i < 100; ++i) {
        // 0xc1 is reserved/invalid in MessagePack, so it exercises processing
        // without creating publication work that would need a connection.
        if (pool.enqueue(std::vector<char>{static_cast<char>(0xc1)})) ++accepted;
    }
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.processed, accepted);
    EXPECT_EQ(stats.queue_depth, 0u);
    EXPECT_EQ(stats.queue_bytes, 0u);
    // Invalid MessagePack fails deserialization before matching_engine::search()
    // is ever reached, so none of these should count towards match timing.
    EXPECT_EQ(stats.match_time_count, 0u);
}

TEST(worker_pool, tracks_match_time_when_search_runs) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    subscriptions.subscribe("value > 10", "client-1");
    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, nullptr, worker_log());

    pool.start();
    // value=5 does not satisfy "value > 10" - search runs (and is timed) but
    // nothing matches, so no publish path (and no connection) is needed.
    EXPECT_TRUE(pool.enqueue(matching_payload(5)));
    EXPECT_TRUE(sidecar_test::drive_until(
        ioc, [&] { return pool.get_stats().processed > 0; }, std::chrono::seconds(2)));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.matched, 0u);
    EXPECT_EQ(stats.match_time_count, 1u);
    EXPECT_GT(stats.match_time_ns_total, 0u);
}

TEST(worker_pool, publishes_matched_message_via_connection) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    subscriptions.subscribe("value > 10", "client-1");

    auto conn = std::make_shared<sidecar_test::fake_connection>();
    std::atomic<int> write_raw_calls{0};
    std::string last_wire;
    conn->on_write_raw = [&](std::span<const char> data) -> asio::awaitable<nats_asio::status> {
        write_raw_calls.fetch_add(1);
        last_wire.assign(data.data(), data.size());
        co_return nats_asio::status{};
    };

    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, conn, worker_log());
    pool.start();
    ASSERT_TRUE(pool.enqueue(matching_payload(42)));

    ASSERT_TRUE(drive_until(
        ioc, [&] { return pool.get_stats().published > 0 || pool.get_stats().publish_failures > 0; },
        std::chrono::seconds(2)));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.matched, 1u);
    EXPECT_EQ(stats.published, 1u);
    EXPECT_EQ(stats.publish_failures, 0u);
    EXPECT_EQ(stats.publish_inflight, 0u);
    EXPECT_EQ(write_raw_calls.load(), 1);
    EXPECT_NE(last_wire.find("PUB output.1 "), std::string::npos);
}

TEST(worker_pool, does_not_publish_when_message_does_not_match) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    subscriptions.subscribe("value > 10", "client-1");

    auto conn = std::make_shared<sidecar_test::fake_connection>();
    std::atomic<int> write_raw_calls{0};
    conn->on_write_raw = [&](std::span<const char>) -> asio::awaitable<nats_asio::status> {
        write_raw_calls.fetch_add(1);
        co_return nats_asio::status{};
    };

    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, conn, worker_log());
    pool.start();
    ASSERT_TRUE(pool.enqueue(matching_payload(5)));  // fails "value > 10"

    ASSERT_TRUE(drive_until(
        ioc, [&] { return pool.get_stats().processed > 0; }, std::chrono::seconds(2)));
    // No publish work should ever be posted for a non-matching message; give
    // the (absent) publish coroutine a moment it can't use, then confirm.
    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(50));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.matched, 0u);
    EXPECT_EQ(stats.published, 0u);
    EXPECT_EQ(write_raw_calls.load(), 0);
}

TEST(worker_pool, publish_failure_is_counted_and_inflight_released) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    subscriptions.subscribe("value > 10", "client-1");

    auto conn = std::make_shared<sidecar_test::fake_connection>();
    conn->on_write_raw = [](std::span<const char>) -> asio::awaitable<nats_asio::status> {
        co_return nats_asio::status(nats_asio::error_code::connection_closed);
    };

    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, conn, worker_log());
    pool.start();
    ASSERT_TRUE(pool.enqueue(matching_payload(42)));

    ASSERT_TRUE(drive_until(
        ioc, [&] { return pool.get_stats().publish_failures > 0; }, std::chrono::seconds(2)));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.matched, 1u);
    EXPECT_EQ(stats.published, 0u);
    EXPECT_EQ(stats.publish_failures, 1u);
    EXPECT_EQ(stats.publish_inflight, 0u);
}

TEST(worker_pool, backpressure_timeout_fails_publish_without_writing) {
    asio::io_context ioc(1);
    auto cfg = worker_config();
    sidecar::attribute_schema schema(cfg.attributes);
    sidecar::subscription_manager subscriptions(cfg.attributes, cfg.output_prefix, worker_log());
    subscriptions.subscribe("value > 10", "client-1");

    auto conn = std::make_shared<sidecar_test::fake_connection>();
    std::atomic<int> write_raw_calls{0};
    conn->on_is_backpressure_active = [] { return true; };
    conn->on_wait_for_drain = [](std::chrono::milliseconds) -> asio::awaitable<nats_asio::status> {
        co_return nats_asio::status(nats_asio::error_code::timeout);
    };
    conn->on_write_raw = [&](std::span<const char>) -> asio::awaitable<nats_asio::status> {
        write_raw_calls.fetch_add(1);
        co_return nats_asio::status{};
    };

    sidecar::worker_pool pool(ioc, cfg, schema, subscriptions, conn, worker_log());
    pool.start();
    ASSERT_TRUE(pool.enqueue(matching_payload(42)));

    ASSERT_TRUE(drive_until(
        ioc, [&] { return pool.get_stats().publish_failures > 0; }, std::chrono::seconds(2)));
    pool.stop();

    auto stats = pool.get_stats();
    EXPECT_EQ(stats.publish_failures, 1u);
    EXPECT_EQ(stats.published, 0u);
    EXPECT_EQ(stats.publish_inflight, 0u);
    EXPECT_EQ(write_raw_calls.load(), 0)
        << "write_raw must not be called once the backpressure wait times out";
}
