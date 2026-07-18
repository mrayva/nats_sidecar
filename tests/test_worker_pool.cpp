#include "worker_pool.hpp"
#include <asio/io_context.hpp>
#include <gtest/gtest.h>
#include <spdlog/sinks/null_sink.h>

namespace {

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

} // namespace

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
}
