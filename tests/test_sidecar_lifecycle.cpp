#include "sidecar.hpp"
#include <gtest/gtest.h>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <spdlog/sinks/null_sink.h>

namespace {

auto make_log() {
    return std::make_shared<spdlog::logger>(
        "test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

sidecar::config sample_config() {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";
    cfg.output_prefix = "sensor.filtered";
    cfg.stats_interval_seconds = 3600;
    cfg.attributes = {
        {"temperature", sidecar::attribute_type::float_val},
    };
    return cfg;
}

} // namespace

namespace sidecar {

struct sidecar_engine_test_access {
    static void start_stats_loop(sidecar::sidecar_engine& engine) {
        engine.m_shutting_down.store(false, std::memory_order_relaxed);
        engine.m_stats_timer = std::make_unique<asio::steady_timer>(engine.m_ioc);
        asio::co_spawn(engine.m_ioc, engine.stats_loop(), asio::detached);
    }

    static bool shutting_down(const sidecar::sidecar_engine& engine) {
        return engine.m_shutting_down.load(std::memory_order_relaxed);
    }
};

} // namespace sidecar

TEST(sidecar_engine, stop_workers_cancels_stats_loop) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());

    sidecar::sidecar_engine_test_access::start_stats_loop(engine);

    asio::steady_timer shutdown_timer(ioc);
    shutdown_timer.expires_after(std::chrono::milliseconds(10));
    shutdown_timer.async_wait([&engine](const std::error_code& ec) {
        if (!ec) {
            engine.stop_workers();
        }
    });

    ioc.run_for(std::chrono::milliseconds(200));

    EXPECT_TRUE(ioc.stopped());
    EXPECT_TRUE(sidecar::sidecar_engine_test_access::shutting_down(engine));
}
