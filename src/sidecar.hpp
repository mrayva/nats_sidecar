#pragma once

#include "config.hpp"
#include "event_bridge.hpp"
#include "subscription_manager.hpp"
#include "lease_manager.hpp"
#include "worker_pool.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <memory>
#include <string>

namespace sidecar {

class sidecar_engine {
public:
    sidecar_engine(asio::io_context& ioc, const config& cfg,
                   std::shared_ptr<spdlog::logger> log);

    // Called once the NATS connection is established.
    // Sets up subscriptions (input + control) and starts the lease manager.
    asio::awaitable<void> start(nats_asio::iconnection_sptr conn);

    // Stop the worker pool. Called during shutdown before ioc cleanup.
    void stop_workers();

private:
    // Callback: incoming data message on the input subject
    asio::awaitable<void> on_data_message(
        std::string_view subject,
        std::optional<std::string_view> reply_to,
        std::span<const char> payload);

    // Callback: subscription request from a client (request/reply pattern)
    asio::awaitable<void> on_subscribe_request(
        std::string_view subject,
        std::optional<std::string_view> reply_to,
        std::span<const char> payload);

    // Callback: unsubscribe request from a client
    asio::awaitable<void> on_unsubscribe_request(
        std::string_view subject,
        std::optional<std::string_view> reply_to,
        std::span<const char> payload);

    // Periodic stats logging
    asio::awaitable<void> stats_loop();

    asio::io_context& m_ioc;
    config m_cfg;
    std::shared_ptr<spdlog::logger> m_log;

    nats_asio::iconnection_sptr m_conn;
    subscription_manager m_sub_mgr;
    attribute_schema m_schema;
    std::unique_ptr<lease_manager> m_lease_mgr;
    std::unique_ptr<worker_pool> m_worker_pool;

    // Only m_messages_received is tracked here (at enqueue time).
    // All other stats come from worker_pool::get_stats().
    std::atomic<uint64_t> m_messages_received{0};
};

} // namespace sidecar
