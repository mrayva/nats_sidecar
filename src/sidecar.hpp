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
#include <nlohmann/json_fwd.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace sidecar {

struct sidecar_engine_test_access;

class sidecar_engine {
public:
    sidecar_engine(asio::io_context& ioc, const config& cfg,
                   std::shared_ptr<spdlog::logger> log);

    // Called once the NATS connection is established.
    // Sets up subscriptions (input + control) and starts the lease manager.
    asio::awaitable<void> start(nats_asio::iconnection_sptr conn);

    // Stop the worker pool. Called during shutdown before ioc cleanup.
    void stop_workers();

    // Wait until every accepted output publication task has completed.
    asio::awaitable<bool> wait_for_publications(std::chrono::milliseconds timeout);

private:
    friend struct sidecar_engine_test_access;

    // Subscribes to every configured input subject, wiring each to
    // on_data_message(). Returns false (having already logged and stopped
    // the ioc) on the first subscription failure. Plain queue-group mode
    // only - mutually exclusive with subscribe_to_inputs_jetstream() below,
    // selected by cfg.jetstream_consumer_enabled().
    asio::awaitable<bool> subscribe_to_inputs();

    // Callback: incoming data message on the input subject
    asio::awaitable<void> on_data_message(
        std::string_view subject,
        std::optional<std::string_view> reply_to,
        std::span<const char> payload);

    // Durable JetStream consumer input path (loss-proof alternative to
    // subscribe_to_inputs()'s plain queue-group subscribe - see config.hpp's
    // jetstream_consumer_enabled()). Provisions/validates the JetStream
    // stream backing cfg.input_stream, following
    // lease_manager::ensure_bucket()'s exact idiom (nats_asio has no
    // dedicated stream-creation API): hand-rolled $JS.API.STREAM.INFO/CREATE
    // request/reply, fail startup outright on a real config mismatch, no
    // auto-repair.
    asio::awaitable<bool> ensure_input_stream();
    bool validate_existing_input_stream(const nlohmann::json& info) const;
    asio::awaitable<bool> create_input_stream(std::chrono::milliseconds timeout);

    // Subscribes via a durable JetStream push consumer instead of a plain
    // queue-group subscribe. Sets m_input_js_sub on success. Returns false
    // (having already logged and stopped the ioc) on failure.
    asio::awaitable<bool> subscribe_to_inputs_jetstream();

    // Callback: incoming JetStream data message. Owned (not zero-copy),
    // because ack/nak/term happen well after this callback returns - inside
    // worker_pool's own resolution logic, once match+publish has actually
    // completed - and js_message_view's zero-copy guarantee doesn't survive
    // that long.
    asio::awaitable<void> on_js_data_message(
        nats_asio::ijs_subscription& sub, const nats_asio::js_message& msg);

    // Callback: subscription request from a client (request/reply pattern)
    asio::awaitable<void> on_subscribe_request(
        std::string subject,
        std::optional<std::string> reply_to,
        std::vector<char> payload);

    // Callback: unsubscribe request from a client
    asio::awaitable<void> on_unsubscribe_request(
        std::string subject,
        std::optional<std::string> reply_to,
        std::vector<char> payload);

    // Periodic stats logging
    asio::awaitable<void> stats_loop();

    asio::io_context& m_ioc;
    config m_cfg;
    std::shared_ptr<spdlog::logger> m_log;

    nats_asio::iconnection_sptr m_conn;
    std::vector<nats_asio::isubscription_sptr> m_data_subs;
    nats_asio::ijs_subscription_sptr m_input_js_sub;  // set only in JetStream-consumer mode
    nats_asio::isubscription_sptr m_subscribe_sub;
    nats_asio::isubscription_sptr m_unsubscribe_sub;
    subscription_manager m_sub_mgr;
    attribute_schema m_schema;
    std::unique_ptr<lease_manager> m_lease_mgr;
    std::unique_ptr<worker_pool> m_worker_pool;
    std::unique_ptr<asio::steady_timer> m_stats_timer;

    // Only m_messages_received is tracked here (at enqueue time).
    // All other stats come from worker_pool::get_stats().
    std::atomic<uint64_t> m_messages_received{0};
    std::atomic<bool> m_shutting_down{false};
};

} // namespace sidecar
