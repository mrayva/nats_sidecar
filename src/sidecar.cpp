#include "sidecar.hpp"
#include <nlohmann/json.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/use_awaitable.hpp>
#include <charconv>

namespace sidecar {

sidecar_engine::sidecar_engine(asio::io_context& ioc, const config& cfg,
                               std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_cfg(cfg), m_log(std::move(log)),
      m_sub_mgr(cfg.attributes, cfg.output_prefix, m_log),
      m_schema(cfg.attributes)
{}

asio::awaitable<void> sidecar_engine::start(nats_asio::iconnection_sptr conn) {
    m_conn = std::move(conn);
    m_shutting_down.store(false, std::memory_order_relaxed);

    // Provision/validate the lease bucket, start reconciliation, and restore
    // persisted subscriptions before accepting any input data.
    m_lease_mgr = std::make_unique<lease_manager>(
        m_ioc, m_conn, m_sub_mgr, m_cfg.lease_bucket,
        m_cfg.lease_ttl_seconds, m_cfg.lease_check_interval_seconds, m_log);

    if (!co_await m_lease_mgr->start()) {
        m_log->error("Lease manager failed to start; refusing unsafe startup");
        m_ioc.stop();
        co_return;
    }

    m_worker_pool = std::make_unique<worker_pool>(
        m_ioc, m_cfg, m_schema, m_sub_mgr, m_conn, m_log);
    m_worker_pool->start();

    // Subscribe to the input data subject
    nats_asio::subscribe_options data_opts;
    if (!m_cfg.input_queue_group.empty()) {
        data_opts.queue_group = m_cfg.input_queue_group;
    }

    auto [data_sub, data_status] = co_await m_conn->subscribe(
        m_cfg.input_subject,
        [this](auto subject, auto reply_to, auto payload) {
            return on_data_message(subject, reply_to, payload);
        },
        data_opts
    );

    if (data_status.failed()) {
        m_log->error("Failed to subscribe to input subject '{}': {}",
                    m_cfg.input_subject, data_status.error());
        m_ioc.stop();
        co_return;
    }
    m_data_sub = std::move(data_sub);
    m_log->info("Subscribed to input subject '{}'", m_cfg.input_subject);

    // Subscribe to subscription control subject (request/reply)
    auto [sub_ctrl, sub_ctrl_status] = co_await m_conn->subscribe(
        m_cfg.subscribe_subject,
        [this](auto subject, auto reply_to, auto payload) -> asio::awaitable<void> {
            std::string subject_copy(subject);
            std::optional<std::string> reply_copy;
            if (reply_to) reply_copy = std::string(*reply_to);
            std::vector<char> payload_copy(payload.begin(), payload.end());
            asio::co_spawn(
                m_ioc,
                on_subscribe_request(std::move(subject_copy), std::move(reply_copy),
                                     std::move(payload_copy)),
                asio::detached);
            co_return;
        }
    );

    if (sub_ctrl_status.failed()) {
        m_log->error("Failed to subscribe to control subject '{}': {}",
                    m_cfg.subscribe_subject, sub_ctrl_status.error());
        m_ioc.stop();
        co_return;
    }
    m_subscribe_sub = std::move(sub_ctrl);
    m_log->info("Listening for subscription requests on '{}'", m_cfg.subscribe_subject);

    // Subscribe to unsubscribe control subject
    auto [unsub_ctrl, unsub_ctrl_status] = co_await m_conn->subscribe(
        m_cfg.unsubscribe_subject,
        [this](auto subject, auto reply_to, auto payload) -> asio::awaitable<void> {
            std::string subject_copy(subject);
            std::optional<std::string> reply_copy;
            if (reply_to) reply_copy = std::string(*reply_to);
            std::vector<char> payload_copy(payload.begin(), payload.end());
            asio::co_spawn(
                m_ioc,
                on_unsubscribe_request(std::move(subject_copy), std::move(reply_copy),
                                       std::move(payload_copy)),
                asio::detached);
            co_return;
        }
    );

    if (unsub_ctrl_status.failed()) {
        m_log->error("Failed to subscribe to unsubscribe subject '{}': {}",
                    m_cfg.unsubscribe_subject, unsub_ctrl_status.error());
        m_ioc.stop();
        co_return;
    }
    m_unsubscribe_sub = std::move(unsub_ctrl);
    m_log->info("Listening for unsubscribe requests on '{}'", m_cfg.unsubscribe_subject);

    // Start stats reporting
    m_stats_timer = std::make_unique<asio::steady_timer>(m_ioc);
    asio::co_spawn(m_ioc, stats_loop(), asio::detached);

    m_log->info("Sidecar engine started (format={}, {} attributes, output={}.<ID>)",
               static_cast<int>(m_cfg.format), m_cfg.attributes.size(), m_cfg.output_prefix);
}

void sidecar_engine::stop_workers() {
    m_shutting_down.store(true, std::memory_order_relaxed);
    if (m_data_sub) m_data_sub->cancel();
    if (m_subscribe_sub) m_subscribe_sub->cancel();
    if (m_unsubscribe_sub) m_unsubscribe_sub->cancel();
    if (m_lease_mgr) m_lease_mgr->stop();
    if (m_stats_timer) {
        std::error_code ec;
        m_stats_timer->cancel(ec);
        if (ec) {
            m_log->debug("Failed to cancel stats timer: {}", ec.message());
        }
    }
    if (m_worker_pool) {
        m_worker_pool->stop();
    }
}

asio::awaitable<bool> sidecar_engine::wait_for_publications(
    std::chrono::milliseconds timeout) {
    if (!m_worker_pool) co_return true;
    co_return co_await m_worker_pool->wait_for_publications(timeout);
}

asio::awaitable<void> sidecar_engine::on_data_message(
    std::string_view /*subject*/,
    std::optional<std::string_view> /*reply_to*/,
    std::span<const char> payload)
{
    m_messages_received++;

    // Skip empty payloads
    if (payload.empty()) co_return;

    if (!m_worker_pool) {
        m_log->warn("Received data before worker pool initialization; dropping payload");
        co_return;
    }

    // Copy payload and enqueue for worker processing
    std::vector<char> payload_copy(payload.begin(), payload.end());
    if (!m_worker_pool->enqueue(std::move(payload_copy))) {
        m_log->debug("Input queue full or stopping; dropped payload");
    }
}

asio::awaitable<void> sidecar_engine::on_subscribe_request(
    std::string /*subject*/,
    std::optional<std::string> reply_to,
    std::vector<char> payload)
{
    if (!reply_to) {
        m_log->warn("Subscribe request without reply_to - ignoring");
        co_return;
    }

    // Parse request: JSON { "expression": "...", "client_id": "..." }
    std::string reply_subject(*reply_to);
    std::string reply_str;

    try {
        auto req = nlohmann::json::parse(
            std::string_view(payload.data(), payload.size()));

        std::string expression = req.at("expression").get<std::string>();
        std::string client_id = req.at("client_id").get<std::string>();

        bool already_held = false;
        if (auto existing_id = m_sub_mgr.find_by_expression(expression)) {
            if (auto existing = m_sub_mgr.get_subscription(*existing_id)) {
                already_held = existing->lease_holders.contains(client_id);
            }
        }

        uint64_t sub_id = m_sub_mgr.subscribe(expression, client_id);
        if (!co_await m_lease_mgr->persist_lease(sub_id, expression, client_id)) {
            if (!already_held) m_sub_mgr.remove_lease(sub_id, client_id);
            throw std::runtime_error("failed to create or refresh lease");
        }

        std::string lease_key = lease_manager::make_lease_key(sub_id, client_id);

        nlohmann::json reply = {
            {"id", sub_id},
            {"topic", m_cfg.output_prefix + "." + std::to_string(sub_id)},
            {"lease_bucket", m_cfg.lease_bucket},
            {"lease_key", lease_key},
            {"lease_ttl_seconds", m_cfg.lease_ttl_seconds}
        };
        reply_str = reply.dump();

    } catch (const atree::Error& e) {
        reply_str = nlohmann::json({{"error", std::string("Invalid expression: ") + e.what()}}).dump();
    } catch (const std::exception& e) {
        reply_str = nlohmann::json({{"error", std::string("Bad request: ") + e.what()}}).dump();
    }

    auto s = co_await m_conn->publish(
        reply_subject,
        std::span<const char>(reply_str.data(), reply_str.size()),
        std::nullopt);

    if (s.failed()) {
        m_log->error("Failed to reply to subscribe request: {}", s.error());
    }
}

asio::awaitable<void> sidecar_engine::on_unsubscribe_request(
    std::string /*subject*/,
    std::optional<std::string> reply_to,
    std::vector<char> payload)
{
    std::string reply_subject;
    if (reply_to) reply_subject = std::string(*reply_to);

    std::string reply_str;
    try {
        auto req = nlohmann::json::parse(
            std::string_view(payload.data(), payload.size()));

        uint64_t sub_id = req.at("id").get<uint64_t>();
        std::string client_id = req.at("client_id").get<std::string>();

        if (!co_await m_lease_mgr->delete_lease(sub_id, client_id)) {
            throw std::runtime_error("failed to delete lease");
        }

        bool fully_removed = m_sub_mgr.remove_lease(sub_id, client_id);
        if (!fully_removed && !m_sub_mgr.get_subscription(sub_id)) {
            // The KV watcher may have processed the delete while kv_delete was
            // awaiting its acknowledgment.
            fully_removed = true;
        }

        reply_str = nlohmann::json({{"id", sub_id}, {"removed", fully_removed}}).dump();

    } catch (const std::exception& e) {
        reply_str = nlohmann::json({{"error", std::string("Bad request: ") + e.what()}}).dump();
    }

    if (!reply_subject.empty()) {
        co_await m_conn->publish(
            reply_subject,
            std::span<const char>(reply_str.data(), reply_str.size()),
            std::nullopt);
    }
}

asio::awaitable<void> sidecar_engine::stats_loop() {
    while (!m_shutting_down.load(std::memory_order_relaxed)) {
        m_stats_timer->expires_after(std::chrono::seconds(m_cfg.stats_interval_seconds));
        std::error_code ec;
        co_await m_stats_timer->async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (m_shutting_down.load(std::memory_order_relaxed) ||
            ec == asio::error::operation_aborted) {
            co_return;
        }
        if (ec) {
            m_log->debug("stats loop timer error: {}", ec.message());
            co_return;
        }

        auto ws = m_worker_pool ? m_worker_pool->get_stats() : worker_pool::stats{};

        m_log->info("stats: received={} processed={} matched={} published={} "
                    "match_failures={} publish_failures={} input_dropped={} "
                    "publish_tasks_dropped={} subscriptions={} queue_depth={} "
                    "queue_bytes={} publish_inflight={}",
                   m_messages_received.load(),
                   ws.processed,
                   ws.matched,
                   ws.published,
                   ws.match_failures,
                   ws.publish_failures,
                   ws.input_dropped,
                   ws.publish_tasks_dropped,
                   m_sub_mgr.active_count(),
                   ws.queue_depth,
                   ws.queue_bytes,
                   ws.publish_inflight);
    }
}

} // namespace sidecar
