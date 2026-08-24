#include "sidecar.hpp"
#include <nlohmann/json.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/use_awaitable.hpp>
#include <algorithm>
#include <charconv>

namespace sidecar {

sidecar_engine::sidecar_engine(asio::io_context& ioc, const config& cfg,
                               std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_cfg(cfg), m_log(std::move(log)),
      m_sub_mgr(cfg.attributes, cfg.output_prefix, m_log, cfg.engine),
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

    if (m_cfg.jetstream_consumer_enabled()) {
        // Provision the stream and subscribe *before* worker_pool exists,
        // not after - m_input_js_sub is passed into worker_pool's
        // constructor so it's fully initialized before worker threads ever
        // start (the same happens-before guarantee m_conn/m_log already
        // rely on), rather than a late setter that worker threads could
        // race against. No message can arrive in the gap between
        // js_subscribe() returning and worker_pool being constructed below:
        // this coroutine runs on the single-threaded ioc and never yields
        // to it (no co_await) between the two, so nothing can interleave.
        if (!co_await ensure_input_stream()) {
            m_log->error("Failed to provision input stream '{}'; refusing unsafe startup",
                        m_cfg.input_stream);
            m_ioc.stop();
            co_return;
        }
        if (!co_await subscribe_to_inputs_jetstream()) co_return;

        m_worker_pool = std::make_unique<worker_pool>(
            m_ioc, m_cfg, m_schema, m_sub_mgr, m_conn, m_log, m_input_js_sub);
        m_worker_pool->start();
    } else {
        m_worker_pool = std::make_unique<worker_pool>(
            m_ioc, m_cfg, m_schema, m_sub_mgr, m_conn, m_log);
        m_worker_pool->start();

        if (!co_await subscribe_to_inputs()) co_return;
    }

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
    for (auto& sub : m_data_subs) { if (sub) sub->cancel(); }
    if (m_input_js_sub) m_input_js_sub->stop();
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

asio::awaitable<bool> sidecar_engine::subscribe_to_inputs() {
    // Subscribe to every configured input data subject - all share this
    // engine's one on_data_message() callback and one subscription_manager,
    // so messages from any of them are matched against the same tree.
    nats_asio::subscribe_options data_opts;
    if (!m_cfg.input_queue_group.empty()) {
        data_opts.queue_group = m_cfg.input_queue_group;
    }

    for (const auto& subject : m_cfg.input_subjects) {
        auto [data_sub, data_status] = co_await m_conn->subscribe(
            subject,
            [this](auto sub, auto reply_to, auto payload) {
                return on_data_message(sub, reply_to, payload);
            },
            data_opts
        );

        if (data_status.failed()) {
            m_log->error("Failed to subscribe to input subject '{}': {}",
                        subject, data_status.error());
            m_ioc.stop();
            co_return false;
        }
        m_data_subs.push_back(std::move(data_sub));
        m_log->info("Subscribed to input subject '{}'", subject);
    }
    co_return true;
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

asio::awaitable<bool> sidecar_engine::ensure_input_stream() {
    constexpr auto timeout = std::chrono::seconds(5);

    const std::string info_subject = "$JS.API.STREAM.INFO." + m_cfg.input_stream;
    const std::string empty_payload = "{}";
    auto [info_reply, info_status] = co_await m_conn->request(
        info_subject,
        std::span<const char>(empty_payload.data(), empty_payload.size()),
        timeout);

    if (info_status.failed()) {
        m_log->error("Failed to inspect input stream '{}': {}",
                     m_cfg.input_stream, info_status.error());
        co_return false;
    }

    nlohmann::json info;
    bool missing = false;
    try {
        info = nlohmann::json::parse(
            std::string_view(info_reply.payload.data(), info_reply.payload.size()));
        missing = info.contains("error") && info["error"].value("code", 0) == 404;
    } catch (const std::exception& e) {
        m_log->error("Invalid input stream info response: {}", e.what());
        co_return false;
    }

    if (!missing) co_return validate_existing_input_stream(info);
    co_return co_await create_input_stream(timeout);
}

bool sidecar_engine::validate_existing_input_stream(const nlohmann::json& info) const {
    if (info.contains("error") || !info.contains("config")) {
        auto description = info.contains("error")
            ? info["error"].value("description", "stream info failed")
            : std::string("missing stream config");
        m_log->error("Failed to inspect input stream '{}': {}", m_cfg.input_stream, description);
        return false;
    }

    const auto& cfg = info["config"];
    const auto retention = cfg.value("retention", std::string{});
    const auto subjects = cfg.value("subjects", std::vector<std::string>{});

    // "workqueue" retention is what makes ack-based cleanup work at all
    // (a message is removed once its one logical consumer acks it) and is
    // also what enforces exactly one consumer group on this stream - the
    // same one-durable-consumer-shared-by-N-instances design this whole
    // redesign depends on. Any other retention policy is a real mismatch,
    // not a cosmetic difference.
    if (retention != "workqueue") {
        m_log->error(
            "Input stream '{}' has retention='{}', expected 'workqueue' - "
            "refusing to use a stream not configured for ack-based cleanup",
            m_cfg.input_stream, retention);
        return false;
    }

    for (const auto& subject : m_cfg.input_subjects) {
        if (std::find(subjects.begin(), subjects.end(), subject) == subjects.end()) {
            m_log->error(
                "Input stream '{}' does not capture configured input subject '{}' "
                "(stream subjects: {})",
                m_cfg.input_stream, subject, subjects.empty() ? "<none>" : subjects.front());
            return false;
        }
    }

    m_log->info("Validated JetStream input stream '{}' ({} subject(s), retention=workqueue)",
               m_cfg.input_stream, m_cfg.input_subjects.size());
    return true;
}

asio::awaitable<bool> sidecar_engine::create_input_stream(std::chrono::milliseconds timeout) {
    nlohmann::json stream_config = {
        {"name", m_cfg.input_stream},
        {"subjects", m_cfg.input_subjects},
        {"retention", "workqueue"},
        {"storage", m_cfg.input_stream_storage},
        {"max_msgs", -1},
        {"max_bytes", -1},
        {"max_age", 0},
        {"max_msg_size", -1},
        {"discard", "old"},
        {"num_replicas", 1}
    };
    std::string payload_str = stream_config.dump();
    auto [create_reply, create_status] = co_await m_conn->request(
        "$JS.API.STREAM.CREATE." + m_cfg.input_stream,
        std::span<const char>(payload_str.data(), payload_str.size()), timeout);
    if (create_status.failed()) {
        m_log->error("Failed to create input stream '{}': {}",
                     m_cfg.input_stream, create_status.error());
        co_return false;
    }

    try {
        auto response = nlohmann::json::parse(
            std::string_view(create_reply.payload.data(), create_reply.payload.size()));
        if (response.contains("error")) {
            m_log->error("Failed to create input stream '{}': {}", m_cfg.input_stream,
                        response["error"].value("description", "unknown error"));
            co_return false;
        }
    } catch (const std::exception& e) {
        m_log->error("Invalid input stream creation response: {}", e.what());
        co_return false;
    }

    m_log->info("Created JetStream input stream '{}' ({} subject(s), retention=workqueue)",
               m_cfg.input_stream, m_cfg.input_subjects.size());
    co_return true;
}

asio::awaitable<bool> sidecar_engine::subscribe_to_inputs_jetstream() {
    nats_asio::js_consumer_config js_cfg;
    js_cfg.stream = m_cfg.input_stream;
    js_cfg.durable_name = m_cfg.consumer_durable_name;
    // deliver_subject is required (validated in finalize_and_validate_config)
    // and must never be left unset - see config.hpp's comment on why an
    // auto-generated inbox would silently break multi-instance sharing.
    js_cfg.deliver_subject = m_cfg.consumer_deliver_subject;
    if (!m_cfg.consumer_deliver_group.empty()) {
        js_cfg.deliver_group = m_cfg.consumer_deliver_group;
    }
    js_cfg.ack = nats_asio::js_ack_policy::explicit_;
    js_cfg.ack_wait = std::chrono::seconds(m_cfg.consumer_ack_wait_seconds);
    js_cfg.max_ack_pending = m_cfg.consumer_max_ack_pending;

    auto [js_sub, js_status] = co_await m_conn->js_subscribe(
        js_cfg,
        [this](nats_asio::ijs_subscription& sub, const nats_asio::js_message& msg) {
            return on_js_data_message(sub, msg);
        });

    if (js_status.failed()) {
        m_log->error(
            "Failed to subscribe to JetStream input (stream='{}', durable='{}', "
            "deliver_subject='{}'): {}",
            m_cfg.input_stream, m_cfg.consumer_durable_name,
            m_cfg.consumer_deliver_subject, js_status.error());
        m_ioc.stop();
        co_return false;
    }
    m_input_js_sub = std::move(js_sub);
    m_log->info(
        "Subscribed to JetStream input stream '{}' (durable='{}', deliver_subject='{}', "
        "deliver_group='{}', max_ack_pending={}, ack_wait={}s)",
        m_cfg.input_stream, m_cfg.consumer_durable_name, m_cfg.consumer_deliver_subject,
        m_cfg.consumer_deliver_group.empty() ? "<none>" : m_cfg.consumer_deliver_group,
        m_cfg.consumer_max_ack_pending, m_cfg.consumer_ack_wait_seconds);
    co_return true;
}

asio::awaitable<void> sidecar_engine::on_js_data_message(
    nats_asio::ijs_subscription& /*sub*/, const nats_asio::js_message& msg)
{
    m_messages_received++;

    if (msg.msg.payload.empty()) {
        // A real delivered message with nothing to process - ack now
        // rather than leave it to redeliver forever for no reason.
        if (m_input_js_sub) {
            auto s = co_await m_input_js_sub->ack(msg);
            if (s.failed()) {
                m_log->warn("Failed to ack empty JetStream input message: {}", s.error());
            }
        }
        co_return;
    }

    if (!m_worker_pool) {
        // Startup-ordering edge case that shouldn't occur in practice (the
        // JetStream subscribe only starts once worker_pool has been
        // constructed with this subscription already wired in) - leave
        // unacked rather than lose it; ack_wait will trigger redelivery
        // once the pool is actually ready.
        m_log->warn("Received JetStream data before worker pool initialization; "
                    "message will redeliver");
        co_return;
    }

    // Copy the payload and the owned js_message for worker processing.
    // ack/nak/term happen later, from worker_pool's own resolution logic,
    // once match+publish has actually completed - not here.
    if (!m_worker_pool->enqueue(msg.msg.payload, msg)) {
        // Backpressure at worker_pool's own queue limit - leave unacked,
        // same "let ack_wait redeliver" rule worker_loop's own
        // publish-inflight backpressure path follows.
        m_log->debug("Input queue full or stopping; JetStream message will redeliver");
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

        // A client that wants to broadcast this request to every instance
        // sharing one control subject (fan-out, not queue group) must supply
        // its own ID rather than let each instance's uncoordinated
        // m_next_id++ counter assign one - otherwise two instances handling
        // the same expression independently could land on different IDs and
        // thus different output topics, with no way for the client to know
        // which instance said what. When "id" is present, restore() is used
        // instead of subscribe(): it adopts the given ID if the expression
        // matches (idempotent, safe to replay), or fails if that ID is
        // already bound to a *different* expression on this instance - a
        // real conflict, surfaced below as an error reply, not silently
        // absorbed. Omitting "id" keeps the original per-instance-assigned
        // behavior for callers still using one control subject per instance.
        uint64_t sub_id;
        if (auto id_it = req.find("id"); id_it != req.end()) {
            sub_id = id_it->get<uint64_t>();
            if (!m_sub_mgr.restore(sub_id, expression, client_id)) {
                throw std::runtime_error(
                    "subscription id " + std::to_string(sub_id) +
                    " is already bound to a different expression on this instance");
            }
        } else {
            sub_id = m_sub_mgr.subscribe(expression, client_id);
        }
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

    } catch (const matching_engine_error& e) {
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

        // not_found also reports removed=true: it means the subscription is
        // already gone (e.g. lease_manager's reconciliation loop expired it
        // concurrently), which is the outcome the caller asked for.
        auto removal = m_sub_mgr.remove_lease(sub_id, client_id);
        bool fully_removed = removal != lease_removal::still_active;

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
        double avg_match_us = ws.match_time_count > 0
            ? (double(ws.match_time_ns_total) / double(ws.match_time_count)) / 1000.0
            : 0.0;

        m_log->info("stats: received={} processed={} matched={} published={} "
                    "match_failures={} publish_failures={} input_dropped={} "
                    "publish_tasks_dropped={} subscriptions={} queue_depth={} "
                    "queue_bytes={} publish_inflight={} avg_match_us={:.2f}",
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
                   ws.publish_inflight,
                   avg_match_us);
    }
}

} // namespace sidecar
