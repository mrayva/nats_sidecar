#include "worker_pool.hpp"
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <chrono>

namespace sidecar {

worker_pool::worker_pool(asio::io_context& ioc, const config& cfg,
                         const attribute_schema& schema,
                         subscription_manager& sub_mgr,
                         nats_asio::iconnection_sptr conn,
                         std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_format(cfg.format), m_schema(schema),
      m_sub_mgr(sub_mgr), m_conn(std::move(conn)), m_log(std::move(log)),
      m_thread_count(cfg.worker_threads > 0 ? cfg.worker_threads
                                            : std::thread::hardware_concurrency()),
      m_queue_max_messages(cfg.input_queue_max_messages),
      m_queue_max_bytes(cfg.input_queue_max_bytes),
      m_publish_max_inflight(cfg.publish_max_inflight),
      m_publish_backpressure_timeout(cfg.publish_backpressure_timeout_ms)
{
    if (m_thread_count == 0) m_thread_count = 1;
}

worker_pool::~worker_pool() {
    stop();
}

void worker_pool::start() {
    if (m_running.exchange(true)) return; // already started
    m_accepting.store(true, std::memory_order_release);

    m_threads.reserve(m_thread_count);
    for (unsigned int i = 0; i < m_thread_count; ++i) {
        m_threads.emplace_back(&worker_pool::worker_loop, this, i);
    }
    m_log->info("Worker pool started with {} threads", m_thread_count);
}

void worker_pool::stop() {
    {
        std::lock_guard<std::mutex> lock(m_enqueue_mutex);
        m_accepting.store(false, std::memory_order_release);
        if (!m_running.exchange(false)) return; // already stopped
    }

    for (auto& t : m_threads) {
        if (t.joinable()) t.join();
    }
    m_threads.clear();
    m_log->info("Worker pool stopped");
}

bool worker_pool::enqueue(std::vector<char> payload) {
    std::lock_guard<std::mutex> lock(m_enqueue_mutex);
    if (!m_accepting.load(std::memory_order_acquire) ||
        m_queued_messages.load(std::memory_order_relaxed) >= m_queue_max_messages ||
        payload.size() > m_queue_max_bytes - std::min(
            m_queue_max_bytes, m_queued_bytes.load(std::memory_order_relaxed))) {
        m_input_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    const auto bytes = payload.size();
    m_queued_messages.fetch_add(1, std::memory_order_relaxed);
    m_queued_bytes.fetch_add(bytes, std::memory_order_relaxed);
    if (!m_queue.enqueue(std::move(payload))) {
        m_queued_messages.fetch_sub(1, std::memory_order_relaxed);
        m_queued_bytes.fetch_sub(bytes, std::memory_order_relaxed);
        m_input_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

std::size_t worker_pool::queue_depth() const {
    return m_queued_messages.load(std::memory_order_relaxed);
}

worker_pool::stats worker_pool::get_stats() const {
    return {
        m_processed.load(std::memory_order_relaxed),
        m_matched.load(std::memory_order_relaxed),
        m_published.load(std::memory_order_relaxed),
        m_match_failures.load(std::memory_order_relaxed),
        m_input_dropped.load(std::memory_order_relaxed),
        m_publish_tasks_dropped.load(std::memory_order_relaxed),
        m_publish_failures.load(std::memory_order_relaxed),
        m_queued_messages.load(std::memory_order_relaxed),
        m_queued_bytes.load(std::memory_order_relaxed),
        m_publish_inflight.load(std::memory_order_relaxed)
    };
}

asio::awaitable<bool> worker_pool::wait_for_publications(
    std::chrono::milliseconds timeout) {
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (m_publish_inflight.load(std::memory_order_acquire) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) co_return false;
        timer.expires_after(std::chrono::milliseconds(10));
        std::error_code ec;
        co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec && ec != asio::error::operation_aborted) co_return false;
    }
    co_return true;
}

void worker_pool::worker_loop(unsigned int worker_id) {
    m_log->debug("Worker {} started", worker_id);

    std::vector<char> payload;
    while (m_running.load(std::memory_order_acquire) ||
           m_queued_messages.load(std::memory_order_acquire) != 0) {
        // Block with timeout to allow checking m_running for graceful shutdown
        bool got = m_queue.wait_dequeue_timed(
            payload, std::chrono::milliseconds(100));

        if (!got) continue;

        m_queued_messages.fetch_sub(1, std::memory_order_relaxed);
        m_queued_bytes.fetch_sub(payload.size(), std::memory_order_relaxed);

        // Get current snapshot — lock-free atomic load
        auto snap = m_sub_mgr.snapshot();
        if (!snap || !snap->tree) {
            payload.clear();
            continue;
        }

        std::span<const char> payload_span(payload.data(), payload.size());

        auto matches = deserialize_and_match(
            *snap->tree, m_schema, m_format, payload_span, m_log);

        m_processed.fetch_add(1, std::memory_order_relaxed);

        if (!matches) {
            m_match_failures.fetch_add(1, std::memory_order_relaxed);
            payload.clear();
            continue;
        }

        if (matches->empty()) {
            payload.clear();
            continue;
        }

        m_matched.fetch_add(1, std::memory_order_relaxed);

        const auto previous_inflight = m_publish_inflight.fetch_add(
            1, std::memory_order_acq_rel);
        if (previous_inflight >= m_publish_max_inflight) {
            m_publish_inflight.fetch_sub(1, std::memory_order_acq_rel);
            m_publish_tasks_dropped.fetch_add(1, std::memory_order_relaxed);
            payload.clear();
            continue;
        }

        // Capture what we need for the publish coroutine
        auto matched_ids = std::move(*matches);
        auto payload_copy = std::move(payload);
        auto snap_copy = std::move(snap);
        auto conn = m_conn;
        auto log = m_log;
        auto& published_counter = m_published;
        auto& failure_counter = m_publish_failures;
        auto& inflight_counter = m_publish_inflight;
        auto backpressure_timeout = m_publish_backpressure_timeout;

        // Post publish work to the ASIO I/O thread
        asio::co_spawn(m_ioc,
            [matched_ids = std::move(matched_ids),
             payload_copy = std::move(payload_copy),
             snap_copy = std::move(snap_copy),
             conn = std::move(conn),
             log,
             &published_counter,
             &failure_counter,
             &inflight_counter,
             backpressure_timeout]() mutable -> asio::awaitable<void> {
                std::span<const char> pub_payload(payload_copy.data(), payload_copy.size());
                try {
                    std::string wire;
                    std::size_t output_count = 0;
                    for (uint64_t sub_id : matched_ids) {
                        auto subj_it = snap_copy->output_subjects.find(sub_id);
                        if (subj_it == snap_copy->output_subjects.end()) continue;
                        wire += "PUB ";
                        wire += subj_it->second;
                        wire += " ";
                        wire += std::to_string(pub_payload.size());
                        wire += "\r\n";
                        wire.append(pub_payload.data(), pub_payload.size());
                        wire += "\r\n";
                        ++output_count;
                    }
                    if (!wire.empty()) {
                        if (conn->is_backpressure_active()) {
                            auto drain_status = co_await conn->wait_for_drain(
                                backpressure_timeout);
                            if (drain_status.failed()) {
                                failure_counter.fetch_add(1, std::memory_order_relaxed);
                                log->warn("Output backpressure wait failed: {}",
                                          drain_status.error());
                                inflight_counter.fetch_sub(1, std::memory_order_acq_rel);
                                co_return;
                            }
                        }
                        auto write_status = co_await conn->write_raw(
                            std::span<const char>(wire.data(), wire.size()));
                        if (write_status.failed()) {
                            failure_counter.fetch_add(1, std::memory_order_relaxed);
                            log->warn("Failed to write matched publications: {}",
                                      write_status.error());
                        } else {
                            published_counter.fetch_add(output_count,
                                                        std::memory_order_relaxed);
                        }
                    }
                } catch (const std::exception& e) {
                    failure_counter.fetch_add(1, std::memory_order_relaxed);
                    log->error("Publication task failed: {}", e.what());
                }
                inflight_counter.fetch_sub(1, std::memory_order_acq_rel);
            },
            asio::detached
        );

        payload.clear();
    }

    m_log->debug("Worker {} stopped", worker_id);
}

} // namespace sidecar
