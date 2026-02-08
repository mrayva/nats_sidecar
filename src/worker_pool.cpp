#include "worker_pool.hpp"
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
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
                                            : std::thread::hardware_concurrency())
{
    if (m_thread_count == 0) m_thread_count = 1;
}

worker_pool::~worker_pool() {
    stop();
}

void worker_pool::start() {
    if (m_running.exchange(true)) return; // already started

    m_threads.reserve(m_thread_count);
    for (unsigned int i = 0; i < m_thread_count; ++i) {
        m_threads.emplace_back(&worker_pool::worker_loop, this, i);
    }
    m_log->info("Worker pool started with {} threads", m_thread_count);
}

void worker_pool::stop() {
    if (!m_running.exchange(false)) return; // already stopped

    // Enqueue poison pills (empty vectors) — one per thread
    for (unsigned int i = 0; i < m_thread_count; ++i) {
        m_queue.enqueue(std::vector<char>{});
    }

    for (auto& t : m_threads) {
        if (t.joinable()) t.join();
    }
    m_threads.clear();
    m_log->info("Worker pool stopped");
}

void worker_pool::enqueue(std::vector<char> payload) {
    m_queue.enqueue(std::move(payload));
}

std::size_t worker_pool::queue_depth() const {
    return m_queue.size_approx();
}

worker_pool::stats worker_pool::get_stats() const {
    return {
        m_processed.load(std::memory_order_relaxed),
        m_matched.load(std::memory_order_relaxed),
        m_published.load(std::memory_order_relaxed),
        m_match_failures.load(std::memory_order_relaxed),
        m_queue.size_approx()
    };
}

void worker_pool::worker_loop(unsigned int worker_id) {
    m_log->debug("Worker {} started", worker_id);

    std::vector<char> payload;
    while (m_running.load(std::memory_order_relaxed)) {
        // Block with timeout to allow checking m_running for graceful shutdown
        bool got = m_queue.wait_dequeue_timed(
            payload, std::chrono::milliseconds(100));

        if (!got) continue;

        // Empty payload = poison pill
        if (payload.empty()) break;

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

        // Capture what we need for the publish coroutine
        auto matched_ids = std::move(*matches);
        auto payload_copy = std::move(payload);
        auto snap_copy = std::move(snap);
        auto conn = m_conn;
        auto log = m_log;
        auto& published_counter = m_published;

        // Post publish work to the ASIO I/O thread
        asio::co_spawn(m_ioc,
            [matched_ids = std::move(matched_ids),
             payload_copy = std::move(payload_copy),
             snap_copy = std::move(snap_copy),
             conn = std::move(conn),
             log,
             &published_counter]() mutable -> asio::awaitable<void> {
                std::span<const char> pub_payload(payload_copy.data(), payload_copy.size());
                for (uint64_t sub_id : matched_ids) {
                    auto subj_it = snap_copy->output_subjects.find(sub_id);
                    if (subj_it == snap_copy->output_subjects.end()) continue;

                    auto s = co_await conn->publish(
                        subj_it->second, pub_payload, std::nullopt);

                    if (s.failed()) {
                        log->warn("Failed to publish to '{}': {}", subj_it->second, s.error());
                    } else {
                        published_counter.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            },
            asio::detached
        );

        payload.clear();
    }

    m_log->debug("Worker {} stopped", worker_id);
}

} // namespace sidecar
