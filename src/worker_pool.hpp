#pragma once

#include "config.hpp"
#include "event_bridge.hpp"
#include "subscription_manager.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/io_context.hpp>
#include <asio/awaitable.hpp>
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace sidecar {

class worker_pool {
public:
    struct stats {
        uint64_t processed = 0;
        uint64_t matched = 0;
        uint64_t published = 0;
        uint64_t match_failures = 0;
        uint64_t input_dropped = 0;
        uint64_t publish_tasks_dropped = 0;
        uint64_t publish_failures = 0;
        std::size_t queue_depth = 0;
        std::size_t queue_bytes = 0;
        std::size_t publish_inflight = 0;
    };

    worker_pool(asio::io_context& ioc, const config& cfg,
                const attribute_schema& schema,
                subscription_manager& sub_mgr,
                nats_asio::iconnection_sptr conn,
                std::shared_ptr<spdlog::logger> log);
    ~worker_pool();

    // Spawn N worker threads. Must be called once.
    void start();

    // Signal workers to stop, drain the queue, and join threads.
    void stop();

    // Enqueue a payload for worker processing (move semantics).
    // Returns false when shutdown has begun or a queue limit is reached.
    bool enqueue(std::vector<char> payload);

    // Wait for every accepted publication coroutine to complete.
    asio::awaitable<bool> wait_for_publications(std::chrono::milliseconds timeout);

    // Approximate queue depth.
    std::size_t queue_depth() const;

    // Atomically read aggregate stats from all workers.
    stats get_stats() const;

private:
    void worker_loop(unsigned int worker_id);

    asio::io_context& m_ioc;
    binary_format m_format;
    const attribute_schema& m_schema;
    subscription_manager& m_sub_mgr;
    nats_asio::iconnection_sptr m_conn;
    std::shared_ptr<spdlog::logger> m_log;

    unsigned int m_thread_count;
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_accepting{false};

    std::size_t m_queue_max_messages;
    std::size_t m_queue_max_bytes;
    std::size_t m_publish_max_inflight;
    std::chrono::milliseconds m_publish_backpressure_timeout;

    moodycamel::BlockingConcurrentQueue<std::vector<char>> m_queue;
    std::vector<std::thread> m_threads;
    std::mutex m_enqueue_mutex;
    std::atomic<std::size_t> m_queued_messages{0};
    std::atomic<std::size_t> m_queued_bytes{0};
    std::atomic<std::size_t> m_publish_inflight{0};

    // Aggregate stats (relaxed atomics)
    std::atomic<uint64_t> m_processed{0};
    std::atomic<uint64_t> m_matched{0};
    std::atomic<uint64_t> m_published{0};
    std::atomic<uint64_t> m_match_failures{0};
    std::atomic<uint64_t> m_input_dropped{0};
    std::atomic<uint64_t> m_publish_tasks_dropped{0};
    std::atomic<uint64_t> m_publish_failures{0};
};

} // namespace sidecar
