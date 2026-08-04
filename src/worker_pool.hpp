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
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace sidecar {

// One NATS PUB frame per id in `matched_ids` that has an entry in
// `output_subjects` (ids missing from the map - e.g. a subscription removed
// between search and publish - are silently skipped), all sharing the same
// payload. Exposed as a free function (rather than kept inline in the
// publish coroutine) so its exact wire output is directly testable.
struct pub_frames {
    std::string wire;
    std::size_t count = 0;
};
pub_frames build_pub_frames(const std::vector<uint64_t>& matched_ids,
                            const std::unordered_map<uint64_t, std::string>& output_subjects,
                            std::span<const char> payload);

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
    // Published/publish_failures/publish_inflight are touched from inside the
    // publish coroutine posted onto m_ioc via asio::co_spawn(..., detached).
    // That coroutine can outlive any particular worker_pool instance (e.g. if
    // it's still suspended when the pool is destroyed), so it captures a
    // shared_ptr to this block by value rather than references into the pool
    // itself - a dangling reference there would be a use-after-free the type
    // system can't catch.
    struct publish_counters {
        std::atomic<uint64_t> published{0};
        std::atomic<uint64_t> publish_failures{0};
        std::atomic<std::size_t> publish_inflight{0};
    };

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
    std::shared_ptr<publish_counters> m_publish_counters = std::make_shared<publish_counters>();

    // Aggregate stats (relaxed atomics)
    std::atomic<uint64_t> m_processed{0};
    std::atomic<uint64_t> m_matched{0};
    std::atomic<uint64_t> m_match_failures{0};
    std::atomic<uint64_t> m_input_dropped{0};
    std::atomic<uint64_t> m_publish_tasks_dropped{0};
};

} // namespace sidecar
