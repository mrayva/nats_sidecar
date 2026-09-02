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
#include <optional>
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
        std::size_t publish_inflight_bytes = 0;
        // Wall-clock time spent inside matching_engine::search() alone (not
        // deserialize/populate), summed across every message that actually
        // reached search. match_time_count is the number of such messages -
        // divide to get an average; both are 0 if none have run yet.
        uint64_t match_time_ns_total = 0;
        uint64_t match_time_count = 0;
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

    // Enqueue a payload for worker processing (move semantics). Plain
    // queue-group mode: no ack concept, matching today's at-most-once
    // behavior exactly. `columnar` marks this payload as a pg_zerialize-style
    // columnar batch (see event_bridge.hpp's deserialize_and_match_columnar) -
    // set per-message from the originating connection's own `columnar` flag,
    // since one process can mix columnar and non-columnar connections.
    // Returns false when shutdown has begun or a queue limit is reached.
    bool enqueue(std::vector<char> payload, bool columnar = false);

    // JetStream-consumer-mode overload: the payload's originating js_message
    // and the specific connection's own js_sub travel with it so the worker
    // can resolve delivery (ack/nak/term) against the *right* consumer
    // after processing completes, not just at enqueue time - see
    // worker_loop()'s four resolution cases. A process can have any number
    // of js-mode connections live at once, each with its own js_sub, so
    // this is per-message rather than a single pool-wide handle.
    bool enqueue(std::vector<char> payload, nats_asio::js_message js_msg,
                 nats_asio::ijs_subscription_sptr js_sub, bool columnar = false);

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
        // Sum of each in-flight task's own upfront estimated wire size (see
        // estimate_pub_bytes() in worker_pool.cpp) - reserved before a task
        // is spawned, released when it completes, bounding aggregate
        // publish memory regardless of how many tasks are concurrently
        // in flight (publish_max_inflight alone only bounds their count).
        std::atomic<std::size_t> publish_inflight_bytes{0};
    };

    void worker_loop(unsigned int worker_id);

    // The element actually carried through m_queue. js_msg/js_sub are both
    // nullopt/null for plain queue-group-mode payloads (no ack concept,
    // matches today's behavior); both populated for JetStream-consumer-mode
    // payloads, whose delivery must be resolved (ack/nak/term) - against
    // this specific js_sub, not any other live connection's - once
    // processing finishes.
    struct queued_message {
        std::vector<char> payload;
        std::optional<nats_asio::js_message> js_msg;
        nats_asio::ijs_subscription_sptr js_sub;
        bool columnar = false;
    };
    bool enqueue_impl(queued_message qm);

    // Posts an ack()/term() call for a JetStream message onto m_ioc,
    // detached - same pattern as the existing publish coroutine below,
    // since ack()/term() are themselves coroutines that need m_conn's I/O
    // thread, but worker_loop runs on a plain std::thread. Captures m_log
    // and the given js_sub by value (not `this`), so the posted coroutine
    // stays valid even if this worker_pool is destroyed while it's still
    // suspended - the exact same lifetime concern publish_counters already
    // solves for the publish path.
    void spawn_ack(nats_asio::ijs_subscription_sptr js_sub, nats_asio::js_message msg);
    void spawn_term(nats_asio::ijs_subscription_sptr js_sub, nats_asio::js_message msg);

    asio::io_context& m_ioc;
    binary_format m_format;
    // Set once at construction from cfg.output_format (see that field's own comment) - unset
    // means "same as m_format", passed straight through to deserialize_and_match_columnar's own
    // identically-defaulted parameter. Row-mode ignores this entirely (see enqueue()/worker_loop
    // below - only the columnar branch reads it), since row-mode has no re-encode step to
    // decouple.
    std::optional<binary_format> m_output_format;
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
    std::size_t m_publish_chunk_bytes;
    std::size_t m_publish_max_inflight_bytes;

    moodycamel::BlockingConcurrentQueue<queued_message> m_queue;
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
    std::atomic<uint64_t> m_match_time_ns_total{0};
    std::atomic<uint64_t> m_match_time_count{0};
};

// Machine-readable mirror of sidecar_engine::stats_loop()'s own "stats: ..." text log line, same
// field names, same values - exposed as a free function (like build_pub_frames above) so its
// exact output is directly testable without capturing spdlog output. Only emitted when
// config::stats_format is "json" or "both" (see stats_loop()), under a distinct "stats_json:"
// log prefix so it can never collide with a "stats:"-matching grep.
std::string build_stats_json(uint64_t received, const worker_pool::stats& ws,
                              std::size_t subscriptions, double avg_match_us);

} // namespace sidecar
