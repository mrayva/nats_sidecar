#include "worker_pool.hpp"
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/steady_timer.hpp>
#include <asio/use_awaitable.hpp>
#include <nlohmann/json.hpp>
#include <chrono>

namespace sidecar {

namespace {
void append_pub_frame(std::string& wire, const std::string& subject,
                      std::span<const char> payload) {
    wire += "PUB ";
    wire += subject;
    wire += " ";
    wire += std::to_string(payload.size());
    wire += "\r\n";
    wire.append(payload.data(), payload.size());
    wire += "\r\n";
}
} // namespace

pub_frames build_pub_frames(const std::vector<uint64_t>& matched_ids,
                            const std::unordered_map<uint64_t, std::string>& output_subjects,
                            std::span<const char> payload) {
    pub_frames frames;
    // Rough upper-bound estimate (subject/size overhead is small relative to
    // the payload for most workloads) to avoid repeated reallocation while
    // appending one frame per matched subscription.
    frames.wire.reserve(matched_ids.size() * (payload.size() + 64));

    for (uint64_t sub_id : matched_ids) {
        auto subj_it = output_subjects.find(sub_id);
        if (subj_it == output_subjects.end()) continue;
        append_pub_frame(frames.wire, subj_it->second, payload);
        ++frames.count;
    }
    return frames;
}

std::string build_stats_json(uint64_t received, const worker_pool::stats& ws,
                              std::size_t subscriptions, double avg_match_us,
                              double avg_fanout_us) {
    nlohmann::json j = {
        {"received", received},
        {"processed", ws.processed},
        {"matched", ws.matched},
        {"published", ws.published},
        {"match_failures", ws.match_failures},
        {"publish_failures", ws.publish_failures},
        {"input_dropped", ws.input_dropped},
        {"publish_tasks_dropped", ws.publish_tasks_dropped},
        {"subscriptions", subscriptions},
        {"queue_depth", ws.queue_depth},
        {"queue_bytes", ws.queue_bytes},
        {"publish_inflight", ws.publish_inflight},
        {"avg_match_us", avg_match_us},
        {"avg_fanout_us", avg_fanout_us}
    };
    return j.dump();
}

worker_pool::worker_pool(asio::io_context& ioc, const config& cfg,
                         const attribute_schema& schema,
                         subscription_manager& sub_mgr,
                         nats_asio::iconnection_sptr conn,
                         std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_format(cfg.format), m_output_format(cfg.output_format), m_schema(schema),
      m_sub_mgr(sub_mgr), m_conn(std::move(conn)), m_log(std::move(log)),
      m_thread_count(cfg.worker_threads > 0 ? cfg.worker_threads
                                            : std::thread::hardware_concurrency()),
      m_queue_max_messages(cfg.input_queue_max_messages),
      m_queue_max_bytes(cfg.input_queue_max_bytes),
      m_publish_max_inflight(cfg.publish_max_inflight),
      m_publish_backpressure_timeout(cfg.publish_backpressure_timeout_ms),
      m_publish_chunk_bytes(cfg.publish_chunk_bytes),
      m_publish_max_inflight_bytes(cfg.publish_max_inflight_bytes)
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

bool worker_pool::enqueue(std::vector<char> payload, bool columnar) {
    return enqueue_impl(queued_message{std::move(payload), std::nullopt, nullptr, columnar});
}

bool worker_pool::enqueue(std::vector<char> payload, nats_asio::js_message js_msg,
                          nats_asio::ijs_subscription_sptr js_sub, bool columnar) {
    return enqueue_impl(queued_message{
        std::move(payload), std::move(js_msg), std::move(js_sub), columnar});
}

bool worker_pool::enqueue_impl(queued_message qm) {
    std::lock_guard<std::mutex> lock(m_enqueue_mutex);
    // Backpressure here (queue full/shutting down) intentionally leaves any
    // JetStream message unacked rather than acking or terming it - ack_wait
    // will trigger real redelivery once there's room, matching the "leave
    // unacked on transient failure" rule the rest of worker_loop follows.
    if (!m_accepting.load(std::memory_order_acquire) ||
        m_queued_messages.load(std::memory_order_relaxed) >= m_queue_max_messages ||
        qm.payload.size() > m_queue_max_bytes - std::min(
            m_queue_max_bytes, m_queued_bytes.load(std::memory_order_relaxed))) {
        m_input_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    const auto bytes = qm.payload.size();
    m_queued_messages.fetch_add(1, std::memory_order_relaxed);
    m_queued_bytes.fetch_add(bytes, std::memory_order_relaxed);
    if (!m_queue.enqueue(std::move(qm))) {
        m_queued_messages.fetch_sub(1, std::memory_order_relaxed);
        m_queued_bytes.fetch_sub(bytes, std::memory_order_relaxed);
        m_input_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void worker_pool::spawn_ack(nats_asio::ijs_subscription_sptr js_sub, nats_asio::js_message msg) {
    if (!js_sub) return;
    auto log = m_log;
    asio::co_spawn(m_ioc,
        [js_sub = std::move(js_sub), msg = std::move(msg), log]() mutable -> asio::awaitable<void> {
            auto s = co_await js_sub->ack(msg);
            if (s.failed()) {
                log->warn("Failed to ack JetStream input message (stream_seq={}): {}",
                          msg.stream_sequence, s.error());
            }
        },
        asio::detached);
}

void worker_pool::spawn_term(nats_asio::ijs_subscription_sptr js_sub, nats_asio::js_message msg) {
    if (!js_sub) return;
    auto log = m_log;
    asio::co_spawn(m_ioc,
        [js_sub = std::move(js_sub), msg = std::move(msg), log]() mutable -> asio::awaitable<void> {
            auto s = co_await js_sub->term(msg);
            if (s.failed()) {
                log->warn("Failed to term JetStream input message (stream_seq={}): {}",
                          msg.stream_sequence, s.error());
            }
        },
        asio::detached);
}

std::size_t worker_pool::queue_depth() const {
    return m_queued_messages.load(std::memory_order_relaxed);
}

worker_pool::stats worker_pool::get_stats() const {
    return {
        m_processed.load(std::memory_order_relaxed),
        m_matched.load(std::memory_order_relaxed),
        m_publish_counters->published.load(std::memory_order_relaxed),
        m_match_failures.load(std::memory_order_relaxed),
        m_input_dropped.load(std::memory_order_relaxed),
        m_publish_tasks_dropped.load(std::memory_order_relaxed),
        m_publish_counters->publish_failures.load(std::memory_order_relaxed),
        m_queued_messages.load(std::memory_order_relaxed),
        m_queued_bytes.load(std::memory_order_relaxed),
        m_publish_counters->publish_inflight.load(std::memory_order_relaxed),
        m_publish_counters->publish_inflight_bytes.load(std::memory_order_relaxed),
        m_match_time_ns_total.load(std::memory_order_relaxed),
        m_match_time_count.load(std::memory_order_relaxed),
        m_fanout_time_ns_total.load(std::memory_order_relaxed),
        m_fanout_time_count.load(std::memory_order_relaxed)
    };
}

asio::awaitable<bool> worker_pool::wait_for_publications(
    std::chrono::milliseconds timeout) {
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (m_publish_counters->publish_inflight.load(std::memory_order_acquire) != 0) {
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

    queued_message qm;
    while (m_running.load(std::memory_order_acquire) ||
           m_queued_messages.load(std::memory_order_acquire) != 0) {
        // Block with timeout to allow checking m_running for graceful shutdown
        bool got = m_queue.wait_dequeue_timed(
            qm, std::chrono::milliseconds(100));

        if (!got) continue;

        m_queued_messages.fetch_sub(1, std::memory_order_relaxed);
        m_queued_bytes.fetch_sub(qm.payload.size(), std::memory_order_relaxed);

        std::span<const char> payload_span(qm.payload.data(), qm.payload.size());

        // row_matches unifies both paths: a non-columnar message always
        // yields 0 or 1 entries (its own payload, verbatim - the exact
        // behavior this replaces); a columnar message yields 0..N entries,
        // one per row that matched something. Everything from here on
        // (stats, malformed/empty/matched branches, the publish coroutine)
        // is one shared code path for both, differing only in how many
        // entries row_matches holds.
        std::optional<std::chrono::nanoseconds> search_time;
        std::size_t rows_searched = 1;
        std::optional<std::vector<row_match>> row_matches;

        {
            // Synchronous, lock-protected read access to the current live tree. tree_guard's
            // shared_lock must not survive past this block - never held across the
            // asio::co_spawn'd publish coroutine below (which can suspend on backpressure/drain) -
            // see subscription_manager::tree_read_guard's own doc comment for why.
            auto tree_guard = m_sub_mgr.acquire_tree();
            if (!tree_guard) {
                // No active subscriptions to check against - legitimately
                // nothing to do, same as the "no match" case below. Acked (not
                // left pending): a subscribe request only ever looks at
                // messages from that point forward, so there's no future
                // consumer who'd want this message redelivered.
                if (qm.js_msg) spawn_ack(qm.js_sub, std::move(*qm.js_msg));
                qm.payload.clear();
                continue;
            }

            if (qm.columnar) {
                row_matches = deserialize_and_match_columnar(
                    *tree_guard, m_schema, m_format, payload_span, m_log,
                    &search_time, &rows_searched, m_output_format);
            } else {
                auto matches = deserialize_and_match(
                    *tree_guard, m_schema, m_format, payload_span, m_log, &search_time);
                if (matches) {
                    row_matches.emplace();
                    if (!matches->empty()) {
                        row_matches->push_back({std::move(*matches), std::move(qm.payload)});
                    }
                }
            }
        }

        if (search_time) {
            m_match_time_ns_total.fetch_add(
                static_cast<uint64_t>(search_time->count()), std::memory_order_relaxed);
            m_match_time_count.fetch_add(rows_searched, std::memory_order_relaxed);
        }

        m_processed.fetch_add(1, std::memory_order_relaxed);

        if (!row_matches) {
            // Malformed payload (or, for a columnar batch, any row's match
            // failing outright) - a poison message that will never succeed
            // no matter how many times it's redelivered. term(), not ack:
            // this is a real, visible "permanently gave up" signal (shows
            // up in js_get_consumer_info's stats) rather than a silent drop.
            m_match_failures.fetch_add(1, std::memory_order_relaxed);
            if (qm.js_msg) spawn_term(qm.js_sub, std::move(*qm.js_msg));
            qm.payload.clear();
            continue;
        }

        if (row_matches->empty()) {
            // Legitimately processed, nothing matched (in any row) - ack
            // now. Otherwise every non-matching message (most real traffic,
            // for any given filter) would sit unacked until ack_wait and
            // redeliver forever for no reason.
            if (qm.js_msg) spawn_ack(qm.js_sub, std::move(*qm.js_msg));
            qm.payload.clear();
            continue;
        }

        m_matched.fetch_add(row_matches->size(), std::memory_order_relaxed);

        // Start of the timed fan-out-resolution window (see stats::fanout_time_ns_total's own
        // comment for exactly what this does and deliberately does not cover) - only recorded if
        // this row_matches actually reaches asio::co_spawn() below, not the backpressure-drop
        // path (see that branch's own `continue` - a dropped message never really got fanned out).
        auto fanout_start = std::chrono::steady_clock::now();

        // Upfront estimate of this message's total publish wire size (same
        // per-frame estimate build_pub_frames() itself uses), reserved
        // against publish_inflight_bytes BEFORE any buffer is actually
        // built. publish_max_inflight alone only bounds the number of
        // concurrent publish tasks - each one could still independently
        // grow toward publish_chunk_bytes before ever checking connection
        // backpressure, so up to publish_max_inflight * publish_chunk_bytes
        // could accumulate with no aggregate cap (a real near-miss found
        // 2026-08-29, right after the chunking fix: RSS climbed toward
        // multiple GB/instance under sustained high fan-out). Reserving the
        // estimate here, before the task is even spawned, bounds aggregate
        // outstanding publish memory the same way input_queue_max_bytes
        // already bounds aggregate input memory.
        std::size_t estimated_bytes = 0;
        for (const auto& rm : *row_matches) {
            estimated_bytes += rm.matched_ids.size() * (rm.payload.size() + 64);
        }

        const auto previous_inflight = m_publish_counters->publish_inflight.fetch_add(
            1, std::memory_order_acq_rel);
        const auto previous_inflight_bytes = m_publish_counters->publish_inflight_bytes.fetch_add(
            estimated_bytes, std::memory_order_acq_rel);
        if (previous_inflight >= m_publish_max_inflight ||
            previous_inflight_bytes + estimated_bytes > m_publish_max_inflight_bytes) {
            m_publish_counters->publish_inflight.fetch_sub(1, std::memory_order_acq_rel);
            m_publish_counters->publish_inflight_bytes.fetch_sub(
                estimated_bytes, std::memory_order_acq_rel);
            m_publish_tasks_dropped.fetch_add(1, std::memory_order_relaxed);
            // Backpressure at the inflight-publish-task/byte limit: leave
            // any js_msg unacked (do not ack, do not term) so ack_wait
            // triggers real redelivery once there's room - this is the
            // actual mechanism that closes the loss gap plain queue-group
            // mode had no equivalent of.
            qm.payload.clear();
            continue;
        }

        // Capture what we need for the publish coroutine. counters is a
        // shared_ptr copy (not a reference to this worker_pool) so the
        // coroutine - which asio::co_spawn detaches onto m_ioc and which may
        // still be suspended after this worker_pool is destroyed - keeps its
        // target alive for as long as it needs it. js_sub is captured the
        // same way, for the same reason, so the ack after a successful
        // write can happen from inside this same coroutine - acking on the
        // I/O thread only after the write that made the message durable
        // downstream actually succeeded is what keeps ack ordered strictly
        // after publish, not a separate, potentially-racing step.
        // Resolve output subjects synchronously here, on the worker thread - not inside the async
        // publish coroutine, which is the whole point of subscription_manager::output_subject()
        // existing as a separate, brief-shared-lock call rather than part of an object handed
        // across the coroutine's suspension points (see tree_guard's own comment above). Scoped
        // to just the ids this message actually matched (typically small), not every active
        // subscription - unlike the old tree_snapshot::output_subjects map, which was rebuilt in
        // full on every single subscribe/unsubscribe.
        std::unordered_map<uint64_t, std::string> output_subjects;
        for (const auto& rm : *row_matches) {
            for (uint64_t sub_id : rm.matched_ids) {
                if (output_subjects.count(sub_id)) continue;
                if (auto subj = m_sub_mgr.output_subject(sub_id)) {
                    output_subjects.emplace(sub_id, std::move(*subj));
                }
            }
        }

        auto fanout_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - fanout_start);
        m_fanout_time_ns_total.fetch_add(
            static_cast<uint64_t>(fanout_ns.count()), std::memory_order_relaxed);
        m_fanout_time_count.fetch_add(row_matches->size(), std::memory_order_relaxed);

        auto matches_to_publish = std::move(*row_matches);
        auto conn = m_conn;
        auto log = m_log;
        auto counters = m_publish_counters;
        auto backpressure_timeout = m_publish_backpressure_timeout;
        auto chunk_bytes = m_publish_chunk_bytes;
        auto reserved_bytes = estimated_bytes;
        auto js_sub = qm.js_sub;
        auto js_msg = std::move(qm.js_msg);

        // Post publish work to the ASIO I/O thread
        asio::co_spawn(m_ioc,
            [matches_to_publish = std::move(matches_to_publish),
             output_subjects = std::move(output_subjects),
             conn = std::move(conn),
             log,
             counters,
             backpressure_timeout,
             chunk_bytes,
             reserved_bytes,
             js_sub = std::move(js_sub),
             js_msg = std::move(js_msg)]() mutable -> asio::awaitable<void> {
                bool publish_ok = false;
                try {
                    // Flush in bounded-size chunks (config: publish_chunk_bytes,
                    // default 4MB) rather than building one combined wire
                    // buffer for every row's frames before a single
                    // write_raw() - a real bug found 2026-08-29: each matched
                    // subscription gets its own full copy of the row's
                    // payload, so a batch (columnar: up to hundreds of rows)
                    // whose rows collectively match thousands of
                    // subscriptions (e.g. a wide-range predicate a large
                    // fraction of subscriptions share) built an unbounded
                    // buffer - observed at 3GB+ RSS per instance, independent
                    // of matching engine, under exactly that workload shape.
                    // Chunking bounds peak memory to O(chunk_bytes) regardless
                    // of match fan-out while writing everything (no data
                    // loss) and preserving the existing "ack only after
                    // every write for this message succeeded" ordering -
                    // just via several co_await write_raw() calls instead of
                    // one when a message's fan-out is large.
                    // Reserved upfront to chunk_bytes - found via `perf` profiling the
                    // io_context thread specifically under real sustained load (2026-08-30):
                    // over half its own CPU time was std::string internals
                    // (_M_append/__memmove/_M_check_length/_S_copy, not socket I/O at all),
                    // because `wire` grew purely via repeated reallocate-and-copy on every
                    // append_pub_frame() call, chunk after chunk, for every single publish
                    // task. std::string::clear() retains capacity, so this reserve pays off
                    // for every chunk within one task, not just the first.
                    std::string wire;
                    wire.reserve(chunk_bytes);
                    std::size_t chunk_count = 0;
                    std::size_t total_published = 0;
                    bool write_failed = false;

                    auto flush_chunk = [&]() -> asio::awaitable<bool> {
                        if (wire.empty()) co_return true;
                        if (conn->is_backpressure_active()) {
                            auto drain_status = co_await conn->wait_for_drain(
                                backpressure_timeout);
                            if (drain_status.failed()) {
                                counters->publish_failures.fetch_add(1, std::memory_order_relaxed);
                                log->warn("Output backpressure wait failed: {}",
                                          drain_status.error());
                                co_return false;
                            }
                        }
                        auto write_status = co_await conn->write_raw(
                            std::span<const char>(wire.data(), wire.size()));
                        if (write_status.failed()) {
                            counters->publish_failures.fetch_add(1, std::memory_order_relaxed);
                            log->warn("Failed to write matched publications: {}",
                                      write_status.error());
                            co_return false;
                        }
                        total_published += chunk_count;
                        wire.clear();
                        chunk_count = 0;
                        co_return true;
                    };

                    for (const auto& rm : matches_to_publish) {
                        std::span<const char> payload(rm.payload.data(), rm.payload.size());
                        for (uint64_t sub_id : rm.matched_ids) {
                            auto subj_it = output_subjects.find(sub_id);
                            if (subj_it == output_subjects.end()) continue;
                            append_pub_frame(wire, subj_it->second, payload);
                            ++chunk_count;
                            if (wire.size() >= chunk_bytes) {
                                if (!co_await flush_chunk()) { write_failed = true; break; }
                            }
                        }
                        if (write_failed) break;
                    }
                    if (!write_failed) {
                        write_failed = !co_await flush_chunk();
                    }
                    if (!write_failed) {
                        counters->published.fetch_add(total_published, std::memory_order_relaxed);
                        publish_ok = true;
                    }
                } catch (const std::exception& e) {
                    counters->publish_failures.fetch_add(1, std::memory_order_relaxed);
                    log->error("Publication task failed: {}", e.what());
                }
                counters->publish_inflight.fetch_sub(1, std::memory_order_acq_rel);
                counters->publish_inflight_bytes.fetch_sub(reserved_bytes, std::memory_order_acq_rel);

                // Ack only after a successful write (or a no-op that wasn't
                // a failure) - a write failure leaves the message unacked,
                // exactly like the backpressure-drop case above, so
                // ack_wait triggers real redelivery instead of silent loss.
                if (js_sub && js_msg && publish_ok) {
                    auto ack_status = co_await js_sub->ack(*js_msg);
                    if (ack_status.failed()) {
                        log->warn("Failed to ack JetStream input message "
                                  "(stream_seq={}): {}",
                                  js_msg->stream_sequence, ack_status.error());
                    }
                }
            },
            asio::detached
        );

        qm.payload.clear();
    }

    m_log->debug("Worker {} stopped", worker_id);
}

} // namespace sidecar
