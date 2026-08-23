#pragma once

// Minimal, configurable fake of nats_asio::iconnection for unit tests that
// need a connection without a live NATS server. Everything not explicitly
// configured or backed by the in-memory KV store below returns a
// not_connected/unimplemented status - tests opt into the specific behavior
// they need via the public std::function hooks or the kv_store map.

#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace sidecar_test {

// Trivial isubscription for tests whose on_subscribe hook needs to return a
// real (non-null) subscription on success.
struct fake_subscription : public nats_asio::isubscription {
    uint64_t sid() noexcept override { return 1; }
    void cancel() noexcept override {}
    uint32_t max_messages() const noexcept override { return 0; }
    uint32_t message_count() const noexcept override { return 0; }
};

// Fake ijs_subscription returned by fake_connection::js_subscribe() below.
// Records every ack/nak/term call (by stream_sequence, in call order) so
// tests can assert exactly what a worker_pool resolution path did, mirroring
// kv_store's role for the KV hooks - a real, inspectable record rather than
// just a boolean "did it fail".
struct fake_js_subscription : public nats_asio::ijs_subscription {
    nats_asio::js_consumer_info consumer_info;
    bool active = true;

    std::vector<uint64_t> acked_stream_seqs;
    std::vector<uint64_t> nakked_stream_seqs;
    std::vector<uint64_t> termed_stream_seqs;

    // Predicates letting a test force a specific ack/term call to fail,
    // mirroring fail_kv_put/fail_kv_delete's pattern.
    std::function<bool(const nats_asio::js_message&)> fail_ack;
    std::function<bool(const nats_asio::js_message&)> fail_term;

    const nats_asio::js_consumer_info& info() const noexcept override { return consumer_info; }
    void stop() noexcept override { active = false; }
    bool is_active() const noexcept override { return active; }

    asio::awaitable<nats_asio::status> ack(const nats_asio::js_message& msg) override {
        if (fail_ack && fail_ack(msg)) {
            co_return nats_asio::status(nats_asio::error_code::operation_failed);
        }
        acked_stream_seqs.push_back(msg.stream_sequence);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> ack(const nats_asio::js_message_view&) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
    asio::awaitable<nats_asio::status> ack_batch(
        const std::vector<nats_asio::js_message>& messages) override {
        for (const auto& m : messages) acked_stream_seqs.push_back(m.stream_sequence);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> nak(
        const nats_asio::js_message& msg, std::chrono::milliseconds) override {
        nakked_stream_seqs.push_back(msg.stream_sequence);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> nak(
        const nats_asio::js_message_view&, std::chrono::milliseconds) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
    asio::awaitable<nats_asio::status> in_progress(const nats_asio::js_message&) override {
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> in_progress(const nats_asio::js_message_view&) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
    asio::awaitable<nats_asio::status> term(const nats_asio::js_message& msg) override {
        if (fail_term && fail_term(msg)) {
            co_return nats_asio::status(nats_asio::error_code::operation_failed);
        }
        termed_stream_seqs.push_back(msg.stream_sequence);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> term(const nats_asio::js_message_view&) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
};

class fake_connection : public nats_asio::iconnection {
public:
    // --- configurable hooks (nullptr = default "unimplemented" behavior) ---
    std::function<asio::awaitable<nats_asio::status>(std::span<const char>)> on_write_raw;
    std::function<bool()> on_is_backpressure_active;
    std::function<asio::awaitable<nats_asio::status>(std::chrono::milliseconds)> on_wait_for_drain;
    std::function<asio::awaitable<std::pair<nats_asio::message, nats_asio::status>>(
        std::string_view, std::span<const char>, std::chrono::milliseconds)>
        on_request;
    std::function<asio::awaitable<nats_asio::status>(std::string_view, std::span<const char>)>
        on_publish;
    // Backs the on_message_cb subscribe() overload only - the one
    // sidecar_engine's data-subject subscription loop actually uses.
    std::function<asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>>(
        std::string_view, nats_asio::on_message_cb, nats_asio::subscribe_options)>
        on_subscribe;
    // Always populated regardless of on_subscribe, in call order - lets
    // tests assert which subjects a subscribe loop actually attempted.
    std::vector<std::string> subscribed_subjects;

    // --- JetStream consumer support ---
    // Set to make js_subscribe() fail (default: succeeds, mirroring
    // on_subscribe's "opt into failure" pattern rather than
    // on_request's "opt into success" one, since the JetStream-consumer
    // path is what most sidecar_engine tests actually want by default).
    std::function<bool(const nats_asio::js_consumer_config&)> fail_js_subscribe;
    // The subscription js_subscribe() most recently returned, and the
    // callback it captured - tests drive simulated delivery by invoking
    // captured_js_cb directly against *last_js_subscription, the same way a
    // real js_subscribe_impl would invoke it on message arrival.
    std::shared_ptr<fake_js_subscription> last_js_subscription;
    nats_asio::on_js_message_cb captured_js_cb;
    std::vector<nats_asio::js_consumer_config> js_subscribe_configs;  // call order

    // --- in-memory KV store backing kv_put/kv_get/kv_delete/kv_keys ---
    struct kv_record {
        std::vector<char> value;
        uint64_t revision = 0;
        std::chrono::system_clock::time_point created;
        nats_asio::kv_entry::operation op = nats_asio::kv_entry::operation::put;
    };
    std::map<std::string, kv_record> kv_store;  // key: "<bucket>/<key>"
    uint64_t next_revision = 1;

    // Predicates letting a test force kv_put/kv_delete to fail for a
    // specific key without touching kv_store, e.g. to simulate a lease
    // persist/delete failure.
    std::function<bool(std::string_view bucket, std::string_view key)> fail_kv_put;
    std::function<bool(std::string_view bucket, std::string_view key)> fail_kv_delete;

    static std::string kv_key(std::string_view bucket, std::string_view key) {
        return std::string(bucket) + "/" + std::string(key);
    }

    // --- iconnection ---
    void start(const nats_asio::connect_config&) override {}
    void stop() noexcept override {}
    bool is_connected() noexcept override { return true; }
    const nats_asio::connection_stats& stats() const noexcept override { return m_stats; }

    asio::awaitable<nats_asio::status> drain(std::chrono::milliseconds) override {
        co_return nats_asio::status{};
    }
    bool is_draining() const noexcept override { return false; }
    const nats_asio::circuit_breaker_stats& circuit_breaker() const noexcept override {
        return m_circuit_breaker;
    }
    void reset_circuit_breaker() noexcept override {}

    asio::awaitable<nats_asio::status> publish(
        std::string_view subject, std::span<const char> payload,
        nats_asio::optional<std::string_view>) override {
        if (on_publish) co_return co_await on_publish(subject, payload);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> publish(
        std::string_view, std::span<const char>, const nats_asio::headers_t&,
        nats_asio::optional<std::string_view>) override {
        co_return nats_asio::status{};
    }

    asio::awaitable<nats_asio::status> write_raw(std::span<const char> data) override {
        if (on_write_raw) co_return co_await on_write_raw(data);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> write_raw_iov(
        std::span<const std::span<const char>>) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }

    nats_asio::status publish_queued(
        std::string_view, std::span<const char>, nats_asio::optional<std::string_view>) override {
        return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> flush() override { co_return nats_asio::status{}; }
    void set_write_coalescing(std::chrono::microseconds, size_t) override {}
    size_t pending_bytes() const noexcept override { return 0; }

    bool is_backpressure_active() const noexcept override {
        return on_is_backpressure_active ? on_is_backpressure_active() : false;
    }
    asio::awaitable<nats_asio::status> wait_for_drain(std::chrono::milliseconds timeout) override {
        if (on_wait_for_drain) co_return co_await on_wait_for_drain(timeout);
        co_return nats_asio::status{};
    }
    asio::awaitable<nats_asio::status> publish_with_backpressure(
        std::string_view, std::span<const char>, nats_asio::optional<std::string_view>,
        std::chrono::milliseconds) override {
        co_return nats_asio::status{};
    }

    asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> request(
        std::string_view subject, std::span<const char> payload,
        std::chrono::milliseconds timeout) override {
        if (on_request) co_return co_await on_request(subject, payload, timeout);
        co_return std::pair<nats_asio::message, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::not_connected)};
    }
    asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> request(
        std::string_view subject, std::span<const char> payload, const nats_asio::headers_t&,
        std::chrono::milliseconds timeout) override {
        if (on_request) co_return co_await on_request(subject, payload, timeout);
        co_return std::pair<nats_asio::message, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::not_connected)};
    }

    asio::awaitable<std::pair<nats_asio::js_pub_ack, nats_asio::status>> js_publish(
        std::string_view, std::span<const char>, std::chrono::milliseconds, bool) override {
        co_return std::pair<nats_asio::js_pub_ack, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::js_pub_ack, nats_asio::status>> js_publish(
        std::string_view, std::span<const char>, const nats_asio::headers_t&,
        std::chrono::milliseconds, bool) override {
        co_return std::pair<nats_asio::js_pub_ack, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<nats_asio::status> js_publish_async(
        std::string_view, std::span<const char>) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
    asio::awaitable<nats_asio::status> js_publish_async(
        std::string_view, std::span<const char>, const nats_asio::headers_t&) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }
    asio::awaitable<nats_asio::js_batch_result> js_publish_batch(
        const std::vector<nats_asio::js_batch_message>&, std::chrono::milliseconds) override {
        co_return nats_asio::js_batch_result{};
    }

    asio::awaitable<std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>> js_subscribe(
        const nats_asio::js_consumer_config& config, nats_asio::on_js_message_cb cb) override {
        js_subscribe_configs.push_back(config);
        if (fail_js_subscribe && fail_js_subscribe(config)) {
            co_return std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>{
                nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
        }
        auto sub = std::make_shared<fake_js_subscription>();
        sub->consumer_info.stream = config.stream;
        sub->consumer_info.name = config.durable_name.value_or("");
        sub->consumer_info.deliver_subject = config.deliver_subject.value_or("");
        last_js_subscription = sub;
        captured_js_cb = std::move(cb);
        co_return std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>{
            sub, nats_asio::status{}};
    }
    asio::awaitable<std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>> js_subscribe(
        const nats_asio::js_consumer_config&, nats_asio::on_js_message_zero_copy_cb) override {
        // sidecar_engine only ever uses the owned-js_message overload above
        // (zero-copy can't survive worker_pool's cross-thread async
        // processing - see js_message_view's own warning) - left
        // unimplemented here, matching this file's general "only back what
        // sidecar_engine actually calls" convention.
        co_return std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<std::vector<nats_asio::js_message>, nats_asio::status>> js_fetch(
        std::string_view, std::string_view, uint32_t, std::chrono::milliseconds) override {
        co_return std::pair<std::vector<nats_asio::js_message>, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::js_consumer_info, nats_asio::status>>
    js_get_consumer_info(std::string_view, std::string_view) override {
        co_return std::pair<nats_asio::js_consumer_info, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<nats_asio::status> js_delete_consumer(
        std::string_view, std::string_view) override {
        co_return nats_asio::status(nats_asio::error_code::operation_failed);
    }

    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_put(
        std::string_view bucket, std::string_view key, std::span<const char> value,
        std::chrono::milliseconds) override {
        if (fail_kv_put && fail_kv_put(bucket, key)) {
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::operation_failed)};
        }
        kv_record rec;
        rec.value.assign(value.begin(), value.end());
        rec.revision = next_revision++;
        rec.created = std::chrono::system_clock::now();
        rec.op = nats_asio::kv_entry::operation::put;
        auto revision = rec.revision;
        kv_store[kv_key(bucket, key)] = std::move(rec);
        co_return std::pair<uint64_t, nats_asio::status>{revision, nats_asio::status{}};
    }

    asio::awaitable<std::pair<nats_asio::kv_entry, nats_asio::status>> kv_get(
        std::string_view bucket, std::string_view key, std::chrono::milliseconds) override {
        auto it = kv_store.find(kv_key(bucket, key));
        if (it == kv_store.end() || it->second.op != nats_asio::kv_entry::operation::put) {
            co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
                {}, nats_asio::status(nats_asio::error_code::key_not_found)};
        }
        nats_asio::kv_entry entry;
        entry.bucket = std::string(bucket);
        entry.key = std::string(key);
        entry.value = it->second.value;
        entry.revision = it->second.revision;
        entry.created = it->second.created;
        entry.op = it->second.op;
        co_return std::pair<nats_asio::kv_entry, nats_asio::status>{entry, nats_asio::status{}};
    }

    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_delete(
        std::string_view bucket, std::string_view key, std::chrono::milliseconds) override {
        if (fail_kv_delete && fail_kv_delete(bucket, key)) {
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::operation_failed)};
        }
        auto it = kv_store.find(kv_key(bucket, key));
        if (it == kv_store.end()) {
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::key_not_found)};
        }
        it->second.op = nats_asio::kv_entry::operation::del;
        it->second.revision = next_revision++;
        auto revision = it->second.revision;
        co_return std::pair<uint64_t, nats_asio::status>{revision, nats_asio::status{}};
    }

    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_purge(
        std::string_view bucket, std::string_view key, std::chrono::milliseconds) override {
        co_return co_await kv_delete(bucket, key, std::chrono::milliseconds(0));
    }
    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_create(
        std::string_view bucket, std::string_view key, std::span<const char> value,
        std::chrono::milliseconds timeout) override {
        auto it = kv_store.find(kv_key(bucket, key));
        if (it != kv_store.end() && it->second.op == nats_asio::kv_entry::operation::put) {
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::already_exists)};
        }
        co_return co_await kv_put(bucket, key, value, timeout);
    }
    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_update(
        std::string_view bucket, std::string_view key, std::span<const char> value, uint64_t revision,
        std::chrono::milliseconds timeout) override {
        auto it = kv_store.find(kv_key(bucket, key));
        if (it == kv_store.end() || it->second.revision != revision) {
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::revision_mismatch)};
        }
        co_return co_await kv_put(bucket, key, value, timeout);
    }

    asio::awaitable<std::pair<std::vector<std::string>, nats_asio::status>> kv_keys(
        std::string_view bucket, std::chrono::milliseconds) override {
        std::vector<std::string> keys;
        const std::string prefix = std::string(bucket) + "/";
        for (const auto& [k, rec] : kv_store) {
            if (rec.op != nats_asio::kv_entry::operation::put) continue;
            if (k.rfind(prefix, 0) == 0) keys.push_back(k.substr(prefix.size()));
        }
        co_return std::pair<std::vector<std::string>, nats_asio::status>{
            std::move(keys), nats_asio::status{}};
    }

    // Matches key_pattern's dot-separated tokens against a key's own
    // dot-separated tokens: '*' matches exactly one token, '>' matches the
    // rest (must be the pattern's last token), anything else must match
    // literally - the same subject-wildcard semantics kv_keys' real
    // subscription-based implementation relies on.
    static bool matches_key_pattern(std::string_view key, std::string_view pattern) {
        auto split = [](std::string_view s) {
            std::vector<std::string_view> parts;
            std::size_t start = 0;
            while (start <= s.size()) {
                auto dot = s.find('.', start);
                if (dot == std::string_view::npos) {
                    parts.push_back(s.substr(start));
                    break;
                }
                parts.push_back(s.substr(start, dot - start));
                start = dot + 1;
            }
            return parts;
        };
        auto key_parts = split(key);
        auto pattern_parts = split(pattern);

        std::size_t i = 0;
        for (; i < pattern_parts.size(); ++i) {
            if (pattern_parts[i] == ">") return true;  // matches all remaining tokens
            if (i >= key_parts.size()) return false;
            if (pattern_parts[i] != "*" && pattern_parts[i] != key_parts[i]) return false;
        }
        return i == key_parts.size();
    }

    asio::awaitable<std::pair<std::vector<std::string>, nats_asio::status>> kv_keys(
        std::string_view bucket, std::string_view key_pattern, std::chrono::milliseconds) override {
        std::vector<std::string> keys;
        const std::string prefix = std::string(bucket) + "/";
        for (const auto& [k, rec] : kv_store) {
            if (rec.op != nats_asio::kv_entry::operation::put) continue;
            if (k.rfind(prefix, 0) != 0) continue;
            auto key_only = k.substr(prefix.size());
            if (matches_key_pattern(key_only, key_pattern)) keys.push_back(std::move(key_only));
        }
        co_return std::pair<std::vector<std::string>, nats_asio::status>{
            std::move(keys), nats_asio::status{}};
    }

    asio::awaitable<std::pair<std::vector<nats_asio::kv_entry>, nats_asio::status>> kv_history(
        std::string_view, std::string_view, std::chrono::milliseconds) override {
        co_return std::pair<std::vector<nats_asio::kv_entry>, nats_asio::status>{
            {}, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<uint64_t, nats_asio::status>> kv_revert(
        std::string_view, std::string_view, uint64_t, std::chrono::milliseconds) override {
        co_return std::pair<uint64_t, nats_asio::status>{
            0, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::ikv_watcher_sptr, nats_asio::status>> kv_watch(
        std::string_view, nats_asio::on_kv_entry_cb, std::string_view) override {
        co_return std::pair<nats_asio::ikv_watcher_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }

    asio::awaitable<nats_asio::status> unsubscribe(const nats_asio::isubscription_sptr&) override {
        co_return nats_asio::status{};
    }
    asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> subscribe(
        std::string_view subject, nats_asio::on_message_cb cb,
        nats_asio::subscribe_options opts) override {
        subscribed_subjects.emplace_back(subject);
        if (on_subscribe) co_return co_await on_subscribe(subject, std::move(cb), opts);
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> subscribe(
        std::string_view, nats_asio::on_message_with_headers_cb,
        nats_asio::subscribe_options) override {
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> subscribe(
        std::string_view, nats_asio::on_message_zero_copy_cb, nats_asio::subscribe_options) override {
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }

private:
    nats_asio::connection_stats m_stats;
    nats_asio::circuit_breaker_stats m_circuit_breaker;
};

} // namespace sidecar_test
