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

class fake_connection : public nats_asio::iconnection {
public:
    // --- configurable hooks (nullptr = default "unimplemented" behavior) ---
    std::function<asio::awaitable<nats_asio::status>(std::span<const char>)> on_write_raw;
    std::function<bool()> on_is_backpressure_active;
    std::function<asio::awaitable<nats_asio::status>(std::chrono::milliseconds)> on_wait_for_drain;
    std::function<asio::awaitable<std::pair<nats_asio::message, nats_asio::status>>(
        std::string_view, std::span<const char>, std::chrono::milliseconds)>
        on_request;

    // --- in-memory KV store backing kv_put/kv_get/kv_delete/kv_keys ---
    struct kv_record {
        std::vector<char> value;
        uint64_t revision = 0;
        std::chrono::system_clock::time_point created;
        nats_asio::kv_entry::operation op = nats_asio::kv_entry::operation::put;
    };
    std::map<std::string, kv_record> kv_store;  // key: "<bucket>/<key>"
    uint64_t next_revision = 1;

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
        std::string_view, std::span<const char>, nats_asio::optional<std::string_view>) override {
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
        const nats_asio::js_consumer_config&, nats_asio::on_js_message_cb) override {
        co_return std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    }
    asio::awaitable<std::pair<nats_asio::ijs_subscription_sptr, nats_asio::status>> js_subscribe(
        const nats_asio::js_consumer_config&, nats_asio::on_js_message_zero_copy_cb) override {
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
        std::string_view, nats_asio::on_message_cb, nats_asio::subscribe_options) override {
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
