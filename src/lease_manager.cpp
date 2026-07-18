#include "lease_manager.hpp"
#include <charconv>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/use_awaitable.hpp>
#include <nlohmann/json.hpp>
#include <atomic>
#include <openssl/evp.h>

namespace sidecar {

lease_manager::lease_manager(asio::io_context& ioc,
                             nats_asio::iconnection_sptr conn,
                             subscription_manager& sub_mgr,
                             const std::string& bucket,
                             uint32_t ttl_seconds,
                             uint32_t check_interval_seconds,
                             std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_conn(std::move(conn)), m_sub_mgr(sub_mgr),
      m_bucket(bucket), m_ttl_seconds(ttl_seconds),
      m_check_interval_seconds(check_interval_seconds),
      m_log(std::move(log))
{}

lease_manager::~lease_manager() { stop(); }

namespace {

std::vector<char> decode_base64(const std::string& encoded) {
    if (encoded.empty()) return {};
    std::vector<unsigned char> decoded(3 * ((encoded.size() + 3) / 4));
    const int size = EVP_DecodeBlock(
        decoded.data(), reinterpret_cast<const unsigned char*>(encoded.data()),
        static_cast<int>(encoded.size()));
    if (size < 0) throw std::runtime_error("invalid base64 data");
    std::size_t actual = static_cast<std::size_t>(size);
    if (!encoded.empty() && encoded.back() == '=') --actual;
    if (encoded.size() > 1 && encoded[encoded.size() - 2] == '=') --actual;
    return std::vector<char>(decoded.begin(), decoded.begin() + actual);
}

} // namespace

asio::awaitable<std::pair<nats_asio::message, nats_asio::status>>
lease_manager::request_plain(const std::string& subject,
                             const std::string& payload,
                             std::chrono::milliseconds timeout) {
    const std::string inbox = m_request_prefix +
        std::to_string(++m_next_request_id);
    auto state = std::make_shared<request_state>();
    m_requests.emplace(inbox, state);

    std::string wire;
    wire.reserve(subject.size() + inbox.size() + payload.size() + 40);
    wire += "PUB ";
    wire += subject;
    wire += " ";
    wire += inbox;
    wire += " ";
    wire += std::to_string(payload.size());
    wire += "\r\n";
    wire += payload;
    wire += "\r\n";
    auto write_status = co_await m_conn->write_raw(
        std::span<const char>(wire.data(), wire.size()));
    if (write_status.failed()) {
        m_requests.erase(inbox);
        co_return std::pair<nats_asio::message, nats_asio::status>{
            {}, write_status};
    }

    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!state->received) {
        if (std::chrono::steady_clock::now() >= deadline) {
            m_requests.erase(inbox);
            co_return std::pair<nats_asio::message, nats_asio::status>{
                {}, nats_asio::status(nats_asio::error_code::request_timeout)};
        }
        timer.expires_after(std::chrono::milliseconds(5));
        co_await timer.async_wait(asio::use_awaitable);
    }
    m_requests.erase(inbox);
    co_return std::pair<nats_asio::message, nats_asio::status>{
        std::move(state->response), nats_asio::status{}};
}

asio::awaitable<bool> lease_manager::start_request_mux() {
    static std::atomic<uint64_t> next_mux{0};
    if (m_request_subscription) m_request_subscription->cancel();
    m_requests.clear();
    m_request_prefix = "_INBOX.nats_sidecar.kv." +
        std::to_string(next_mux.fetch_add(1, std::memory_order_relaxed) + 1) + ".";
    auto [subscription, status] = co_await m_conn->subscribe(
        m_request_prefix + ">",
        [this](std::string_view subject,
               std::optional<std::string_view> reply_to,
               std::span<const char> payload) -> asio::awaitable<void> {
            auto it = m_requests.find(std::string(subject));
            if (it != m_requests.end() && !it->second->received) {
                it->second->response.subject = std::string(subject);
                if (reply_to) it->second->response.reply_to = std::string(*reply_to);
                it->second->response.payload.assign(payload.begin(), payload.end());
                it->second->received = true;
            }
            co_return;
        });
    if (status.failed()) {
        m_log->error("lease_manager: failed to create request inbox: {}", status.error());
        co_return false;
    }
    m_request_subscription = std::move(subscription);
    auto flush_status = co_await m_conn->flush();
    if (flush_status.failed()) {
        m_log->error("lease_manager: failed to activate request inbox: {}",
                     flush_status.error());
        co_return false;
    }
    co_return true;
}

asio::awaitable<std::pair<nats_asio::kv_entry, nats_asio::status>>
lease_manager::get_lease(const std::string& key) {
    const std::string kv_subject = "$KV." + m_bucket + "." + key;
    const std::string request_payload =
        nlohmann::json({{"last_by_subj", kv_subject}}).dump();
    auto [response, status] = co_await request_plain(
        "$JS.API.STREAM.MSG.GET.KV_" + m_bucket,
        request_payload,
        std::chrono::seconds(5));
    if (status.failed()) {
        co_return std::pair<nats_asio::kv_entry, nats_asio::status>{{}, status};
    }

    try {
        auto body = nlohmann::json::parse(
            std::string_view(response.payload.data(), response.payload.size()));
        if (body.contains("error")) {
            const auto code = body["error"].value("code", 0);
            if (code == 404) {
                co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
                    {}, nats_asio::status(nats_asio::error_code::key_not_found)};
            }
            co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
                {}, nats_asio::status(body["error"].value(
                    "description", "message get failed"))};
        }

        const auto& message = body.at("message");
        if (message.contains("hdrs")) {
            const auto headers = decode_base64(message.at("hdrs").get<std::string>());
            const std::string_view header_view(headers.data(), headers.size());
            if (header_view.find("KV-Operation: DEL") != std::string_view::npos ||
                header_view.find("KV-Operation: PURGE") != std::string_view::npos) {
                co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
                    {}, nats_asio::status(nats_asio::error_code::key_not_found)};
            }
        }

        nats_asio::kv_entry entry;
        entry.bucket = m_bucket;
        entry.key = key;
        entry.revision = message.value("seq", uint64_t{0});
        entry.value = decode_base64(message.value("data", std::string{}));
        co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
            std::move(entry), nats_asio::status{}};
    } catch (const std::exception& e) {
        co_return std::pair<nats_asio::kv_entry, nats_asio::status>{
            {}, nats_asio::status(std::string("invalid message get response: ") + e.what())};
    }
}

asio::awaitable<std::pair<std::vector<std::string>, nats_asio::status>>
lease_manager::list_lease_keys() {
    const std::string prefix = "$KV." + m_bucket + ".";
    const std::string request_payload =
        nlohmann::json({{"subjects_filter", prefix + ">"}}).dump();
    auto [response, status] = co_await request_plain(
        "$JS.API.STREAM.INFO.KV_" + m_bucket,
        request_payload,
        std::chrono::seconds(5));
    if (status.failed()) {
        co_return std::pair<std::vector<std::string>, nats_asio::status>{{}, status};
    }

    try {
        const auto body = nlohmann::json::parse(
            std::string_view(response.payload.data(), response.payload.size()));
        if (body.contains("error")) {
            co_return std::pair<std::vector<std::string>, nats_asio::status>{
                {}, nats_asio::status(body["error"].value(
                    "description", "stream info failed"))};
        }

        std::vector<std::string> keys;
        if (body.contains("state") && body["state"].contains("subjects")) {
            for (const auto& [subject, count] : body["state"]["subjects"].items()) {
                (void)count;
                if (subject.starts_with(prefix) && subject.size() > prefix.size()) {
                    keys.emplace_back(subject.substr(prefix.size()));
                }
            }
        }
        co_return std::pair<std::vector<std::string>, nats_asio::status>{
            std::move(keys), nats_asio::status{}};
    } catch (const std::exception& e) {
        co_return std::pair<std::vector<std::string>, nats_asio::status>{
            {}, nats_asio::status(std::string("invalid stream info response: ") + e.what())};
    }
}

std::string lease_manager::make_lease_key(uint64_t subscription_id,
                                          const std::string& client_id) {
    return std::to_string(subscription_id) + "." + client_id;
}

bool lease_manager::parse_lease_key(const std::string& key,
                                    uint64_t& subscription_id,
                                    std::string& client_id) {
    auto dot = key.find('.');
    if (dot == std::string::npos || dot == 0 || dot == key.size() - 1) {
        return false;
    }

    auto id_str = key.substr(0, dot);
    auto [ptr, ec] = std::from_chars(id_str.data(), id_str.data() + id_str.size(),
                                     subscription_id);
    if (ec != std::errc{} || ptr != id_str.data() + id_str.size()) return false;

    client_id = key.substr(dot + 1);
    return true;
}

asio::awaitable<bool> lease_manager::ensure_bucket() {
    using nlohmann::json;
    constexpr auto timeout = std::chrono::seconds(5);

    const std::string stream_name = "KV_" + m_bucket;
    const std::string info_subject = "$JS.API.STREAM.INFO." + stream_name;
    const std::string request_body = "{}";
    auto [info_reply, info_status] = co_await request_plain(
        info_subject, request_body, timeout);

    bool missing = false;
    json info;
    if (info_status.failed()) {
        m_log->error("lease_manager: failed to inspect KV bucket '{}': {}",
                     m_bucket, info_status.error());
        co_return false;
    }

    try {
        info = json::parse(std::string_view(info_reply.payload.data(), info_reply.payload.size()));
        missing = info.contains("error") && info["error"].value("code", 0) == 404;
    } catch (const std::exception& e) {
        m_log->error("lease_manager: invalid stream info response: {}", e.what());
        co_return false;
    }

    const uint64_t expected_max_age = static_cast<uint64_t>(m_ttl_seconds) * 1'000'000'000ULL;
    const std::string expected_subject = "$KV." + m_bucket + ".>";

    if (!missing) {
        if (info.contains("error") || !info.contains("config")) {
            auto description = info.contains("error")
                ? info["error"].value("description", "stream info failed")
                : std::string("missing stream config");
            m_log->error("lease_manager: failed to inspect bucket '{}': {}",
                         m_bucket, description);
            co_return false;
        }

        const auto& cfg = info["config"];
        const auto max_age = cfg.value("max_age", uint64_t{0});
        const auto history = cfg.value("max_msgs_per_subject", int64_t{0});
        const auto subjects = cfg.value("subjects", std::vector<std::string>{});
        const bool subject_ok = std::find(subjects.begin(), subjects.end(),
                                          expected_subject) != subjects.end();
        if (max_age != expected_max_age || history != 1 || !subject_ok) {
            m_log->error(
                "lease_manager: bucket '{}' configuration mismatch "
                "(expected ttl={}s, history=1, subject='{}')",
                m_bucket, m_ttl_seconds, expected_subject);
            co_return false;
        }

        m_log->info("lease_manager: validated KV bucket '{}' (TTL={}s)",
                    m_bucket, m_ttl_seconds);
        co_return true;
    }

    json stream_config = {
        {"name", stream_name},
        {"subjects", {expected_subject}},
        {"retention", "limits"},
        {"storage", "file"},
        {"max_msgs", -1},
        {"max_bytes", -1},
        {"max_age", expected_max_age},
        {"max_msgs_per_subject", 1},
        {"max_msg_size", -1},
        {"discard", "old"},
        {"num_replicas", 1},
        {"allow_rollup_hdrs", true},
        {"deny_delete", true},
        {"allow_direct", true}
    };
    const std::string create_payload = stream_config.dump();
    auto [create_reply, create_status] = co_await request_plain(
        "$JS.API.STREAM.CREATE." + stream_name, create_payload, timeout);
    if (create_status.failed()) {
        m_log->error("lease_manager: failed to create KV bucket '{}': {}",
                     m_bucket, create_status.error());
        co_return false;
    }

    try {
        auto response = json::parse(
            std::string_view(create_reply.payload.data(), create_reply.payload.size()));
        if (response.contains("error")) {
            m_log->error("lease_manager: failed to create KV bucket '{}': {}",
                         m_bucket,
                         response["error"].value("description", "unknown error"));
            co_return false;
        }
    } catch (const std::exception& e) {
        m_log->error("lease_manager: invalid bucket creation response: {}", e.what());
        co_return false;
    }

    m_log->info("lease_manager: created KV bucket '{}' (TTL={}s)",
                m_bucket, m_ttl_seconds);
    co_return true;
}

asio::awaitable<bool> lease_manager::persist_lease(
    uint64_t subscription_id, const std::string& expression,
    const std::string& client_id) {
    nlohmann::json record = {
        {"version", 1},
        {"subscription_id", subscription_id},
        {"client_id", client_id},
        {"expression", expression}
    };
    const std::string value = record.dump();
    const std::string key = make_lease_key(subscription_id, client_id);
    const std::string subject = "$KV." + m_bucket + "." + key;
    if (m_conn->is_backpressure_active()) {
        auto drain_status = co_await m_conn->wait_for_drain(std::chrono::seconds(5));
        if (drain_status.failed()) {
            m_log->error("lease_manager: output backpressure blocked lease '{}': {}",
                         key, drain_status.error());
            co_return false;
        }
    }
    std::string wire;
    wire.reserve(subject.size() + value.size() + 32);
    wire += "PUB ";
    wire += subject;
    wire += " ";
    wire += std::to_string(value.size());
    wire += "\r\n";
    wire += value;
    wire += "\r\n";
    auto status = co_await m_conn->write_raw(
        std::span<const char>(wire.data(), wire.size()));
    if (status.failed()) {
        m_log->error("lease_manager: failed to persist lease '{}': {}",
                     key, status.error());
        co_return false;
    }

    uint64_t revision = 0;
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        auto [entry, get_status] = co_await get_lease(key);
        if (!get_status.failed() && entry.value.size() == value.size() &&
            std::equal(entry.value.begin(), entry.value.end(), value.begin())) {
            revision = entry.revision;
            break;
        }
        if (get_status.failed()) {
            m_log->debug("lease_manager: lease '{}' not visible yet: {}", key,
                         get_status.error());
        } else {
            m_log->debug("lease_manager: lease '{}' verification mismatch "
                         "(expected {} bytes, got {} bytes, revision={})",
                         key, value.size(), entry.value.size(), entry.revision);
        }
        timer.expires_after(std::chrono::milliseconds(10));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (revision == 0) {
        m_log->error("lease_manager: timed out verifying persisted lease '{}'", key);
        co_return false;
    }
    m_log->debug("lease_manager: persisted lease '{}' at revision {}", key, revision);
    m_expirations[key] = std::chrono::system_clock::now() +
                         std::chrono::seconds(m_ttl_seconds);
    co_return true;
}

asio::awaitable<bool> lease_manager::delete_lease(
    uint64_t subscription_id, const std::string& client_id) {
    const std::string key = make_lease_key(subscription_id, client_id);
    const std::string subject = "$KV." + m_bucket + "." + key;
    const std::string tombstone = nlohmann::json({
        {"version", 1}, {"deleted", true}
    }).dump();
    std::string wire;
    wire.reserve(subject.size() + tombstone.size() + 32);
    wire += "PUB ";
    wire += subject;
    wire += " ";
    wire += std::to_string(tombstone.size());
    wire += "\r\n";
    wire += tombstone;
    wire += "\r\n";
    auto status = co_await m_conn->write_raw(
        std::span<const char>(wire.data(), wire.size()));
    if (status.failed()) {
        m_log->error("lease_manager: failed to delete lease '{}': {}",
                     key, status.error());
        co_return false;
    }

    bool verified = false;
    asio::steady_timer timer(co_await asio::this_coro::executor);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        auto [entry, get_status] = co_await get_lease(key);
        if (!get_status.failed() && entry.value.size() == tombstone.size() &&
            std::equal(entry.value.begin(), entry.value.end(), tombstone.begin())) {
            verified = true;
            break;
        }
        timer.expires_after(std::chrono::milliseconds(10));
        co_await timer.async_wait(asio::use_awaitable);
    }
    if (!verified) {
        m_log->error("lease_manager: timed out verifying deleted lease '{}'", key);
        co_return false;
    }
    m_log->debug("lease_manager: deleted lease '{}'", key);
    m_expirations.erase(key);
    co_return true;
}

asio::awaitable<bool> lease_manager::restore_leases() {
    auto [keys, keys_status] = co_await list_lease_keys();
    if (keys_status.failed()) {
        m_log->error("lease_manager: failed to list leases: {}", keys_status.error());
        co_return false;
    }

    std::size_t restored = 0;
    for (const auto& key : keys) {
        uint64_t key_subscription_id = 0;
        std::string key_client_id;
        if (!parse_lease_key(key, key_subscription_id, key_client_id)) {
            m_log->warn("lease_manager: ignoring invalid lease key '{}'", key);
            continue;
        }

        auto [entry, get_status] = co_await get_lease(key);
        if (get_status.failed() || entry.op != nats_asio::kv_entry::operation::put) {
            continue; // It may have expired between the key scan and this get.
        }

        try {
            auto record = nlohmann::json::parse(
                std::string_view(entry.value.data(), entry.value.size()));
            if (record.value("deleted", false)) continue;
            const auto version = record.at("version").get<int>();
            const auto subscription_id = record.at("subscription_id").get<uint64_t>();
            const auto client_id = record.at("client_id").get<std::string>();
            const auto expression = record.at("expression").get<std::string>();
            if (version != 1 || subscription_id != key_subscription_id ||
                client_id != key_client_id) {
                throw std::runtime_error("record does not match its key");
            }
            if (!m_sub_mgr.restore(subscription_id, expression, client_id)) {
                throw std::runtime_error("record conflicts with another active lease");
            }
            const auto created = entry.created == std::chrono::system_clock::time_point{}
                ? std::chrono::system_clock::now() : entry.created;
            m_expirations[key] = created + std::chrono::seconds(m_ttl_seconds);
            ++restored;
        } catch (const std::exception& e) {
            m_log->warn("lease_manager: ignoring invalid lease '{}': {}", key, e.what());
        }
    }

    m_log->info("lease_manager: restored {} active lease(s)", restored);
    co_return true;
}

asio::awaitable<void> lease_manager::cleanup_loop() {
    while (!m_stopping.load(std::memory_order_acquire)) {
        m_cleanup_timer->expires_after(
            std::chrono::seconds(m_check_interval_seconds));
        std::error_code ec;
        co_await m_cleanup_timer->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));
        if (m_stopping.load(std::memory_order_acquire) ||
            ec == asio::error::operation_aborted) {
            co_return;
        }
        if (ec) {
            m_log->warn("lease_manager: cleanup timer failed: {}", ec.message());
            continue;
        }

        std::vector<std::string> keys;
        keys.reserve(m_expirations.size());
        for (const auto& [key, expiry] : m_expirations) keys.push_back(key);

        for (const auto& key : keys) {
            auto [entry, status] = co_await get_lease(key);
            if (!status.failed()) {
                const auto created = entry.created == std::chrono::system_clock::time_point{}
                    ? std::chrono::system_clock::now() : entry.created;
                m_expirations[key] = created + std::chrono::seconds(m_ttl_seconds);
                continue;
            }
            if (status.code() != nats_asio::error_code::key_not_found) {
                m_log->warn("lease_manager: failed to reconcile lease '{}': {}",
                            key, status.error());
                continue;
            }

            uint64_t subscription_id = 0;
            std::string client_id;
            if (parse_lease_key(key, subscription_id, client_id)) {
                m_log->info("lease_manager: lease expired/deleted for subscription {}, client '{}'",
                            subscription_id, client_id);
                m_sub_mgr.remove_lease(subscription_id, client_id);
            }
            m_expirations.erase(key);
        }
    }
}

void lease_manager::stop() {
    if (m_stopping.exchange(true, std::memory_order_acq_rel)) return;
    if (m_request_subscription) m_request_subscription->cancel();
    m_requests.clear();
    if (m_cleanup_timer) {
        std::error_code ec;
        m_cleanup_timer->cancel(ec);
    }
}

asio::awaitable<bool> lease_manager::start() {
    if (!co_await start_request_mux()) co_return false;
    if (!co_await ensure_bucket()) co_return false;
    if (!co_await restore_leases()) co_return false;

    m_stopping.store(false, std::memory_order_release);
    m_cleanup_timer = std::make_unique<asio::steady_timer>(m_ioc);
    asio::co_spawn(m_ioc, cleanup_loop(), asio::detached);
    m_log->info("lease_manager: reconciling KV bucket '{}' every {}s",
                m_bucket, m_check_interval_seconds);
    co_return true;
}

} // namespace sidecar
