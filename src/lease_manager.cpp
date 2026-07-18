#include "lease_manager.hpp"
#include <charconv>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/redirect_error.hpp>
#include <asio/use_awaitable.hpp>
#include <nlohmann/json.hpp>

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

asio::awaitable<std::pair<nats_asio::message, nats_asio::status>>
lease_manager::request_plain(const std::string& subject,
                             const std::string& payload,
                             std::chrono::milliseconds timeout) {
    co_return co_await m_conn->request(
        subject, std::span<const char>(payload.data(), payload.size()), timeout);
}

asio::awaitable<std::pair<nats_asio::kv_entry, nats_asio::status>>
lease_manager::get_lease(const std::string& key) {
    co_return co_await m_conn->kv_get(m_bucket, key, std::chrono::seconds(5));
}

asio::awaitable<std::pair<std::vector<std::string>, nats_asio::status>>
lease_manager::list_lease_keys() {
    co_return co_await m_conn->kv_keys(m_bucket, std::chrono::seconds(5));
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
    auto [revision, status] = co_await m_conn->kv_put(
        m_bucket, key, std::span<const char>(value.data(), value.size()),
        std::chrono::seconds(5));
    if (status.failed()) {
        m_log->error("lease_manager: failed to persist lease '{}': {}",
                     key, status.error());
        co_return false;
    }
    if (revision == 0) {
        m_log->error("lease_manager: persisted lease '{}' without a revision", key);
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
    auto [revision, status] = co_await m_conn->kv_delete(
        m_bucket, key, std::chrono::seconds(5));
    if (status.failed()) {
        m_log->error("lease_manager: failed to delete lease '{}': {}",
                     key, status.error());
        co_return false;
    }
    if (revision == 0) {
        m_log->error("lease_manager: deleted lease '{}' without a revision", key);
        co_return false;
    }
    m_log->debug("lease_manager: deleted lease '{}' at revision {}", key, revision);
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
    if (m_cleanup_timer) {
        std::error_code ec;
        m_cleanup_timer->cancel(ec);
    }
}

asio::awaitable<bool> lease_manager::start() {
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
