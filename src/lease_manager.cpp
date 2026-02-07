#include "lease_manager.hpp"
#include <charconv>
#include <asio/use_awaitable.hpp>

namespace sidecar {

lease_manager::lease_manager(asio::io_context& ioc,
                             nats_asio::iconnection_sptr conn,
                             subscription_manager& sub_mgr,
                             const std::string& bucket,
                             uint32_t check_interval_seconds,
                             std::shared_ptr<spdlog::logger> log)
    : m_ioc(ioc), m_conn(std::move(conn)), m_sub_mgr(sub_mgr),
      m_bucket(bucket), m_check_interval_seconds(check_interval_seconds),
      m_log(std::move(log))
{}

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
    if (ec != std::errc{}) return false;

    client_id = key.substr(dot + 1);
    return true;
}

void lease_manager::on_kv_entry(const nats_asio::kv_entry& entry) {
    // We only care about deletions (TTL expiry or explicit delete)
    if (entry.op == nats_asio::kv_entry::operation::put) {
        m_log->debug("lease_manager: KV put for key '{}'", entry.key);
        return;
    }

    // entry.op is del or purge — lease expired
    uint64_t sub_id;
    std::string client_id;
    if (!parse_lease_key(entry.key, sub_id, client_id)) {
        m_log->warn("lease_manager: failed to parse lease key '{}'", entry.key);
        return;
    }

    m_log->info("lease_manager: lease expired for subscription {}, client '{}'",
               sub_id, client_id);

    bool fully_removed = m_sub_mgr.remove_lease(sub_id, client_id);
    if (fully_removed) {
        m_log->info("lease_manager: subscription {} fully removed (no active leases)", sub_id);
    }
}

asio::awaitable<bool> lease_manager::start() {
    auto [watcher, status] = co_await m_conn->kv_watch(
        m_bucket,
        [this](const nats_asio::kv_entry& entry) -> asio::awaitable<void> {
            on_kv_entry(entry);
            co_return;
        },
        ">"  // watch all keys in the bucket
    );

    if (status.failed()) {
        m_log->error("lease_manager: failed to watch KV bucket '{}': {}",
                    m_bucket, status.error());
        co_return false;
    }

    m_watcher = watcher;
    m_log->info("lease_manager: watching KV bucket '{}'", m_bucket);
    co_return true;
}

} // namespace sidecar
