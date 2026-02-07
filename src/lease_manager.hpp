#pragma once

#include "subscription_manager.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

namespace sidecar {

// Lease key format: <BE-ID>.<client-id>
// Value: anything (presence = alive). TTL enforced by NATS KV.
//
// The lease_manager watches the KV bucket for deletions/purges (TTL expiry)
// and notifies the subscription_manager to remove the corresponding lease.

class lease_manager {
public:
    lease_manager(asio::io_context& ioc,
                  nats_asio::iconnection_sptr conn,
                  subscription_manager& sub_mgr,
                  const std::string& bucket,
                  uint32_t check_interval_seconds,
                  std::shared_ptr<spdlog::logger> log);

    // Start watching the KV bucket for lease changes.
    // Must be called after NATS connection is established.
    asio::awaitable<bool> start();

    // Build a lease key from subscription ID and client ID
    static std::string make_lease_key(uint64_t subscription_id, const std::string& client_id);

    // Parse a lease key back into subscription_id and client_id.
    // Returns false if the key format is invalid.
    static bool parse_lease_key(const std::string& key,
                                uint64_t& subscription_id,
                                std::string& client_id);

private:
    // KV watcher callback - invoked on entry changes
    void on_kv_entry(const nats_asio::kv_entry& entry);

    asio::io_context& m_ioc;
    nats_asio::iconnection_sptr m_conn;
    subscription_manager& m_sub_mgr;
    std::string m_bucket;
    uint32_t m_check_interval_seconds;
    std::shared_ptr<spdlog::logger> m_log;
    nats_asio::ikv_watcher_sptr m_watcher;
};

} // namespace sidecar
