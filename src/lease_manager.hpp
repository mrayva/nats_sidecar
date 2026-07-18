#pragma once

#include "subscription_manager.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace sidecar {

// Lease key format: <subscription-id>.<client-id>. The JSON value contains the
// complete subscription record, and NATS KV max_age enforces its TTL. Periodic
// reconciliation removes subscriptions whose records have expired.

class lease_manager {
public:
    lease_manager(asio::io_context& ioc,
                  nats_asio::iconnection_sptr conn,
                  subscription_manager& sub_mgr,
                  const std::string& bucket,
                  uint32_t ttl_seconds,
                  uint32_t check_interval_seconds,
                  std::shared_ptr<spdlog::logger> log);

    ~lease_manager();

    // Validate/provision the KV bucket, restore leases, and start reconciliation.
    // Must be called after NATS connection is established.
    asio::awaitable<bool> start();

    // Create or refresh the server-owned lease record. The bucket TTL applies
    // to every write, so repeating subscribe safely renews the lease.
    asio::awaitable<bool> persist_lease(uint64_t subscription_id,
                                        const std::string& expression,
                                        const std::string& client_id);

    asio::awaitable<bool> delete_lease(uint64_t subscription_id,
                                       const std::string& client_id);

    void stop();

    // Build a lease key from subscription ID and client ID
    static std::string make_lease_key(uint64_t subscription_id, const std::string& client_id);

    // Parse a lease key back into subscription_id and client_id.
    // Returns false if the key format is invalid.
    static bool parse_lease_key(const std::string& key,
                                uint64_t& subscription_id,
                                std::string& client_id);

private:
    struct request_state {
        bool received = false;
        nats_asio::message response;
    };

    asio::awaitable<bool> start_request_mux();
    asio::awaitable<bool> ensure_bucket();
    asio::awaitable<bool> restore_leases();
    asio::awaitable<void> cleanup_loop();
    asio::awaitable<std::pair<nats_asio::message, nats_asio::status>>
    request_plain(const std::string& subject, const std::string& payload,
                  std::chrono::milliseconds timeout);
    asio::awaitable<std::pair<nats_asio::kv_entry, nats_asio::status>>
    get_lease(const std::string& key);
    asio::awaitable<std::pair<std::vector<std::string>, nats_asio::status>>
    list_lease_keys();

    asio::io_context& m_ioc;
    nats_asio::iconnection_sptr m_conn;
    subscription_manager& m_sub_mgr;
    std::string m_bucket;
    uint32_t m_ttl_seconds;
    uint32_t m_check_interval_seconds;
    std::shared_ptr<spdlog::logger> m_log;
    std::unique_ptr<asio::steady_timer> m_cleanup_timer;
    nats_asio::isubscription_sptr m_request_subscription;
    std::string m_request_prefix;
    uint64_t m_next_request_id = 0;
    std::unordered_map<std::string, std::shared_ptr<request_state>> m_requests;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> m_expirations;
    std::atomic<bool> m_stopping{false};
};

} // namespace sidecar
