#pragma once

#include "config.hpp"
#include "tree_snapshot.hpp"
#include <atree.hpp>
#include <spdlog/spdlog.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace sidecar {

struct subscription_info {
    uint64_t id;
    std::string expression;
    // Clients holding active leases for this subscription
    std::unordered_set<std::string> lease_holders;
};

// Manages boolean expression subscriptions in the A-Tree.
// Uses RCU-style snapshot swapping: readers get a lock-free shared_ptr<const tree_snapshot>,
// writers serialize via mutex and atomically publish new snapshots.
class subscription_manager {
public:
    subscription_manager(const std::vector<attribute_def>& attributes,
                         const std::string& output_prefix,
                         std::shared_ptr<spdlog::logger> log);

    // Subscribe with a boolean expression. Returns the subscription ID
    // (new or existing). Throws atree::Error on invalid expression.
    uint64_t subscribe(const std::string& expression, const std::string& client_id);

    // Remove a specific client's lease from a subscription.
    // Returns true if the subscription was fully removed (no more lease holders).
    bool remove_lease(uint64_t subscription_id, const std::string& client_id);

    // Remove all leases for a subscription. Returns true if it existed.
    bool remove_subscription(uint64_t subscription_id);

    // Look up subscription by ID
    std::optional<subscription_info> get_subscription(uint64_t id) const;

    // Look up subscription ID by expression string
    std::optional<uint64_t> find_by_expression(const std::string& expression) const;

    // Get an immutable snapshot for lock-free concurrent reads.
    std::shared_ptr<const tree_snapshot> snapshot() const;

    // Stats
    std::size_t active_count() const;

private:
    // Rebuild tree from all current expressions and publish a new snapshot.
    void publish_snapshot();

    std::shared_ptr<spdlog::logger> m_log;

    // Serializes all write operations (subscribe/remove).
    // Near-zero contention: all writers run on the ASIO thread.
    std::mutex m_write_mutex;

    // Needed to rebuild tree from scratch on expression changes.
    std::vector<attribute_def> m_attributes;
    std::string m_output_prefix;

    // Current snapshot — atomic load/store for lock-free reader access.
    std::shared_ptr<const tree_snapshot> m_snapshot;

    // Writer-only state (protected by m_write_mutex)
    uint64_t m_next_id = 1;
    std::unordered_map<std::string, uint64_t> m_expr_to_id;
    std::unordered_map<uint64_t, subscription_info> m_subscriptions;
};

} // namespace sidecar
