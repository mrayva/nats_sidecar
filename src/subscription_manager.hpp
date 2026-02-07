#pragma once

#include "config.hpp"
#include <atree.hpp>
#include <spdlog/spdlog.h>
#include <cstdint>
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
// Thread-safe: all public methods are internally synchronized (for future
// multi-threaded use), but designed for single-threaded asio usage today.
class subscription_manager {
public:
    subscription_manager(const std::vector<attribute_def>& attributes,
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

    // Get the A-Tree (for searching). Caller must not modify.
    const atree::Tree& tree() const { return m_tree; }

    // Stats
    std::size_t active_count() const;

private:
    atree::Tree m_tree;
    std::shared_ptr<spdlog::logger> m_log;

    uint64_t m_next_id = 1;

    // expression string -> subscription_id
    std::unordered_map<std::string, uint64_t> m_expr_to_id;

    // subscription_id -> subscription_info
    std::unordered_map<uint64_t, subscription_info> m_subscriptions;
};

} // namespace sidecar
