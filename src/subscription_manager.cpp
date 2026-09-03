#include "subscription_manager.hpp"
#include <fmt/compile.h>
#include <fmt/format.h>

// This file replaced an RCU-style "rebuild a brand-new tree from scratch and atomically swap it
// in" pattern that ran on every single subscribe() call - O(current subscription count) work per
// insert, O(K^2) total across K sequential inserts. That cost was never inherent to the matching
// engines themselves (matching_engine::insert() is already a genuinely incremental, single-item
// primitive for all three engines - see matching_engine.cpp), only to this class's own "throw
// away and rebuild everything" wrapper around it. Confirmed as a real production problem: bulk-
// subscribing thousands of expressions saturated the single ASIO I/O thread badly enough to
// trigger cascading JetStream lease-timeout failures under real fleet load.
//
// Now there is one live matching_engine, mutated via true incremental insert() calls under an
// exclusive lock, instead of being discarded and rebuilt on every write. See subscription_manager
// .hpp's own class-level comment for the shared_mutex reader/writer contract, and
// worker_pool.cpp's worker_loop() for why the tree and the output-subject lookup no longer need
// to be published together as one wholesale-immutable object (the output-subject lookup used to
// be read from inside an async publish coroutine, potentially long after a message's synchronous
// match phase - it's now resolved synchronously instead, so it doesn't need to survive that long).

namespace sidecar {

subscription_manager::subscription_manager(
    const std::vector<attribute_def>& attributes,
    const std::string& output_prefix,
    std::shared_ptr<spdlog::logger> log,
    engine_type engine)
    : m_log(std::move(log)),
      m_attributes(attributes),
      m_output_prefix(output_prefix),
      m_engine(engine),
      m_tree(build_matching_engine(m_engine, m_attributes))
{
}

void subscription_manager::rebuild_tree_locked() {
    std::unique_ptr<matching_engine> tree = build_matching_engine(m_engine, m_attributes);
    for (const auto& [id, sub] : m_subscriptions) {
        tree->insert(id, sub.expression);
    }
    m_tree = std::move(tree);
    m_active_count = m_subscriptions.size();
}

uint64_t subscription_manager::subscribe(const std::string& expression,
                                         const std::string& client_id) {
    std::unique_lock lock(m_mutex);

    // Check if expression already exists — lease-only change, no tree mutation
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) {
        auto& sub = m_subscriptions[it->second];
        sub.lease_holders.insert(client_id);
        m_log->info("Reused subscription {} for expression '{}', client '{}'",
                   it->second, expression, client_id);
        return it->second;
    }

    // New expression — insert directly into the live tree first to validate (throws before maps
    // are modified). True incremental insert: O(this one expression), not O(current subscription
    // count) - the actual fix for the O(K^2) bulk-subscribe cost (see this file's top comment).
    uint64_t id = m_next_id;
    try {
        m_tree->insert(id, expression);
    } catch (...) {
        // Safety net, not the common case: an engine's insert() rejecting an invalid expression
        // should leave it untouched, but rebuild from m_subscriptions (unchanged - the new sub
        // was never added) rather than assume that's true for all three engines. Costs no more
        // than the old code already paid unconditionally for every invalid-expression rejection.
        rebuild_tree_locked();
        throw;
    }
    ++m_next_id;

    subscription_info info;
    info.id = id;
    info.expression = expression;
    info.lease_holders.insert(client_id);
    m_subscriptions[id] = std::move(info);
    m_expr_to_id[expression] = id;
    m_active_count = m_subscriptions.size();

    m_log->info("New subscription {} for expression '{}', client '{}'",
               id, expression, client_id);
    return id;
}

bool subscription_manager::restore(uint64_t subscription_id,
                                   const std::string& expression,
                                   const std::string& client_id) {
    std::unique_lock lock(m_mutex);

    auto expr_it = m_expr_to_id.find(expression);
    if (expr_it != m_expr_to_id.end()) {
        if (expr_it->second != subscription_id) return false;
        m_subscriptions.at(subscription_id).lease_holders.insert(client_id);
        m_next_id = std::max(m_next_id, subscription_id + 1);
        return true;
    }

    auto id_it = m_subscriptions.find(subscription_id);
    if (id_it != m_subscriptions.end()) return false;

    try {
        m_tree->insert(subscription_id, expression);
    } catch (...) {
        rebuild_tree_locked();
        throw;
    }

    subscription_info info;
    info.id = subscription_id;
    info.expression = expression;
    info.lease_holders.insert(client_id);
    m_subscriptions.emplace(subscription_id, std::move(info));
    m_expr_to_id.emplace(expression, subscription_id);
    m_active_count = m_subscriptions.size();

    m_next_id = std::max(m_next_id, subscription_id + 1);
    return true;
}

lease_removal subscription_manager::remove_lease(uint64_t subscription_id,
                                                 const std::string& client_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return lease_removal::not_found;

    it->second.lease_holders.erase(client_id);

    if (it->second.lease_holders.empty()) {
        // No more clients — remove subscription and rebuild the tree. No engine here exposes a
        // delete primitive, so this stays O(remaining subscriptions), same as before this file's
        // insert-path fix - see rebuild_tree_locked()'s own comment.
        m_expr_to_id.erase(it->second.expression);
        m_log->info("Removed subscription {} (expression '{}') - no active leases",
                   subscription_id, it->second.expression);
        m_subscriptions.erase(it);
        rebuild_tree_locked();
        return lease_removal::fully_removed;
    }

    m_log->debug("Removed lease for client '{}' on subscription {}, {} leases remain",
                client_id, subscription_id, it->second.lease_holders.size());
    return lease_removal::still_active;
}

bool subscription_manager::remove_subscription(uint64_t subscription_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return false;

    m_expr_to_id.erase(it->second.expression);
    m_log->info("Force-removed subscription {} (expression '{}')",
               subscription_id, it->second.expression);
    m_subscriptions.erase(it);
    rebuild_tree_locked();
    return true;
}

std::optional<subscription_info> subscription_manager::get_subscription(uint64_t id) const {
    std::shared_lock lock(m_mutex);
    auto it = m_subscriptions.find(id);
    if (it != m_subscriptions.end()) return it->second;
    return std::nullopt;
}

std::optional<uint64_t> subscription_manager::find_by_expression(const std::string& expression) const {
    std::shared_lock lock(m_mutex);
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) return it->second;
    return std::nullopt;
}

subscription_manager::tree_read_guard subscription_manager::acquire_tree() const {
    std::shared_lock lock(m_mutex);
    return tree_read_guard(std::move(lock), m_tree.get());
}

std::optional<std::string> subscription_manager::output_subject(uint64_t id) const {
    std::shared_lock lock(m_mutex);
    if (m_subscriptions.find(id) == m_subscriptions.end()) return std::nullopt;
    return fmt::format(FMT_COMPILE("{}.{}"), m_output_prefix, id);
}

std::size_t subscription_manager::active_count() const {
    std::shared_lock lock(m_mutex);
    return m_active_count;
}

} // namespace sidecar
