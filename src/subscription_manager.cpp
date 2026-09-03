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
//
// Subscription storage (m_subscriptions_by_id/m_subscriptions_overflow) replaced a single
// std::unordered_map<uint64_t, subscription_info> with an array-indexed-first, hash-map-fallback
// design - see m_subscriptions_by_id's own comment in the header for why (ids are dense/monotonic
// in real use, confirmed via subscription_registry::resolve_id(), but restore() is a public API
// that must still accept an arbitrary uint64_t correctly). find_locked()/insert_locked()/
// erase_locked()/for_each_locked() below are the one place that split lives, so every other
// method just calls them without repeating the "which structure does this id belong to" check.

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

const subscription_info* subscription_manager::find_locked(uint64_t id) const {
    if (id < kArrayIndexCap) {
        if (id < m_subscriptions_by_id.size() && m_subscriptions_by_id[id].has_value()) {
            return &*m_subscriptions_by_id[id];
        }
        return nullptr;
    }
    auto it = m_subscriptions_overflow.find(id);
    return it != m_subscriptions_overflow.end() ? &it->second : nullptr;
}

subscription_info* subscription_manager::find_locked(uint64_t id) {
    return const_cast<subscription_info*>(
        const_cast<const subscription_manager*>(this)->find_locked(id));
}

void subscription_manager::insert_locked(uint64_t id, subscription_info info) {
    if (id < kArrayIndexCap) {
        if (id >= m_subscriptions_by_id.size()) m_subscriptions_by_id.resize(id + 1);
        m_subscriptions_by_id[id] = std::move(info);
    } else {
        m_subscriptions_overflow.insert_or_assign(id, std::move(info));
    }
}

void subscription_manager::erase_locked(uint64_t id) {
    if (id < kArrayIndexCap) {
        if (id < m_subscriptions_by_id.size()) m_subscriptions_by_id[id].reset();
    } else {
        m_subscriptions_overflow.erase(id);
    }
}

template <typename Fn>
void subscription_manager::for_each_locked(Fn&& fn) const {
    for (std::uint64_t id = 0; id < m_subscriptions_by_id.size(); ++id) {
        if (m_subscriptions_by_id[id]) fn(id, m_subscriptions_by_id[id]->expression);
    }
    for (const auto& [id, info] : m_subscriptions_overflow) {
        fn(id, info.expression);
    }
}

void subscription_manager::rebuild_tree_locked() {
    std::unique_ptr<matching_engine> tree = build_matching_engine(m_engine, m_attributes);
    for_each_locked([&tree](uint64_t id, const std::string& expression) {
        tree->insert(id, expression);
    });
    m_tree = std::move(tree);
    // m_active_count is maintained incrementally at each mutation site now (see its own comment
    // in the header) - rebuild_tree_locked() neither adds nor removes subscriptions, so it has
    // nothing to update here.
}

uint64_t subscription_manager::subscribe(const std::string& expression,
                                         const std::string& client_id) {
    std::unique_lock lock(m_mutex);

    // Check if expression already exists — lease-only change, no tree mutation
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) {
        // Present in m_expr_to_id implies a live record at this id - both are always updated
        // together (see the "new expression" branch below and remove_lease()/
        // remove_subscription()'s own paired erase_locked() calls).
        find_locked(it->second)->lease_holders.insert(client_id);
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
        // should leave it untouched, but rebuild from live subscriptions (unchanged - the new
        // sub was never added) rather than assume that's true for all three engines. Costs no
        // more than the old code already paid unconditionally for every invalid-expression
        // rejection.
        rebuild_tree_locked();
        throw;
    }
    ++m_next_id;

    subscription_info info;
    info.id = id;
    info.expression = expression;
    info.lease_holders.insert(client_id);
    insert_locked(id, std::move(info));
    m_expr_to_id[expression] = id;
    ++m_active_count;

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
        find_locked(subscription_id)->lease_holders.insert(client_id);
        m_next_id = std::max(m_next_id, subscription_id + 1);
        return true;
    }

    // Defensive: catches subscription_id already having a live record under a DIFFERENT
    // expression than the one being restored (m_expr_to_id lookup above already ruled out THIS
    // expression already pointing here) - same guard the previous map-based lookup provided,
    // kept for the same "data inconsistency should fail loudly, not silently overwrite" reason.
    if (find_locked(subscription_id) != nullptr) return false;

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
    insert_locked(subscription_id, std::move(info));
    m_expr_to_id.emplace(expression, subscription_id);
    ++m_active_count;

    m_next_id = std::max(m_next_id, subscription_id + 1);
    return true;
}

lease_removal subscription_manager::remove_lease(uint64_t subscription_id,
                                                 const std::string& client_id) {
    std::unique_lock lock(m_mutex);

    subscription_info* sub = find_locked(subscription_id);
    if (sub == nullptr) return lease_removal::not_found;

    sub->lease_holders.erase(client_id);

    if (sub->lease_holders.empty()) {
        // No more clients — remove subscription and rebuild the tree. No engine here exposes a
        // delete primitive, so this stays O(remaining subscriptions), same as before this file's
        // insert-path fix - see rebuild_tree_locked()'s own comment. Save the expression before
        // erase_locked() clears the record (reclaiming its string/set memory - the id itself is
        // never reused, see m_subscriptions_by_id's own header comment, so the record can safely
        // go back to empty rather than needing to remember anything about what it held).
        std::string removed_expression = std::move(sub->expression);
        m_expr_to_id.erase(removed_expression);
        m_log->info("Removed subscription {} (expression '{}') - no active leases",
                   subscription_id, removed_expression);
        erase_locked(subscription_id);
        --m_active_count;
        rebuild_tree_locked();
        return lease_removal::fully_removed;
    }

    m_log->debug("Removed lease for client '{}' on subscription {}, {} leases remain",
                client_id, subscription_id, sub->lease_holders.size());
    return lease_removal::still_active;
}

bool subscription_manager::remove_subscription(uint64_t subscription_id) {
    std::unique_lock lock(m_mutex);

    subscription_info* sub = find_locked(subscription_id);
    if (sub == nullptr) return false;

    std::string expression = std::move(sub->expression);
    m_expr_to_id.erase(expression);
    m_log->info("Force-removed subscription {} (expression '{}')",
               subscription_id, expression);
    erase_locked(subscription_id);
    --m_active_count;
    rebuild_tree_locked();
    return true;
}

std::optional<subscription_info> subscription_manager::get_subscription(uint64_t id) const {
    std::shared_lock lock(m_mutex);
    const subscription_info* sub = find_locked(id);
    if (sub != nullptr) return *sub; // copy
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
    if (find_locked(id) == nullptr) return std::nullopt;
    return fmt::format(FMT_COMPILE("{}.{}"), m_output_prefix, id);
}

std::size_t subscription_manager::active_count() const {
    std::shared_lock lock(m_mutex);
    return m_active_count;
}

} // namespace sidecar
