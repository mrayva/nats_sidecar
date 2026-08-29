#pragma once

#include "config.hpp"
#include "matching_engine.hpp"
#include <spdlog/spdlog.h>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace sidecar {

// Result of removing a single client's lease from a subscription.
enum class lease_removal {
    not_found,      // subscription_id doesn't exist (already gone)
    still_active,   // other clients still hold a lease on it
    fully_removed   // this was the last lease holder; subscription is gone
};

struct subscription_info {
    uint64_t id;
    std::string expression;
    // Clients holding active leases for this subscription
    std::unordered_set<std::string> lease_holders;
};

// Manages boolean expression subscriptions in the matching engine.
// One live matching_engine, mutated via true incremental insert() calls (see subscribe()/
// restore()) and protected by a shared_mutex: writers (subscribe/restore/remove, always on the
// single ASIO thread - see m_mutex's own comment) take a unique_lock, readers (worker threads'
// search() calls, via acquire_tree()) take a shared_lock, exactly like the multi-reader
// concurrent access this class's previous RCU-snapshot design already relied on being safe.
// Deliberately NOT lock-free for readers (unlike the pattern this replaced) - see
// subscription_manager.cpp's own top-of-file comment for why that trade is the actual fix here,
// not an accident.
class subscription_manager {
public:
    subscription_manager(const std::vector<attribute_def>& attributes,
                         const std::string& output_prefix,
                         std::shared_ptr<spdlog::logger> log,
                         engine_type engine = engine_type::atree);

    // Subscribe with a boolean expression. Returns the subscription ID
    // (new or existing). Throws matching_engine_error on invalid expression.
    uint64_t subscribe(const std::string& expression, const std::string& client_id);

    // Restore a persisted subscription using its original ID. Returns false
    // if the record conflicts with already-restored state.
    bool restore(uint64_t subscription_id, const std::string& expression,
                 const std::string& client_id);

    // Remove a specific client's lease from a subscription.
    lease_removal remove_lease(uint64_t subscription_id, const std::string& client_id);

    // Remove all leases for a subscription. Returns true if it existed.
    bool remove_subscription(uint64_t subscription_id);

    // Look up subscription by ID
    std::optional<subscription_info> get_subscription(uint64_t id) const;

    // Look up subscription ID by expression string
    std::optional<uint64_t> find_by_expression(const std::string& expression) const;

    // RAII handle giving synchronous, lock-protected read access to the current live matching
    // tree - holds a shared_lock for its own lifetime. Must only be used synchronously (e.g.
    // across a matching_engine::search() call) and must never be held across an asio coroutine
    // suspension point: doing so would block every writer (subscribe/restore/remove) for as long
    // as the await takes, or worse. Let it go out of scope as soon as the synchronous match call
    // is done - see worker_pool.cpp's worker_loop() for the intended usage shape.
    class tree_read_guard {
    public:
        const matching_engine& operator*() const { return *m_tree; }
        const matching_engine* operator->() const { return m_tree; }
        explicit operator bool() const { return m_tree != nullptr; }

    private:
        friend class subscription_manager;
        tree_read_guard(std::shared_lock<std::shared_mutex> lock, const matching_engine* tree)
            : m_lock(std::move(lock)), m_tree(tree) {}

        std::shared_lock<std::shared_mutex> m_lock;
        const matching_engine* m_tree;
    };

    // Acquire synchronous read access to the current live tree. See tree_read_guard's own
    // comment for the usage contract.
    tree_read_guard acquire_tree() const;

    // Output subject for `id` if it is still an active subscription right now, nullopt
    // otherwise (e.g. removed between a worker's search() and this call - already an expected,
    // silently-handled case at every call site, not a new failure mode). Cheap: a brief
    // shared_lock plus an O(1) map lookup, safe to call synchronously from a worker thread.
    std::optional<std::string> output_subject(uint64_t id) const;

    // Stats
    std::size_t active_count() const;

private:
    // Discards the current tree and rebuilds it from scratch by re-parsing and re-inserting
    // every expression in m_subscriptions. O(current subscription count) - only used where that
    // cost is unavoidable: as a safety net after a failed incremental insert() (see subscribe()/
    // restore()'s own comments), and for remove_lease()/remove_subscription(), since none of
    // a-tree/be-tree/pstree expose a delete primitive. Must be called with m_mutex already
    // held exclusively.
    void rebuild_tree_locked();

    std::shared_ptr<spdlog::logger> m_log;

    // Protects everything below: the writer-only subscription bookkeeping AND the live tree
    // itself. Writers (subscribe/restore/remove, always on the single ASIO thread - near-zero
    // contention among writers by construction) take a unique_lock; readers (worker threads'
    // acquire_tree()/output_subject() calls) take a shared_lock, so concurrent searches across
    // worker threads remain exactly as concurrent as they were under the old RCU design.
    mutable std::shared_mutex m_mutex;

    // Needed to rebuild the tree from scratch (rebuild_tree_locked()) and to construct a fresh
    // one at startup.
    std::vector<attribute_def> m_attributes;
    std::string m_output_prefix;
    engine_type m_engine;

    // The one live matching tree, mutated in place via true incremental insert() calls -
    // protected by m_mutex (see above), not swapped wholesale on every write like the old
    // tree_snapshot/m_snapshot pattern this replaced.
    std::unique_ptr<matching_engine> m_tree;
    std::size_t m_active_count = 0;

    // Writer-only state (protected by m_mutex)
    uint64_t m_next_id = 1;
    std::unordered_map<std::string, uint64_t> m_expr_to_id;
    std::unordered_map<uint64_t, subscription_info> m_subscriptions;
};

} // namespace sidecar
