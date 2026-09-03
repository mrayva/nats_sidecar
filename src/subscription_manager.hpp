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

    // Below: the array-vs-overflow-map split (see m_subscriptions_by_id's own comment for why
    // both exist) collapsed into one small set of helpers so subscribe()/restore()/remove_lease()/
    // remove_subscription()/get_subscription()/output_subject()/rebuild_tree_locked() each call
    // one of these instead of repeating the "which structure does this id belong to" branch six
    // times over. All assume m_mutex is already held by the caller (shared or exclusive as
    // appropriate - these don't lock themselves).

    // Non-owning pointer to id's live record, or nullptr if id has no active subscription.
    subscription_info* find_locked(uint64_t id);
    const subscription_info* find_locked(uint64_t id) const;

    // Inserts a new live record for id (id must not already be live - callers check that via
    // find_locked() first, same as the map-based code this replaced did).
    void insert_locked(uint64_t id, subscription_info info);

    // Removes id's live record (id must currently be live). Does not touch m_active_count -
    // callers already do that alongside their own m_expr_to_id.erase() call.
    void erase_locked(uint64_t id);

    // Invokes fn(id, expression) for every currently-live subscription, in no particular order -
    // used only by rebuild_tree_locked(), which doesn't care about order.
    template <typename Fn>
    void for_each_locked(Fn&& fn) const;

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
    // No longer derivable as "m_subscriptions.size()" (a live hash map's own entry count) now
    // that subscriptions are array-indexed with dead slots left in place - maintained explicitly
    // at every mutation site instead (subscribe()/restore()'s new-subscription branches
    // increment, remove_lease()'s fully_removed and remove_subscription() decrement).
    // rebuild_tree_locked() no longer touches this - it neither adds nor removes subscriptions,
    // only reconstructs the tree from whatever's already live.
    std::size_t m_active_count = 0;

    // Writer-only state (protected by m_mutex)
    uint64_t m_next_id = 1;
    std::unordered_map<std::string, uint64_t> m_expr_to_id;

    // Indexed DIRECTLY by subscription id - not a hash map - for every id below
    // kArrayIndexCap. Ids are confirmed dense, monotonic, permanent integers in real use
    // (subscription_registry::resolve_id() hands them out as NATS JetStream KV bucket revision
    // numbers - see that function's own comment), so array indexing is available here in a way
    // it wouldn't be for a truly arbitrary key space. Validated via an isolated microbenchmark
    // before this change (array vs unordered_map, same workload shape): ~51-54% fewer cycles,
    // consistent across two fan-out levels - real hashtable/cache-locality cost eliminated, not
    // just relocated (see git history/README for the full writeup).
    //
    // A dead id's SLOT gets cleared (reset() - same memory-reclamation as unordered_map::erase())
    // but the array's own LENGTH only ever grows up to kArrayIndexCap, since a future restore()
    // for any previously-seen id must still land in bounds; ids themselves are never reused for a
    // different expression (the registry's own permanence guarantee already assumes the same id
    // resolves to the same expression forever, restore()'d or not - reusing a dead id here would
    // break that). Memory cost is therefore proportional to the highest id ever assigned
    // fleet-wide (capped), not the active count - accepted as a real, if usually small (each
    // empty slot costs a few bytes), tradeoff rather than pre-building a paged/compacting
    // structure with no evidence it's needed yet.
    //
    // kArrayIndexCap exists because ids aren't ALWAYS small in practice - restore() is a public
    // API that accepts a caller-supplied id directly (see restore()'s own doc comment), and this
    // class has to stay correct for an arbitrary uint64_t there even though the real production
    // path never produces one (a real test exercises exactly this: a ~0x9F3A7B2... id, simulating
    // an uncoordinated/externally-assigned id rather than the registry's own dense sequence).
    // 1 million comfortably covers any realistic fleet-wide cumulative subscription count for
    // this project's current scale (worst case ~1M * sizeof(std::optional<subscription_info>),
    // on the order of tens of MB, not a real concern) while still safely falling back to
    // m_subscriptions_overflow - functionally identical to this class's old, sole
    // std::unordered_map, just now the rarely-taken path - for anything larger, rather than
    // attempting a multi-terabyte allocation.
    static constexpr uint64_t kArrayIndexCap = 1'000'000;
    std::vector<std::optional<subscription_info>> m_subscriptions_by_id;
    std::unordered_map<uint64_t, subscription_info> m_subscriptions_overflow;

    // m_subscriptions.size() (the map's own live-entry count) no longer exists to read this back
    // from - maintained explicitly at every mutation site instead (subscribe()/restore()'s new-
    // subscription branches increment, remove_lease()'s fully_removed and remove_subscription()
    // decrement). rebuild_tree_locked() no longer touches this - it neither adds nor removes
    // subscriptions, only reconstructs the tree from whatever's already live.
    std::size_t m_active_count_tracked = 0;
};

} // namespace sidecar
