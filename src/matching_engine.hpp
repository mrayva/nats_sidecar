#pragma once

#include "config.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace sidecar {

// Thrown by matching_engine::insert() on an invalid expression. Wraps
// whichever concrete engine's own exception type (atree::Error, be-tree's
// BetreeException) behind one type callers need to catch.
class matching_engine_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Hash/KeyEqual pair enabling heterogeneous lookup on a
// std::unordered_map<std::string, V> - i.e. .find(some_string_view) without
// constructing a temporary std::string just to satisfy the map's key type.
// std::equal_to<> is already transparent; is_transparent here is what makes
// unordered_map itself offer the string_view-taking find() overload.
// event_bridge.hpp's attribute_schema and betree_event_sink's index map
// below both do this lookup once per (row, attribute) pair - see their
// own comments for why avoiding that allocation there matters.
struct transparent_string_hash {
    using is_transparent = void;
    std::size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
};

template<class V>
using string_view_lookup_map =
    std::unordered_map<std::string, V, transparent_string_hash, std::equal_to<>>;

// A tiny, linear-scan (name, value) list - an alternative to
// string_view_lookup_map<V> for a map that's small and built once at
// startup (a handful of attributes, matching a-tree-ffi's own "Vec, not
// HashMap - schemas are small" reasoning for the identical shape of
// problem), then looked up by string_view many times on the hot path. For
// that size, a hash computation's fixed per-call overhead outweighs a
// short linear scan comparing string_views directly - real, measured cost:
// profiling nats_sidecar's single-instance columnar-batch workload found
// betree_event_sink::index_for() (which does exactly this lookup once per
// (row, attribute) pair) at ~2.3% of total CPU, using
// string_view_lookup_map. Only used where that's been specifically
// measured to matter (betree_event_sink's own attribute-name-to-index
// map) - not a blanket replacement for string_view_lookup_map elsewhere.
template<class V>
class small_attr_map {
public:
    // Insert-or-update; matches string_view_lookup_map's operator[]=
    // construction-time usage shape closely enough without actually
    // needing operator[] itself (this class never needs "insert a
    // default-constructed V and hand back a mutable reference to it").
    void set(std::string name, V value) {
        for (auto& [n, v] : m_entries) {
            if (n == name) { v = std::move(value); return; }
        }
        m_entries.emplace_back(std::move(name), std::move(value));
    }

    // Returns nullptr on a miss, mirroring string_view_lookup_map::find()
    // returning end() - callers already null/end()-check either way.
    const V* find(std::string_view name) const {
        for (const auto& [n, v] : m_entries) {
            if (n == name) return &v;
        }
        return nullptr;
    }

    std::size_t size() const { return m_entries.size(); }

private:
    std::vector<std::pair<std::string, V>> m_entries;
};

// Engine-agnostic event sink. event_bridge populates one of these per
// incoming message instead of writing directly into a concrete
// atree::EventBuilder / be::Event. Attribute names are std::string_view,
// not std::string: both concrete engines' own builders already take
// std::string_view (atree::EventBuilder converts to an owned std::string
// only once, at the point it actually needs a null-terminated C string for
// the Rust FFI boundary - see atree.hpp), so requiring an owned std::string
// here would just be a second, redundant allocation on top of that one.
class event_sink {
public:
    virtual ~event_sink() = default;
    virtual void with_boolean(std::string_view name, bool value) = 0;
    virtual void with_integer(std::string_view name, int64_t value) = 0;
    virtual void with_float(std::string_view name, double value) = 0;
    virtual void with_string(std::string_view name, std::string_view value) = 0;
    virtual void with_string_list(std::string_view name, const std::vector<std::string>& values) = 0;
    virtual void with_integer_list(std::string_view name, const std::vector<int64_t>& values) = 0;
    virtual void with_undefined(std::string_view name) = 0;
};

// Engine-agnostic boolean-expression matching tree. Rebuilt fresh from
// scratch on every subscribe/unsubscribe (RCU snapshot pattern in
// subscription_manager) - not designed for incremental delete.
class matching_engine {
public:
    virtual ~matching_engine() = default;

    // Throws matching_engine_error on an invalid expression.
    virtual void insert(uint64_t id, const std::string& expression) = 0;

    // const: called concurrently by worker threads against a shared,
    // already-published (and therefore logically immutable) snapshot.
    virtual std::unique_ptr<event_sink> make_event() const = 0;

    // Matches a populated event (must have come from this same engine's
    // make_event()) against every inserted expression.
    virtual std::vector<uint64_t> search(event_sink& event) const = 0;

    // Whether an event_sink from make_event() may safely be reused for many
    // populate+search() cycles in a row (repopulating and re-searching the
    // *same* object) instead of needing a fresh one every time - true only
    // for an engine whose own event representation supports that safely.
    // a-tree does (see mrayva/a-tree's ATree::recycle_event(), which
    // atree_matching_engine::search() routes through below): reusing one
    // event_sink across a whole columnar batch instead of allocating a
    // fresh one per row is a real, separately-measured cost eliminated for
    // every row after the batch's first. be-tree's own event *container*
    // (the outer struct + variable-pointer array betree_make_event()
    // allocates) is also true for the same reason now -
    // betree_matching_engine::search() resets it via Event::clear(), which
    // is just betree_set_variable(event, index, nullptr) - the same
    // existing, already-exercised primitive with_undefined() already used,
    // not new/unverified code. What's still NOT reused: be-tree allocates a
    // fresh native *variable* object on every individual attribute set via
    // its underlying C library regardless of whether the event container
    // itself is fresh or recycled, and updating an already-set slot's
    // variable in place instead of allocating a new one would need new
    // be-tree C API this codebase doesn't have yet - a separate, riskier
    // lever, not attempted here. Defaults false for any other engine.
    virtual bool reuses_events() const { return false; }
};

// Builds a fresh matching_engine for the given engine type and attribute
// schema (no expressions inserted yet). Throws matching_engine_error if the
// engine type isn't available in this build.
std::unique_ptr<matching_engine> build_matching_engine(
    engine_type type, const std::vector<attribute_def>& attributes);

} // namespace sidecar
