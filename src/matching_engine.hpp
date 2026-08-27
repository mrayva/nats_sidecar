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
};

// Builds a fresh matching_engine for the given engine type and attribute
// schema (no expressions inserted yet). Throws matching_engine_error if the
// engine type isn't available in this build.
std::unique_ptr<matching_engine> build_matching_engine(
    engine_type type, const std::vector<attribute_def>& attributes);

} // namespace sidecar
