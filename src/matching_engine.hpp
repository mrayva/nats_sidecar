#pragma once

#include "config.hpp"
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sidecar {

// Thrown by matching_engine::insert() on an invalid expression. Wraps
// whichever concrete engine's own exception type (atree::Error, be-tree's
// BetreeException) behind one type callers need to catch.
class matching_engine_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Engine-agnostic event sink. event_bridge populates one of these per
// incoming message instead of writing directly into a concrete
// atree::EventBuilder / be::Event.
class event_sink {
public:
    virtual ~event_sink() = default;
    virtual void with_boolean(const std::string& name, bool value) = 0;
    virtual void with_integer(const std::string& name, int64_t value) = 0;
    virtual void with_float(const std::string& name, double value) = 0;
    virtual void with_string(const std::string& name, std::string_view value) = 0;
    virtual void with_string_list(const std::string& name, const std::vector<std::string>& values) = 0;
    virtual void with_integer_list(const std::string& name, const std::vector<int64_t>& values) = 0;
    virtual void with_undefined(const std::string& name) = 0;
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
