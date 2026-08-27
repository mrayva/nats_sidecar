#include "matching_engine.hpp"
#include "dialect.hpp"
#include <atree.hpp>
#include <betree_cpp.hpp>

namespace sidecar {

// Generous, fixed bounds for be-tree's domain-partitioned numeric/string
// schema. a-tree's own schema carries no such bounds, so attribute_def has
// nothing narrower to offer - wide enough to cover any real attribute value,
// at the cost of forgoing partitioning benefit a tighter bound would give.
namespace {
constexpr int64_t kBetreeIntMin = -1'000'000'000LL;
constexpr int64_t kBetreeIntMax = 1'000'000'000LL;
constexpr double kBetreeFloatMin = -1e9;
constexpr double kBetreeFloatMax = 1e9;
constexpr std::size_t kBetreeStringCount = 1024;
}

namespace {

atree::Tree build_atree(const std::vector<attribute_def>& attributes) {
    auto builder = atree::Tree::builder();
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:      builder.with_boolean(attr.name); break;
            case attribute_type::integer:      builder.with_integer(attr.name); break;
            case attribute_type::float_val:    builder.with_float(attr.name); break;
            case attribute_type::string:       builder.with_string(attr.name); break;
            case attribute_type::string_list:  builder.with_string_list(attr.name); break;
            case attribute_type::integer_list: builder.with_integer_list(attr.name); break;
        }
    }
    return std::move(builder).build();
}

class atree_event_sink : public event_sink {
public:
    explicit atree_event_sink(atree::EventBuilder builder) : m_builder(std::move(builder)) {}

    void with_boolean(std::string_view name, bool value) override {
        m_builder.with_boolean(name, value);
    }
    void with_integer(std::string_view name, int64_t value) override {
        m_builder.with_integer(name, value);
    }
    void with_float(std::string_view name, double value) override {
        m_builder.with_float(name, value);
    }
    void with_string(std::string_view name, std::string_view value) override {
        m_builder.with_string(name, value);
    }
    void with_string_list(std::string_view name, const std::vector<std::string>& values) override {
        m_builder.with_string_list(name, values);
    }
    void with_integer_list(std::string_view name, const std::vector<int64_t>& values) override {
        m_builder.with_integer_list(name, values);
    }
    void with_undefined(std::string_view name) override {
        m_builder.with_undefined(name);
    }

    atree::EventBuilder& native() { return m_builder; }

private:
    atree::EventBuilder m_builder;
};

class atree_matching_engine : public matching_engine {
public:
    explicit atree_matching_engine(atree::Tree tree) : m_tree(std::move(tree)) {}

    void insert(uint64_t id, const std::string& expression) override {
        try {
            m_tree.insert(id, translate_to_atree_dialect(expression));
        } catch (const atree::Error& e) {
            throw matching_engine_error(e.what());
        }
    }

    std::unique_ptr<event_sink> make_event() const override {
        return std::make_unique<atree_event_sink>(m_tree.make_event());
    }

    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<atree_event_sink&>(event);
        try {
            // search_reusing() (not search()) unconditionally: it never
            // frees `sink`'s underlying builder itself - ownership stays
            // with `sink` (an atree_event_sink, via its atree::EventBuilder
            // member's own RAII destructor) either way, so this is exactly
            // as correct for a one-shot event_sink (used for a single row,
            // then destroyed - its destructor frees the builder once, same
            // as before) as for one reused across many rows (see
            // reuses_events() below) - and only the latter actually
            // benefits from the recycling search_reusing() enables.
            return m_tree.search_reusing(sink.native());
        } catch (const atree::Error& e) {
            throw matching_engine_error(e.what());
        }
    }

    bool reuses_events() const override { return true; }

private:
    atree::Tree m_tree;
};

// be::Event's set_* methods are index-based, not name-based (unlike
// atree::EventBuilder) - indices are assigned in schema-declaration order
// and looked up by name here to keep event_sink's interface uniform.
be::Tree build_betree(const std::vector<attribute_def>& attributes,
                       string_view_lookup_map<std::size_t>& indices_out)
{
    be::Tree tree;
    std::size_t idx = 0;
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:
                tree.add_boolean(attr.name, true);
                break;
            case attribute_type::integer:
                tree.add_integer(attr.name, true, kBetreeIntMin, kBetreeIntMax);
                break;
            case attribute_type::float_val:
                tree.add_float(attr.name, true, kBetreeFloatMin, kBetreeFloatMax);
                break;
            case attribute_type::string:
                tree.add_string(attr.name, true, kBetreeStringCount);
                break;
            case attribute_type::string_list:
                tree.add_string_list(attr.name, true, kBetreeStringCount);
                break;
            case attribute_type::integer_list:
                tree.add_integer_list(attr.name, true, kBetreeIntMin, kBetreeIntMax);
                break;
        }
        indices_out[attr.name] = idx++;
    }
    return tree;
}

class betree_event_sink : public event_sink {
public:
    betree_event_sink(be::Event event, const string_view_lookup_map<std::size_t>& indices)
        : m_event(std::move(event)), m_indices(indices),
          m_touched(m_indices.size(), false), m_spares(m_indices.size(), nullptr) {}

    // m_spares may hold detached-but-not-freed betree_variable objects
    // (see reset()'s own comment) that never made it back into m_event's
    // own variables[] array by the time this sink is destroyed -
    // betree_free_event() (invoked when m_event's destructor runs) only
    // walks what's *currently attached*, so anything still sitting in the
    // pool needs its own explicit free here, or it leaks.
    ~betree_event_sink() override {
        for (betree_variable* spare : m_spares) {
            if (spare != nullptr) betree_free_variable(spare);
        }
    }
    betree_event_sink(const betree_event_sink&) = delete;
    betree_event_sink& operator=(const betree_event_sink&) = delete;

    // "Increment B": bool/integer/float reuse an existing betree_variable
    // from a prior row via betree_update_*_variable() (a plain field
    // write - see that function's own doc comment in betree.h) instead of
    // paying betree_make_*_variable()'s allocation on every single call,
    // when reset() (below) left a spare for this exact slot from a
    // previous row. First use of a slot (no spare yet) falls back to the
    // existing, already-correct allocate-and-attach path unchanged.
    // string/string_list/integer_list are NOT covered - those values hold
    // their own nested heap allocations (a bstrdup'd string, a
    // dynamically-sized array) where "update in place" would need real
    // size-comparison/reallocation logic, a separate and harder problem,
    // not attempted here.
    void with_boolean(std::string_view name, bool value) override {
        std::size_t idx = index_for(name);
        if (betree_variable* spare = take_spare(idx)) {
            betree_update_boolean_variable(spare, value);
            reattach(idx, spare);
        } else {
            m_event.set_boolean(idx, value);
        }
        m_touched[idx] = true;
    }
    void with_integer(std::string_view name, int64_t value) override {
        std::size_t idx = index_for(name);
        if (betree_variable* spare = take_spare(idx)) {
            betree_update_integer_variable(spare, value);
            reattach(idx, spare);
        } else {
            m_event.set_integer(idx, value);
        }
        m_touched[idx] = true;
    }
    void with_float(std::string_view name, double value) override {
        std::size_t idx = index_for(name);
        if (betree_variable* spare = take_spare(idx)) {
            betree_update_float_variable(spare, value);
            reattach(idx, spare);
        } else {
            m_event.set_float(idx, value);
        }
        m_touched[idx] = true;
    }
    void with_string(std::string_view name, std::string_view value) override {
        std::size_t idx = index_for(name);
        m_event.set_string(idx, value);
        m_touched[idx] = true;
    }
    void with_string_list(std::string_view name, const std::vector<std::string>& values) override {
        std::size_t idx = index_for(name);
        std::vector<std::string_view> views(values.begin(), values.end());
        m_event.set_string_list(idx, views);
        m_touched[idx] = true;
    }
    void with_integer_list(std::string_view name, const std::vector<int64_t>& values) override {
        std::size_t idx = index_for(name);
        m_event.set_integer_list(idx, values);
        m_touched[idx] = true;
    }
    void with_undefined(std::string_view name) override {
        // Deliberately NOT marked touched, and deliberately still goes
        // through Event::clear() (free, not pool) rather than the spare
        // path - this is the "explicitly told to be undefined" case
        // (event_bridge.hpp's populate_event() only calls it when
        // extracting a field's value threw), not the hot common-attribute
        // path Increment B targets. reset() clearing an untouched slot
        // gives the exact same end state anyway, so leaving this alone is
        // correct, not just convenient.
        m_event.clear(index_for(name));
    }

    be::Event& native() { return m_event; }

    // Clears every attribute slot this row didn't touch back to unset (so
    // a row with fewer attributes than a previous one can never see that
    // previous row's leftover value - see reuses_events()'s own doc
    // comment, matching_engine.hpp), while DETACHING (not freeing) the
    // ones it did touch into m_spares for with_boolean/with_integer/
    // with_float to reuse next row via betree_update_*_variable() instead
    // of reallocating. Detaching means writing event->variables[i]
    // directly instead of calling betree_set_variable(event, i, nullptr)
    // (which is what Event::clear() does, and which would free the
    // variable, defeating the whole point) - safe because betree_event's
    // fields are a plain public struct, and because after this write the
    // slot is genuinely undefined for search purposes (be-tree's regular
    // search path treats a null variables[i] as unknown/undefined, exactly
    // the state we want), it's just that this codebase, not be-tree's own
    // free_event(), is now responsible for eventually freeing that
    // detached pointer - which the destructor above does for anything
    // still in m_spares when this sink goes away.
    void reset() {
        betree_event* raw = m_event.get();
        for (std::size_t i = 0; i < m_indices.size(); ++i) {
            if (m_touched[i]) {
                betree_variable* v = raw->variables[i];
                raw->variables[i] = nullptr;
                if (m_spares[i] != nullptr) {
                    // Should never happen (take_spare() always empties a
                    // slot before it's used again) - free defensively
                    // rather than leak if this invariant is ever violated.
                    betree_free_variable(m_spares[i]);
                }
                m_spares[i] = v;
            } else {
                m_event.clear(i);
            }
            m_touched[i] = false;
        }
    }

private:
    std::size_t index_for(std::string_view name) const {
        auto it = m_indices.find(name);
        if (it == m_indices.end()) {
            throw matching_engine_error("unknown attribute: " + std::string(name));
        }
        return it->second;
    }

    betree_variable* take_spare(std::size_t idx) {
        betree_variable* v = m_spares[idx];
        m_spares[idx] = nullptr;
        return v;
    }

    // Reattaches a variable pulled from m_spares[idx] back into the event
    // at the SAME index it was detached from, without going through
    // betree_set_variable() - that function would also re-derive and
    // bstrdup() the attribute name/id into the variable on every call
    // (see its own comment in betree.cpp), which is redundant work here:
    // this variable's attr_var was already resolved correctly for this
    // exact index when it was first created, and never needs to change
    // since a spare is only ever reattached to the slot it came from. Safe
    // for the same reason reset()'s detach is: betree_event's fields are a
    // plain public struct, and the slot is guaranteed null right now (this
    // is only called immediately after take_spare(), never otherwise), so
    // there is nothing to free on the way in either.
    void reattach(std::size_t idx, betree_variable* variable) {
        m_event.get()->variables[idx] = variable;
    }

    be::Event m_event;
    const string_view_lookup_map<std::size_t>& m_indices;
    std::vector<bool> m_touched;
    std::vector<betree_variable*> m_spares;
};

class betree_matching_engine : public matching_engine {
public:
    betree_matching_engine(be::Tree tree, string_view_lookup_map<std::size_t> indices)
        : m_tree(std::move(tree)), m_indices(std::move(indices)) {}

    void insert(uint64_t id, const std::string& expression) override {
        std::string rewritten;
        try {
            rewritten = translate_to_betree_dialect(expression);
        } catch (const std::exception& e) {
            throw matching_engine_error(e.what());
        }
        if (!m_tree.insert(id, rewritten)) {
            throw matching_engine_error("invalid expression: " + expression);
        }
    }

    std::unique_ptr<event_sink> make_event() const override {
        return std::make_unique<betree_event_sink>(m_tree.make_event(), m_indices);
    }

    // Increment A of the be-tree reuse work (see matching_engine.hpp's
    // reuses_events() doc comment): resets `sink` right after every search
    // (success or failure) instead of leaving that to the caller, so a
    // caller that keeps reusing the same event_sink across many rows (see
    // reuses_events() below) - and only the latter actually
    // needs it - gets an event that's always "blank" and ready for the next
    // row's with_*() calls, exactly as if make_event() had just been called
    // again. This eliminates betree_make_event()'s own per-row allocation
    // (the outer struct + variable-pointer array) for a reusing caller;
    // it does NOT touch the per-attribute betree_make_*_variable() cost
    // (still paid on every with_*() call regardless) - that's the separate,
    // riskier "Increment B" lever (would need a new be-tree C API to update
    // an existing variable's value in place instead of allocating a fresh
    // one), not attempted here.
    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<betree_event_sink&>(event);
        try {
            auto matched = m_tree.search(sink.native()).matched_subs;
            sink.reset();
            return matched;
        } catch (const std::exception& e) {
            sink.reset();
            throw matching_engine_error(e.what());
        }
    }

    // be-tree's Event::clear() (called by betree_event_sink::reset() above)
    // is an existing, already-exercised be-tree primitive - just
    // betree_set_variable(event, index, nullptr), the same call every
    // with_undefined() already makes - not new, unverified C code, so
    // reusing the event *container* this way carries none of the
    // per-attribute-variable memory-safety uncertainty this method's
    // default (false) exists to avoid. Only the outer event allocation
    // (betree_make_event()'s bmalloc+bcalloc) is avoided by this - see
    // search()'s own comment for what's still NOT covered.
    bool reuses_events() const override { return true; }

private:
    be::Tree m_tree;
    string_view_lookup_map<std::size_t> m_indices;
};

} // namespace

std::unique_ptr<matching_engine> build_matching_engine(
    engine_type type, const std::vector<attribute_def>& attributes)
{
    switch (type) {
        case engine_type::atree:
            return std::make_unique<atree_matching_engine>(build_atree(attributes));
        case engine_type::betree: {
            string_view_lookup_map<std::size_t> indices;
            auto tree = build_betree(attributes, indices);
            return std::make_unique<betree_matching_engine>(std::move(tree), std::move(indices));
        }
    }
    throw matching_engine_error("unknown engine type");
}

} // namespace sidecar
