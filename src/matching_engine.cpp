#include "matching_engine.hpp"
#include "dialect.hpp"
#include "pstree_dialect.hpp"
#include <atree.hpp>
#include <betree_cpp.hpp>
// tree.h redirects to tree.hpp under __cplusplus (its own dispatch, see the file itself) -
// plain C++ declarations, not a C ABI, so no extern "C" wrapper here: free_sub() (used below,
// not exposed via betree_cpp.hpp/betree.h) is a normal C++-linkage global function.
#include <tree.h>
#include <pstree/pst_dynamic.hpp>

#include <algorithm>
#include <unordered_map>

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

// PS-Tree's own string encoding (order_key.hpp's StringCodec) is a FIXED-depth tree - one
// inner-node level per character position, sized by this constant - not a variable-length
// comparison like a-tree/be-tree's own string handling. A string longer than this is silently
// TRUNCATED at encode time (bytes past this length never affect which predicate space it
// falls into), so two distinct strings sharing this-many-byte prefix are indistinguishable to
// pstree. Chosen generously for typical attribute values (names, symbols, categories, ids);
// documented as a real, structural limitation in README.md, not a bug.
constexpr std::size_t kPstreeStringMaxLen = 128;
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
                       small_attr_map<std::size_t>& indices_out)
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
        indices_out.set(attr.name, idx++);
    }
    return tree;
}

class betree_event_sink : public event_sink {
public:
    betree_event_sink(be::Event event, const small_attr_map<std::size_t>& indices)
        : m_event(std::move(event)), m_indices(indices),
          m_touched(m_indices.size(), false), m_spares(m_indices.size(), nullptr),
          m_report(make_report()), m_undefined((m_indices.size() + 63) / 64, 0) {}

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
        free_report(m_report);
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

    // Persistent report/undefined-bitmap buffers for
    // betree_matching_engine::search() to pass into Tree::search_reusing(),
    // avoiding a fresh make_report()/make_undefined() allocation on every
    // single row - see search_reusing()'s own doc comment (betree_cpp.hpp).
    // `m_report` must be reset (see reset() below) before each reuse;
    // `m_undefined` needs no such reset, search_reusing() recomputes it in
    // place every call.
    struct report* report() { return m_report; }
    std::uint64_t* undefined_scratch() { return m_undefined.data(); }

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
        // Leaves m_report ready for the next search_reusing() call - see
        // report()'s own doc comment above for why this belongs here
        // (right after a search, not before the next one): reset() already
        // runs unconditionally after every search (see
        // betree_matching_engine::search() below), so this keeps that same
        // "always call reset() after search, sink is ready to reuse again"
        // invariant covering the report too, not just the event.
        betree_reset_report(m_report);
    }

private:
    std::size_t index_for(std::string_view name) const {
        const std::size_t* idx = m_indices.find(name);
        if (idx == nullptr) {
            throw matching_engine_error("unknown attribute: " + std::string(name));
        }
        return *idx;
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
    const small_attr_map<std::size_t>& m_indices;
    std::vector<bool> m_touched;
    std::vector<betree_variable*> m_spares;
    struct report* m_report;
    std::vector<std::uint64_t> m_undefined;
};

class betree_matching_engine : public matching_engine {
public:
    betree_matching_engine(be::Tree tree, small_attr_map<std::size_t> indices)
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
    // (the outer struct + variable-pointer array) for a reusing caller.
    // "Increment B" (betree_event_sink's own with_boolean/with_integer/
    // with_float, see their doc comments) separately covers the
    // per-attribute betree_make_*_variable() cost this method's own reset
    // doesn't touch.
    // search_reusing() (not search()): sink already owns a persistent
    // report/undefined-scratch pair (see betree_event_sink::report()/
    // undefined_scratch()'s own doc comments) sized once for this tree's
    // attribute count instead of allocated fresh on every row - "Increment
    // C" of the be-tree reuse work, covering the __libc_calloc costs
    // make_undefined()/make_report() showed up as in profiling, on top of
    // Increments A (event-container reuse) and B (per-variable reuse)
    // above.
    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<betree_event_sink&>(event);
        try {
            auto matched = m_tree.search_reusing(sink.native(), sink.report(), sink.undefined_scratch());
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
    small_attr_map<std::size_t> m_indices;
};

// list_valued attributes have no analog in PSTDynamic's model (an event attribute is always a
// single value there, never a list - see mrayva/pstree's README) - excluded from the schema
// entirely, not just from indexing: any expression that actually references one is rejected at
// translation time by ast_to_pstree_dnf() (pstree_dialect.cpp), which is the only place that
// needs to know they exist.
std::vector<pstree::AttrSchema> build_pstree_schema(const std::vector<attribute_def>& attributes) {
    std::vector<pstree::AttrSchema> schema;
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:
                schema.push_back({attr.name, pstree::ValueType::kBoolean});
                break;
            case attribute_type::integer:
                schema.push_back({attr.name, pstree::ValueType::kInteger});
                break;
            case attribute_type::float_val:
                schema.push_back({attr.name, pstree::ValueType::kFloat});
                break;
            case attribute_type::string:
                schema.push_back({attr.name, pstree::ValueType::kString, kPstreeStringMaxLen});
                break;
            case attribute_type::string_list:
            case attribute_type::integer_list:
                break;
        }
    }
    return schema;
}

// No incremental population API to speak of: PSTDynamic::Event is just a
// std::vector<EventPair> built fresh from each with_*() call, so unlike
// atree/betree there's no native builder object to wrap - this class
// itself IS the builder.
class pstree_event_sink : public event_sink {
public:
    void with_boolean(std::string_view name, bool value) override {
        m_event.push_back({std::string(name), pstree::Value(value)});
    }
    void with_integer(std::string_view name, int64_t value) override {
        m_event.push_back({std::string(name), pstree::Value(static_cast<std::int64_t>(value))});
    }
    void with_float(std::string_view name, double value) override {
        m_event.push_back({std::string(name), pstree::Value(value)});
    }
    void with_string(std::string_view name, std::string_view value) override {
        m_event.push_back({std::string(name), pstree::Value(std::string(value))});
    }
    void with_string_list(std::string_view name, const std::vector<std::string>&) override {
        throw matching_engine_error(
            "pstree does not support list-valued attributes: '" + std::string(name) + "'");
    }
    void with_integer_list(std::string_view name, const std::vector<int64_t>&) override {
        throw matching_engine_error(
            "pstree does not support list-valued attributes: '" + std::string(name) + "'");
    }
    // Deliberately a no-op, not a value push: PSTDynamic's own model treats "this attribute
    // isn't in the event" (pstree::findAttr() returning nullptr) as the one and only
    // representation of undefined/absent - there's no separate "present but undefined" value
    // to encode, so simply never adding the pair here already IS correct.
    void with_undefined(std::string_view) override {}

    pstree::Event& native() { return m_event; }
    void reset() { m_event.clear(); }

private:
    pstree::Event m_event;
};

class pstree_matching_engine : public matching_engine {
public:
    explicit pstree_matching_engine(std::vector<attribute_def> attributes)
        : m_parsing_tree(build_betree(attributes, m_unused_indices)),
          m_pstd(build_pstree_schema(attributes)) {}

    // Each inserted expression becomes one or more PSTDynamic subscriptions - one per DNF
    // clause (see ast_to_pstree_dnf()'s own doc comment for why OR/NOT need this at all,
    // PSTDynamic's own model being pure-conjunction) - sharing a fresh "clause id" mapped back
    // to the caller's own `id` in m_clause_to_sub, so search() can deduplicate a subscription
    // matched via more than one of its own OR'd clauses back down to a single result. Parsing
    // reuses be-tree's own already-linked, already-tested parser (betree_make_sub(), against
    // m_parsing_tree - a throwaway instance built purely to parse/type-check, never searched)
    // rather than writing a second parser for the same grammar - see pstree_dialect.hpp's own
    // doc comment for the full reasoning.
    void insert(uint64_t id, const std::string& expression) override {
        std::string translated;
        try {
            translated = translate_to_betree_dialect(expression);
        } catch (const std::exception& e) {
            throw matching_engine_error(e.what());
        }

        struct betree_sub* sub =
            betree_make_sub(m_parsing_tree.get(), id, 0, nullptr, translated.c_str());
        if (sub == nullptr) {
            throw matching_engine_error("pstree: invalid expression: " + expression);
        }
        std::vector<pstree_clause> dnf;
        try {
            dnf = ast_to_pstree_dnf(sub->expr);
        } catch (...) {
            free_sub(sub);
            throw;
        }
        free_sub(sub);

        std::vector<uint64_t> insertedClauseIds;
        try {
            for (auto& clause : dnf) {
                if (clause.empty()) {
                    // An unconditionally-true DNF clause (a literal `true`/`not false`
                    // somewhere in the expression) - PSTDynamic's own model has no way to
                    // represent "matches every event", only a conjunction of real
                    // predicates (insertSubscription() itself throws on an empty predicate
                    // list) - a real, structural limitation, not an omission.
                    throw matching_engine_error(
                        "pstree: expression '" + expression +
                        "' has an unconditionally-true clause (e.g. a literal 'true'), "
                        "which pstree cannot index - every clause needs at least one "
                        "real predicate");
                }
                uint64_t clauseId = m_next_clause_id++;
                m_pstd.insertSubscription(pstree::Subscription{clauseId, clause});
                insertedClauseIds.push_back(clauseId);
                m_clause_to_sub[clauseId] = id;
            }
        } catch (const matching_engine_error&) {
            for (auto clauseId : insertedClauseIds) {
                m_pstd.deleteSubscription(clauseId);
                m_clause_to_sub.erase(clauseId);
            }
            throw;
        } catch (const std::exception& e) {
            for (auto clauseId : insertedClauseIds) {
                m_pstd.deleteSubscription(clauseId);
                m_clause_to_sub.erase(clauseId);
            }
            throw matching_engine_error(e.what());
        }
    }

    std::unique_ptr<event_sink> make_event() const override {
        return std::make_unique<pstree_event_sink>();
    }

    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<pstree_event_sink&>(event);
        std::vector<uint64_t> result;
        try {
            auto clauseMatches = m_pstd.matchEvent(sink.native());
            for (auto clauseId : clauseMatches) {
                auto it = m_clause_to_sub.find(clauseId);
                uint64_t subId = (it != m_clause_to_sub.end()) ? it->second : clauseId;
                if (std::find(result.begin(), result.end(), subId) == result.end()) {
                    result.push_back(subId);
                }
            }
        } catch (const std::exception& e) {
            sink.reset();
            throw matching_engine_error(e.what());
        }
        sink.reset();
        return result;
    }

    // pstree_event_sink is a plain, freshly-constructed std::vector<EventPair> builder with no
    // native-engine allocation behind it worth pooling across rows (unlike a-tree/be-tree's own
    // native event objects) - reset() already just clears the vector in place, so there is
    // nothing this flag would unlock that reset() doesn't already give a caller regardless.
    bool reuses_events() const override { return true; }

private:
    // Declaration order matters here: members initialize in this order, and m_parsing_tree's
    // own initializer (below) passes m_unused_indices by reference into build_betree() - it
    // must already be constructed (an empty vector, ready to receive .set() calls) by then.
    small_attr_map<std::size_t> m_unused_indices;
    be::Tree m_parsing_tree;
    pstree::PSTDynamic m_pstd;
    std::unordered_map<uint64_t, uint64_t> m_clause_to_sub;
    uint64_t m_next_clause_id = 1;
};

} // namespace

std::unique_ptr<matching_engine> build_matching_engine(
    engine_type type, const std::vector<attribute_def>& attributes)
{
    switch (type) {
        case engine_type::atree:
            return std::make_unique<atree_matching_engine>(build_atree(attributes));
        case engine_type::betree: {
            small_attr_map<std::size_t> indices;
            auto tree = build_betree(attributes, indices);
            return std::make_unique<betree_matching_engine>(std::move(tree), std::move(indices));
        }
        case engine_type::pstree:
            return std::make_unique<pstree_matching_engine>(attributes);
    }
    throw matching_engine_error("unknown engine type");
}

} // namespace sidecar
