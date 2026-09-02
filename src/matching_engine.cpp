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

#include <unordered_map>
#include <unordered_set>

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
// kBetreeStringCount is a HARD cap, not a hint, despite betree_cpp.hpp's own add_string() doc
// comment calling it "Estimated number of unique string values (for optimization)": confirmed by
// reading be-tree's own get_id_for_string() (config.cpp) directly - it returns INVALID_STR (which
// then surfaces as "invalid expression" at insert()) the moment a (name, attribute) pair would be
// the (count+1)th DISTINCT string value ever seen for that attribute, across every subscription
// this Tree instance has ever parsed - "invalid expression" here does not mean the expression
// itself is malformed, only that this schema's string domain is full. Found via a real failure:
// the K=32000 exchange/symbol set-membership benchmark (symbols drawn from an 11,951-value real
// pool, up to 128 per subscription) hit this almost immediately with the old 1024 bound - only 25/
// 32000 subscribes succeeded. Bumping this costs nothing at runtime: get_id_for_string's own
// backing store (string_map, config.cpp's add_to_string_map) is a plain dynamically-growing
// hashmap, not an array preallocated to `count` slots - `count` is purely a logical ceiling, never
// a memory reservation. 65536 comfortably covers this project's real NYSE symbol universe (a
// single table's own distinct-symbol pool is in the thousands) with headroom, at zero extra cost.
constexpr std::size_t kBetreeStringCount = 65536;

// PS-Tree's own string handling used to be a FIXED-depth, one-inner-node-level-per-character
// tree (order_key.hpp's StringCodec), which silently TRUNCATED any string longer than a
// configured bound and paid a real, measured per-comparison cost proportional to that bound
// (this constant used to be 32, chosen specifically to bound that cost - see git history for
// the full measurement). pstree now INTERNS string attribute values into small integer ids at
// event-population/predicate-insert time instead (see pstree's own StringInternTable in
// pst_dynamic.hpp for why that's correctness-safe here specifically: this project's shared
// grammar, be-tree's parser.y, can never produce an ordering predicate against a string
// attribute, and interning only needs to preserve equality/set-membership, not order) - both
// the length-truncation limit and the per-comparison cost this constant used to bound are gone;
// pstree::AttrSchema's stringMaxLen field is now unused (kept only for source compatibility).
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
            case attribute_type::decimal:
                throw matching_engine_error(
                    "atree does not support decimal attributes: '" + attr.name + "'");
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
    void with_decimal(std::string_view name, const pstree::Int256&) override {
        throw matching_engine_error(
            "atree does not support decimal attributes: '" + std::string(name) + "'");
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
            case attribute_type::decimal:
                throw matching_engine_error(
                    "betree does not support decimal attributes: '" + attr.name + "'");
        }
        indices_out.set(attr.name, idx++);
    }
    return tree;
}

// pstree_matching_engine's own private be-tree instance exists PURELY to reuse be-tree's
// already-tested parser (see pstree_dialect.hpp's own top-of-file comment) - never searched,
// never matched against a real event. Unlike build_betree() above (which builds be-tree's REAL
// matching schema, and correctly throws on `decimal` since be-tree has no native decimal
// representation to match against), this variant treats a decimal-typed attribute as an
// ordinary be-tree float for PARSING PURPOSES ONLY - accepting the same int64/double literal
// syntax any other numeric attribute would (pstree_dialect.cpp's own literal-promotion step is
// what turns that parsed int64/double into a canonical-scale Int256 afterward). Deliberately a
// separate function, not build_betree() plus a bool flag - this codebase's own convention (see
// pstree::StringInternTable's internForInsert/lookupForSearch split) is a separate,
// purpose-named function over a flag parameter when two callers have genuinely different
// correctness requirements for the same case, not a cosmetic split.
be::Tree build_betree_for_pstree_parsing(const std::vector<attribute_def>& attributes,
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
            case attribute_type::decimal:
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
    void with_decimal(std::string_view name, const pstree::Int256&) override {
        throw matching_engine_error(
            "betree does not support decimal attributes: '" + std::string(name) + "'");
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
                schema.push_back({attr.name, pstree::ValueType::kBoolean, 0, nullptr});
                break;
            case attribute_type::integer:
                schema.push_back({attr.name, pstree::ValueType::kInteger, 0, nullptr});
                break;
            case attribute_type::float_val:
                schema.push_back({attr.name, pstree::ValueType::kFloat, 0, nullptr});
                break;
            case attribute_type::string:
                // stringMaxLen/stringIntern (last two fields) are unused/ignored by pstree since
                // string interning replaced StringCodec - see StringInternTable's own comment
                // (pst_dynamic.hpp) and matching_engine.cpp's own comment above this function.
                schema.push_back({attr.name, pstree::ValueType::kString, 0, nullptr});
                break;
            case attribute_type::decimal:
                // attr.decimal_scale is guaranteed present here - required and validated in
                // finalize_and_validate_config() (cli.cpp) whenever type == decimal.
                schema.push_back({attr.name, pstree::ValueType::kDecimal, 0, nullptr,
                                   *attr.decimal_scale});
                break;
            case attribute_type::string_list:
            case attribute_type::integer_list:
                break;
        }
    }
    return schema;
}

// Attribute name -> canonical decimal_scale, for whichever attributes are decimal-typed -
// pstree_dialect.cpp's ast_to_pstree_dnf() needs this to promote a be-tree-parsed int64/double
// literal into the target attribute's own canonical-scale pstree::Int256 (see that file's own
// doc comment). Built once at construction (see pstree_matching_engine's own member below),
// reused across every insert() call.
decimal_scale_map make_decimal_scales(const std::vector<attribute_def>& attributes) {
    decimal_scale_map out;
    for (const auto& attr : attributes) {
        if (attr.type == attribute_type::decimal && attr.decimal_scale) {
            out[attr.name] = *attr.decimal_scale;
        }
    }
    return out;
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
    void with_decimal(std::string_view name, const pstree::Int256& value) override {
        m_event.push_back({std::string(name), pstree::Value(value)});
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
        : m_parsing_tree(build_betree_for_pstree_parsing(attributes, m_unused_indices)),
          m_pstd(build_pstree_schema(attributes)),
          m_decimalScales(make_decimal_scales(attributes)) {}

    // Frees every `betree_sub*` insert() ever parsed against m_parsing_tree - see insert()'s
    // own comment for why this can't happen any earlier (a real, confirmed use-after-free found
    // via a live gdb backtrace under real subscribe load: be-tree's own config->pred_map
    // (hashmap.cpp's assign_pred/jsw_rbfind) stores RAW pointers into a sub's own AST predicate
    // nodes for cross-sub deduplication, and persists for m_parsing_tree's WHOLE lifetime, not
    // just one betree_make_sub() call - freeing a sub right after parsing it (the original code
    // here) leaves pred_map's own rbtree holding dangling pointers that a LATER insert() call's
    // own predicate comparison (bool_expr_cmp, called from jsw_rbfind) can dereference. Confirmed
    // safe to defer: jsw_rbdelete() (jsw_rbtree.cpp) only frees the rbtree's own node wrappers,
    // never the void* data they store, so pred_map's own destruction (inside m_parsing_tree's
    // destructor, which runs AFTER this one - C++ destroys the body first, then members in
    // reverse declaration order) never touches this memory - no double-free.
    ~pstree_matching_engine() override {
        for (struct betree_sub* sub : m_parsed_subs) {
            free_sub(sub);
        }
    }

    // Reserved top bit of the PSTDynamic-subscription-id space: SET means "this is a
    // synthetic clause id, look it up in m_clause_to_sub"; CLEAR means "this literal value
    // IS the caller's own subscription id" (see insert()'s own comment for when each path is
    // used, and search()'s for why the clear-bit path can skip translation AND dedup
    // entirely, not just translation).
    static constexpr uint64_t kSyntheticIdBit = uint64_t{1} << 63;

    // Each inserted expression becomes one or more PSTDynamic subscriptions - one per DNF
    // clause (see ast_to_pstree_dnf()'s own doc comment for why OR/NOT need this at all,
    // PSTDynamic's own model being pure-conjunction). A subscription with exactly ONE clause
    // (no OR in the expression - the common case; this project's own benchmark generates it
    // ~90% of the time) uses the caller's own `id` directly as the PSTDynamic subscription id,
    // skipping m_clause_to_sub entirely - safe because a single-clause subscription's own
    // bucket is visited AT MOST ONCE per event by construction (the canonical-decomposition
    // disjointness property PSTree's own tests already verify: a single predicate can never be
    // double-counted for one query point), so there is nothing to deduplicate either. Multi-
    // clause (OR'd) subscriptions, and any subscription whose caller-supplied `id` happens to
    // already have `kSyntheticIdBit` set (astronomically unlikely for a real registry, but
    // handled rather than assumed away), fall back to a fresh synthetic clause id mapped back
    // to `id` in m_clause_to_sub, exactly as before - search() still needs both translation and
    // dedup for these, since more than one of a subscription's own OR'd clauses can genuinely
    // match the same event. Found via `perf annotate` on matching_engine_bench: after the O(n^2)
    // dedup fix and PSTDynamic's own per-match unordered_map::at() fix (see both repos' READMEs),
    // these two remaining per-match hashmap operations were ~71% of search()'s own self-time at
    // scale - this removes both for the dominant case rather than just one.
    //
    // Parsing reuses be-tree's own already-linked, already-tested parser (betree_make_sub(),
    // against m_parsing_tree - a throwaway instance built purely to parse/type-check, never
    // searched) rather than writing a second parser for the same grammar - see
    // pstree_dialect.hpp's own doc comment for the full reasoning.
    //
    // `sub` is deliberately NOT freed here (on either the success or the dnf-extraction-failure
    // path) - see this class's own destructor for why an immediate free_sub() is a real,
    // confirmed use-after-free (be-tree's own config->pred_map retains raw pointers into a
    // sub's predicate AST nodes for cross-sub deduplication, for m_parsing_tree's whole
    // lifetime, not just this one call) and why deferring to the destructor is safe.
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
        m_parsed_subs.push_back(sub);
        std::vector<pstree_clause> dnf = ast_to_pstree_dnf(sub->expr, m_decimalScales);

        bool useDirectId = (dnf.size() == 1) && ((id & kSyntheticIdBit) == 0);
        if (!useDirectId) m_hasSyntheticClauses = true;

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
                uint64_t clauseId = useDirectId ? id : (kSyntheticIdBit | m_next_clause_id++);
                m_pstd.insertSubscription(pstree::Subscription{clauseId, clause});
                insertedClauseIds.push_back(clauseId);
                if (!useDirectId) m_clause_to_sub[clauseId] = id;
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
        try {
            auto clauseMatches = m_pstd.matchEvent(sink.native());
            // Fast path: if no subscription has EVER taken the synthetic-clause-id path (see
            // m_hasSyntheticClauses' own comment), every id in clauseMatches is guaranteed to be
            // a direct caller-supplied subscription id - the loop below would take the
            // `(clauseId & kSyntheticIdBit) == 0` branch on every single iteration, producing a
            // `result` that is byte-for-byte identical to `clauseMatches` itself (no translation,
            // no dedup - direct-id ids can appear at most once per event by construction, see
            // that branch's own comment). Skip building that redundant second vector entirely -
            // found via `perf report` on a live realistic-selectivity fleet trial (2026-09): once
            // PSTDynamic::matchEvent()'s own cost got cheap enough (Phase 3/4 fixes), this
            // function's copy loop became the SINGLE LARGEST internal contributor in the whole
            // sidecar (18.27% of total system time, more than matchEvent() itself at 14.86%) -
            // pure copy overhead, not real work, for the common (no-OR) subscription shape.
            if (!m_hasSyntheticClauses) {
                sink.reset();
                return clauseMatches;
            }
            std::vector<uint64_t> result;
            // Dedup via a hash set, not std::find over `result` - a linear scan here is
            // O(matches) per match, i.e. O(matches^2) per event overall. At scale (many
            // independent wide-range predicates matching a large fraction of subscriptions per
            // event - see nats_sidecar's own matching-engine benchmark/README), matches per
            // event can reach into the thousands, and the quadratic scan dominated total search
            // time: confirmed via `perf record`/`perf annotate` on matching_engine_bench at
            // K=20,000 - 66.8% of ALL search-phase self-time was in this function's own compare-
            // and-branch loop, not in PSTDynamic::matchEvent (7.1%) or anywhere else in pstree.
            //
            // `seen` MUST be thread_local, not a member of this object (an earlier version made
            // it a plain `mutable` member, m_seen_scratch - a real, confirmed data race: search()
            // is const and called concurrently by every worker thread against the SAME engine
            // instance under only a shared_lock (subscription_manager::acquire_tree() - readers
            // are never serialized against each other, only against writers), so two threads
            // mutating one std::unordered_set at once corrupted its internal buckets and
            // segfaulted - reproduced with worker_threads>1 plus any OR'd/multi-clause
            // subscription (the only shape that reaches this insert() call at all; single-clause
            // subscriptions take the direct-id `continue` below and never touch this set). This
            // is the exact class of race pstree's own PSTDynamic::matchEvent() already documents
            // avoiding for the identical reason (see its own doc comment, and pstree commit
            // c7704e9's SubPredicate fix) - that lesson just hadn't reached this wrapper's own
            // scratch-buffer optimization, added independently and earlier. `static thread_local`
            // keeps the original point of being a member at all (avoid a fresh heap allocation
            // every event - the buckets persist across calls on the same thread) without ever
            // sharing the set across threads.
            static thread_local std::unordered_set<uint64_t> seen;
            seen.clear();
            result.reserve(clauseMatches.size());
            for (auto clauseId : clauseMatches) {
                if ((clauseId & kSyntheticIdBit) == 0) {
                    // Direct-id path (see insert()'s own comment): this literal value already
                    // IS the caller's subscription id, and by construction (single-clause,
                    // canonical-decomposition disjointness) can appear at most once in
                    // clauseMatches for this event - skip both the translation lookup and the
                    // dedup check, not just one of them.
                    result.push_back(clauseId);
                    continue;
                }
                auto it = m_clause_to_sub.find(clauseId);
                uint64_t subId = (it != m_clause_to_sub.end()) ? it->second : clauseId;
                if (seen.insert(subId).second) {
                    result.push_back(subId);
                }
            }
            sink.reset();
            return result;
        } catch (const std::exception& e) {
            sink.reset();
            throw matching_engine_error(e.what());
        }
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
    // Every betree_sub* insert() has ever parsed against m_parsing_tree, freed only in this
    // class's own destructor - see both the destructor's and insert()'s own comments for why
    // freeing any earlier is a real, confirmed use-after-free.
    std::vector<struct betree_sub*> m_parsed_subs;
    pstree::PSTDynamic m_pstd;
    // Built once from the constructor's own `attributes` (make_decimal_scales) - insert()'s own
    // ast_to_pstree_dnf() call needs this to promote a be-tree-parsed literal into the target
    // attribute's canonical-scale pstree::Int256, see pstree_dialect.hpp's own doc comment.
    // Declared here (not earlier) so its position matches the constructor's own initializer-list
    // order (member init order always follows DECLARATION order, not initializer-list order -
    // see pstree's own commit history for a real -Wreorder bug caught from exactly this
    // mismatch previously).
    decimal_scale_map m_decimalScales;
    std::unordered_map<uint64_t, uint64_t> m_clause_to_sub;
    uint64_t m_next_clause_id = 1;
    // The dedup scratch set used to live here as a `mutable` member - moved to a function-local
    // `static thread_local` inside search() itself, see that function's own comment for why
    // (a real, confirmed data race under concurrent worker threads).
    //
    // True once ANY subscription has ever taken the synthetic-clause-id path in insert() (an
    // OR'd/multi-clause expression, or a caller id that already happened to have kSyntheticIdBit
    // set). Deliberately monotonic - set true, never reset to false on delete: search() only
    // needs "no clause has EVER used the synthetic path" to safely skip its own translate/dedup
    // loop (see search()'s own comment) - leaving this true after every synthetic-clause
    // subscription is later deleted just forgoes that fast path unnecessarily, never a
    // correctness risk. Protected by the same external lock discipline m_clause_to_sub/
    // m_next_clause_id already rely on (subscription_manager's unique_lock around insert(),
    // shared_lock around search()) - no separate synchronization needed for a plain bool here.
    bool m_hasSyntheticClauses = false;
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
