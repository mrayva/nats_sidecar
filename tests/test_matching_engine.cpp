#include "matching_engine.hpp"
#include <gtest/gtest.h>
#include <algorithm>

namespace {

std::vector<sidecar::attribute_def> trade_attributes() {
    return {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
        {"active",       sidecar::attribute_type::boolean},
        {"tags",         sidecar::attribute_type::string_list},
    };
}

struct trade_event {
    double price;
    int64_t volume;
    std::string symbol;
    bool active;
    std::vector<std::string> tags = {};
};

std::vector<uint64_t> match(sidecar::matching_engine& engine, const trade_event& ev) {
    auto sink = engine.make_event();
    sink->with_float("trade_price", ev.price);
    sink->with_integer("trade_volume", ev.volume);
    sink->with_string("symbol", ev.symbol);
    sink->with_boolean("active", ev.active);
    sink->with_string_list("tags", ev.tags);
    return engine.search(*sink);
}

bool contains(const std::vector<uint64_t>& ids, uint64_t id) {
    return std::find(ids.begin(), ids.end(), id) != ids.end();
}

class matching_engine_test : public ::testing::TestWithParam<sidecar::engine_type> {};

std::string engine_name(sidecar::engine_type t) {
    return t == sidecar::engine_type::atree ? "atree" : "betree";
}

} // namespace

TEST_P(matching_engine_test, simple_comparison_matches) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_price > 100.0");

    EXPECT_TRUE(contains(match(*engine, {150.0, 100, "AAPL", true}), 1));
    EXPECT_FALSE(contains(match(*engine, {50.0, 100, "AAPL", true}), 1));
}

// Runs against both engines - for a-tree this re-confirms existing
// behavior (reuses_events() already true), for be-tree this is the new
// case this test exists for (matching_engine::reuses_events()'s
// "Increment A" - see its own doc comment): a caller that keeps reusing
// the same event_sink across many searches (matching how
// event_bridge.cpp's match_columnar_batch() actually uses it for a
// columnar batch's rows) instead of a fresh one every time. The risk:
// a value set on one search must not still be visible on the next one if
// it isn't set again - checked here with a sink reused across three
// searches, the second deliberately setting *fewer* attributes than the
// first (mirroring a batch where not every row sets every attribute),
// against expressions chosen so a leaked stale value would flip the
// answer rather than coincidentally agree with the correct one.
TEST_P(matching_engine_test, event_sink_reused_across_searches_has_no_stale_values) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_volume > 1000");
    engine->insert(2, "tags one of (\"urgent\")");

    auto sink = engine->make_event();

    // First search: sets every attribute, including volume and tags -
    // both expressions match.
    sink->with_float("trade_price", 10.0);
    sink->with_integer("trade_volume", 2000);
    sink->with_string("symbol", "AAPL");
    sink->with_boolean("active", true);
    sink->with_string_list("tags", {"urgent"});
    auto r1 = engine->search(*sink);
    EXPECT_TRUE(contains(r1, 1));
    EXPECT_TRUE(contains(r1, 2));

    // Second search, the SAME sink object: only sets price this time -
    // trade_volume and tags are left unset. If either leaked from the
    // first search, this would incorrectly still match.
    sink->with_float("trade_price", 20.0);
    auto r2 = engine->search(*sink);
    EXPECT_FALSE(contains(r2, 1))
        << "trade_volume leaked from a prior search on a reused event_sink";
    EXPECT_FALSE(contains(r2, 2))
        << "tags leaked from a prior search on a reused event_sink";

    // Third search, same sink again: set volume back above the threshold -
    // proves the sink is genuinely reusable more than once, not just
    // resettable a single time.
    sink->with_integer("trade_volume", 5000);
    auto r3 = engine->search(*sink);
    EXPECT_TRUE(contains(r3, 1));
    EXPECT_FALSE(contains(r3, 2));
}

// be-tree's own reuse of scalar (bool/int/float) event attributes across
// searches ("Increment B" - see betree_event_sink's own doc comments in
// matching_engine.cpp) keeps a per-slot pool of detached-but-not-freed
// betree_variable objects, updated in place via betree_update_*_variable()
// instead of reallocated, whenever a slot is touched again after being
// left untouched. That pool logic - detach into the pool, pull back out,
// update, reattach, or leave detached and eventually free at destruction -
// is exactly the kind of manual C-level memory lifecycle code most likely
// to hide a leak, double-free, or stale-read bug, so this specifically
// hammers it: 12 searches on one reused sink, alternately touching and
// NOT touching each of the three scalar attributes in a staggered pattern
// (so every attribute cycles through touched->untouched->touched several
// times, not just once), checked against expressions that would give a
// different, easily-distinguishable answer if either a value leaked
// through when it shouldn't have, or failed to actually update when it
// should have. Run under ASan+UBSan (build-sanitizer/) as well as
// normally - a functional pass here doesn't rule out a leak or a
// use-after-free that just hasn't manifested as a wrong answer yet.
TEST_P(matching_engine_test, event_sink_reused_across_many_cycles_with_staggered_attributes) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_volume > 1000");
    engine->insert(3, "active");

    auto sink = engine->make_event();

    for (int cycle = 0; cycle < 12; ++cycle) {
        const bool touch_price = (cycle % 2) == 0;
        const bool touch_volume = (cycle % 3) == 0;
        const bool touch_active = (cycle % 4) == 0;

        if (touch_price) sink->with_float("trade_price", 200.0);
        if (touch_volume) sink->with_integer("trade_volume", 2000);
        if (touch_active) sink->with_boolean("active", true);

        auto r = engine->search(*sink);
        EXPECT_EQ(contains(r, 1), touch_price)
            << "cycle " << cycle << ": trade_price stale-or-missing";
        EXPECT_EQ(contains(r, 2), touch_volume)
            << "cycle " << cycle << ": trade_volume stale-or-missing";
        EXPECT_EQ(contains(r, 3), touch_active)
            << "cycle " << cycle << ": active stale-or-missing";
    }
}

TEST_P(matching_engine_test, and_or_combinators) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_price > 50.0 and trade_volume > 1000");
    engine->insert(2, "trade_price > 10000.0 or symbol = \"COIN\"");

    auto r1 = match(*engine, {60.0, 2000, "AAPL", true});
    EXPECT_TRUE(contains(r1, 1));
    EXPECT_FALSE(contains(r1, 2));

    auto r2 = match(*engine, {1.0, 1, "COIN", false});
    EXPECT_FALSE(contains(r2, 1));
    EXPECT_TRUE(contains(r2, 2));
}

TEST_P(matching_engine_test, not_and_boolean_truthiness) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "not (trade_price > 100.0)");
    engine->insert(2, "active and trade_price > 100.0");

    auto r = match(*engine, {150.0, 100, "AAPL", true});
    EXPECT_FALSE(contains(r, 1));
    EXPECT_TRUE(contains(r, 2));

    auto r2 = match(*engine, {50.0, 100, "AAPL", true});
    EXPECT_TRUE(contains(r2, 1));
    EXPECT_FALSE(contains(r2, 2));
}

TEST_P(matching_engine_test, string_equality_and_inequality) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "symbol = \"AAPL\"");
    engine->insert(2, "symbol <> \"AAPL\"");

    auto r = match(*engine, {1.0, 1, "AAPL", true});
    EXPECT_TRUE(contains(r, 1));
    EXPECT_FALSE(contains(r, 2));

    auto r2 = match(*engine, {1.0, 1, "MSFT", true});
    EXPECT_FALSE(contains(r2, 1));
    EXPECT_TRUE(contains(r2, 2));
}

TEST_P(matching_engine_test, invalid_expression_throws_matching_engine_error) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    EXPECT_THROW(engine->insert(1, "this is not valid !!!"), sidecar::matching_engine_error);
}

TEST_P(matching_engine_test, space_separated_keywords_work_natively_on_both_engines) {
    // a-tree and be-tree agree on this spelling verbatim (confirmed against
    // both engines' real lexers) - no dialect translation is involved here.
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "symbol is not null");
    engine->insert(2, "trade_volume not in (1, 2, 3)");

    auto r = match(*engine, {150.0, 100, "AAPL", true});
    EXPECT_TRUE(contains(r, 1));
    EXPECT_TRUE(contains(r, 2));
}

INSTANTIATE_TEST_SUITE_P(
    atree_and_betree, matching_engine_test,
    ::testing::Values(sidecar::engine_type::atree, sidecar::engine_type::betree),
    [](const ::testing::TestParamInfo<sidecar::engine_type>& info) {
        return engine_name(info.param);
    });

TEST(matching_engine, betree_rejects_is_not_empty) {
    // is_empty/is_not_empty apply to list-typed attributes, not scalars.
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::betree, trade_attributes());
    EXPECT_THROW(engine->insert(1, "tags is not empty"), sidecar::matching_engine_error);
}

// pstree isn't included in the atree_and_betree parameterized suite above: every one of that
// suite's fixtures unconditionally populates the "tags" (string_list) attribute via match()'s
// shared with_string_list() call, even for expressions that never reference it - and
// pstree_event_sink::with_string_list() throws unconditionally (pstree has no representation
// for list-valued attributes at all, see matching_engine.cpp/pstree_dialect.hpp). Rather than
// restructure that shared fixture around a limitation specific to one engine, pstree gets its
// own narrower, targeted checks here instead - the systematic operator/type coverage it does
// share with a-tree/be-tree already lives in test_matching_engine_differential.cpp, whose
// per-field-optional event fixture doesn't have this problem.
TEST(matching_engine, pstree_invalid_expression_throws_matching_engine_error) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    EXPECT_THROW(engine->insert(1, "this is not valid !!!"), sidecar::matching_engine_error);
}

TEST(matching_engine, pstree_rejects_list_valued_attribute_reference) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    EXPECT_THROW(engine->insert(1, "tags one of (\"urgent\")"), sidecar::matching_engine_error);
}

// Exercises pstree_matching_engine::insert()'s rollback path: "trade_price > 10.0 or true"
// expands (ast_to_pstree_dnf) to two DNF clauses - a real, indexable one and a second,
// unconditionally-true EMPTY clause pstree can't represent at all (see
// pstree_dialect.cpp/pst_dynamic.hpp's own comments on this). The first clause is inserted
// into PSTDynamic successfully before the second is found to be un-insertable and the whole
// insert() call throws - the already-inserted first clause must be rolled back
// (PSTDynamic::deleteSubscription()), or a later, unrelated event could spuriously match a
// subscription that was supposed to have been entirely rejected.
TEST(matching_engine, pstree_partial_dnf_insert_failure_rolls_back_cleanly) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    EXPECT_THROW(engine->insert(1, "trade_price > 10.0 or true"), sidecar::matching_engine_error);

    // The rejected subscription must leave no trace: an event that would satisfy the
    // real half of the rejected expression (trade_price > 10.0) must not match id 1.
    auto sink = engine->make_event();
    sink->with_float("trade_price", 100.0);
    sink->with_integer("trade_volume", 1);
    sink->with_string("symbol", "AAPL");
    sink->with_boolean("active", true);
    auto r = engine->search(*sink);
    EXPECT_FALSE(contains(r, 1));

    // The engine must still be usable afterward - a later, unrelated insert should succeed
    // and match normally, confirming the rollback didn't leave PSTDynamic's own internal
    // state (dimension trees, leaf groups, clause-id bookkeeping) corrupted. Re-populate
    // `sink` first: search() already reset it (pstree_event_sink::reset() clears the event
    // vector outright, unlike a-tree/be-tree's "ready to reuse" semantics).
    engine->insert(2, "trade_price > 10.0");
    sink->with_float("trade_price", 100.0);
    sink->with_integer("trade_volume", 1);
    sink->with_string("symbol", "AAPL");
    sink->with_boolean("active", true);
    auto r2 = engine->search(*sink);
    EXPECT_TRUE(contains(r2, 2));
    EXPECT_FALSE(contains(r2, 1));
}

TEST(matching_engine, pstree_and_or_not_combinators_and_reuse) {
    // Exercises the OR-into-DNF-clauses path (ast_to_pstree_dnf) end to end, including a
    // negated AND (De Morgan pushdown) and a reused event_sink across two searches - the same
    // shape as matching_engine_test's own and_or_combinators/not_and_boolean_truthiness/
    // event_sink_reused_across_searches cases above, just without touching "tags".
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    engine->insert(1, "trade_price > 50.0 and trade_volume > 1000");
    engine->insert(2, "trade_price > 10000.0 or symbol = \"COIN\"");
    engine->insert(3, "not (trade_price > 100.0 and active)");

    auto sink = engine->make_event();
    sink->with_float("trade_price", 150.0);
    sink->with_integer("trade_volume", 2000);
    sink->with_string("symbol", "AAPL");
    sink->with_boolean("active", true);
    auto r1 = engine->search(*sink);
    EXPECT_TRUE(contains(r1, 1));
    EXPECT_FALSE(contains(r1, 2));
    EXPECT_FALSE(contains(r1, 3)) // not(150.0 > 100.0 and active=true) = not(true) = false
        << "sub 3 (\"not (trade_price > 100.0 and active)\") should not match: "
           "both inner conjuncts are true, so the negation is false";

    sink->with_float("trade_price", 1.0);
    sink->with_integer("trade_volume", 1);
    sink->with_string("symbol", "COIN");
    sink->with_boolean("active", false);
    auto r2 = engine->search(*sink);
    EXPECT_FALSE(contains(r2, 1));
    EXPECT_TRUE(contains(r2, 2));
    EXPECT_TRUE(contains(r2, 3));
}

namespace {
std::vector<sidecar::attribute_def> decimal_attributes() {
    // decimal_scale=2 (e.g. cents) - kept SEPARATE from trade_attributes() above rather than
    // added to it: build_atree()/build_betree() (matching_engine.cpp) throw the moment schema
    // CONSTRUCTION sees a decimal-typed attribute at all, not just when one is referenced by an
    // expression - adding it to the shared helper would break every existing
    // matching_engine_test (atree/betree) parameterized case before any test body even runs.
    return {{"amount", sidecar::attribute_type::decimal, std::int32_t{2}}};
}

pstree::Int256 int256_from_i64(std::int64_t v) {
    pstree::Int256 r;
    std::uint64_t bits = static_cast<std::uint64_t>(v);
    std::uint64_t fill = (v < 0) ? ~std::uint64_t{0} : std::uint64_t{0};
    r.limb = {bits, fill, fill, fill};
    return r;
}
} // namespace

// End-to-end test of pstree_dialect.cpp's own literal-promotion step (the last piece of the
// native decimal support added this session that hadn't been exercised by anything yet): a real
// subscription EXPRESSION TEXT ("amount > 100.50") parsed by be-tree's own reused parser
// (be-tree has no decimal literal kind at all - the "100.50" arrives as an ordinary AST double),
// promoted to a canonical-scale Int256 by ast_to_pstree_dnf(), matched against a real event
// built via event_sink::with_decimal(). Covers ordering (>, <=) and equality, not just the
// pstree-internal predicate-level coverage pstree's own test suite already has.
TEST(matching_engine, pstree_decimal_ordering_and_equality_end_to_end) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, decimal_attributes());
    engine->insert(1, "amount > 100.50");
    engine->insert(2, "amount = 42.00");
    engine->insert(3, "amount <= 10.00");

    auto match_amount = [&](std::int64_t scaledValue) {
        auto sink = engine->make_event();
        sink->with_decimal("amount", int256_from_i64(scaledValue));
        return engine->search(*sink);
    };

    auto r1 = match_amount(15025); // 150.25 > 100.50
    EXPECT_TRUE(contains(r1, 1));
    EXPECT_FALSE(contains(r1, 2));
    EXPECT_FALSE(contains(r1, 3));

    auto r2 = match_amount(4200); // 42.00 == 42.00
    EXPECT_FALSE(contains(r2, 1));
    EXPECT_TRUE(contains(r2, 2));
    EXPECT_FALSE(contains(r2, 3));

    auto r3 = match_amount(500); // 5.00 <= 10.00
    EXPECT_FALSE(contains(r3, 1));
    EXPECT_FALSE(contains(r3, 2));
    EXPECT_TRUE(contains(r3, 3));

    // The literal promotion boundary itself: exactly 100.50 should NOT satisfy the strict `>`.
    auto rBoundary = match_amount(10050);
    EXPECT_FALSE(contains(rBoundary, 1))
        << "amount > 100.50 should not match amount == 100.50 exactly (strict inequality)";
}

TEST(matching_engine, pstree_decimal_elem_of_not_supported) {
    // Confirmed empirically (not assumed) that this is a PRE-EXISTING be-tree limitation, not
    // decimal-specific: even an ordinary float_val attribute already rejects "in"/"not in" in
    // this whole project (be-tree's own semantic binding requires the declared variable type
    // and the literal-list type to match exactly - BETREE_FLOAT never matches an integer list,
    // regardless of attribute name). Decimal attributes are declared via add_float in the
    // parsing-only be-tree schema (build_betree_for_pstree_parsing), so they inherit this exact
    // restriction automatically - "in"/"not in" against a decimal attribute fails to PARSE
    // (a clean matching_engine_error from betree_make_sub returning nullptr), never reaching
    // ast_to_pstree_dnf's own to_dnf_set_expr at all.
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, decimal_attributes());
    EXPECT_THROW(engine->insert(1, "amount in (7, 9, 11)"), sidecar::matching_engine_error);
}

TEST(matching_engine, atree_rejects_decimal_attribute_at_schema_build) {
    EXPECT_THROW(
        sidecar::build_matching_engine(sidecar::engine_type::atree, decimal_attributes()),
        sidecar::matching_engine_error);
}

TEST(matching_engine, betree_rejects_decimal_attribute_at_schema_build) {
    EXPECT_THROW(
        sidecar::build_matching_engine(sidecar::engine_type::betree, decimal_attributes()),
        sidecar::matching_engine_error);
}

TEST(matching_engine, atree_accepts_is_not_empty) {
    // a-tree has a real IsNotEmpty token; only be-tree lacks the rule.
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::atree, trade_attributes());
    EXPECT_NO_THROW(engine->insert(1, "tags is not empty"));
}
