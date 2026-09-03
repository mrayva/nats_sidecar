#include "matching_engine.hpp"
#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <vector>

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

// Regression test for a real, confirmed data race: pstree_matching_engine::search()'s dedup
// scratch set used to be a plain `mutable` member (m_seen_scratch), shared by every concurrent
// caller of the same engine instance - worker_pool's own worker threads all call search()
// concurrently against one shared engine (subscription_manager::acquire_tree() hands out a
// shared_lock, never serializing readers against each other), so two threads both mutating one
// std::unordered_set at once corrupted its buckets and segfaulted. Only OR'd (multi-clause)
// subscriptions ever reach that dedup path at all (single-clause subscriptions take insert()'s
// own direct-id fast path, bypassing it) - which is exactly why every earlier single-clause-only
// test/benchmark in this project's history never caught this. Fixed by making the scratch set a
// function-local `static thread_local` instead of a member - see search()'s own comment
// (matching_engine.cpp) for the full story. This test's real purpose is to run under
// ThreadSanitizer (ctest under build-sanitizer/SIDECAR_SANITIZER=thread) - it may not reliably
// reproduce the crash on a plain, uninstrumented build even without the fix.
//
// Deliberately does NOT reuse trade_attributes()/match()/trade_event above: those set a
// `tags: string_list` attribute unconditionally on every event, which pstree rejects outright
// (a real, pre-existing, unrelated restriction - "pstree does not support list-valued
// attributes") - confirmed directly while debugging this test (an early version threw that
// exception from every thread simultaneously, an uncaught-exception storm that looked exactly
// like a crash but had nothing to do with the race being tested). This test's own schema omits
// list attributes entirely so only the race fix itself is under test.
TEST(matching_engine, pstree_concurrent_search_with_or_subscriptions_does_not_race) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    // A mix of single-clause (AND-only) and OR'd (multi-clause) subscriptions - the OR'd ones
    // are what actually exercise the now-fixed dedup path.
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_price > 50.0 and trade_volume > 100");
    engine->insert(3, "trade_price > 200.0 or trade_volume > 900");
    engine->insert(4, "symbol = \"AAPL\" or trade_price < 10.0");
    engine->insert(5, "trade_price > 150.0 or symbol = \"MSFT\" or trade_volume > 700");

    auto search_one = [&engine](double price, int64_t volume, std::string_view symbol) {
        auto sink = engine->make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", volume);
        sink->with_string("symbol", symbol);
        return engine->search(*sink);
    };

    constexpr int kThreads = 8;
    constexpr int kIterationsPerThread = 2000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&search_one, t]() {
            for (int i = 0; i < kIterationsPerThread; ++i) {
                search_one(static_cast<double>((t * 37 + i) % 300), (t * 53 + i) % 1000,
                           (i % 2 == 0) ? "AAPL" : "MSFT");
            }
        });
    }
    for (auto& th : threads) th.join();
    // Reaching here without crashing/hanging under TSan is the actual assertion; also confirm
    // ordinary matching still works correctly afterward.
    EXPECT_TRUE(contains(search_one(250.0, 50, "AAPL"), 1));
}

// search()'s own translate/dedup loop (matching_engine.cpp) is skipped entirely when no
// subscription has ever taken the synthetic-clause-id path (m_hasSyntheticClauses stays false) -
// found to matter via `perf report` on a live fleet trial once PSTDynamic::matchEvent()'s own
// cost got cheap enough for search()'s own copy loop to become the largest single contributor.
// This test builds an engine with ONLY single-clause (no OR) subscriptions - implicitly
// exercising that fast path - and confirms it produces correct matches, not just "doesn't crash".
TEST(matching_engine, pstree_search_fast_path_with_only_single_clause_subscriptions_is_correct) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_volume > 1000");
    engine->insert(3, "symbol = \"AAPL\"");
    engine->insert(4, "trade_price > 500.0 and trade_volume > 2000");

    auto search_one = [&engine](double price, int64_t volume, std::string_view symbol) {
        auto sink = engine->make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", volume);
        sink->with_string("symbol", symbol);
        return engine->search(*sink);
    };

    auto r1 = search_one(150.0, 500, "AAPL");
    EXPECT_TRUE(contains(r1, 1));
    EXPECT_FALSE(contains(r1, 2));
    EXPECT_TRUE(contains(r1, 3));
    EXPECT_FALSE(contains(r1, 4));

    auto r2 = search_one(600.0, 3000, "MSFT");
    EXPECT_TRUE(contains(r2, 1));
    EXPECT_TRUE(contains(r2, 2));
    EXPECT_FALSE(contains(r2, 3));
    EXPECT_TRUE(contains(r2, 4));

    auto r3 = search_one(1.0, 1, "GOOG");
    EXPECT_TRUE(r3.empty());
}

// m_hasSyntheticClauses is deliberately monotonic - once any subscription takes the synthetic-
// clause-id path (an OR'd/multi-clause expression), it stays true for the engine's whole
// lifetime, even if that subscription is never referenced again. matching_engine has no
// incremental delete (subscription_manager rebuilds a fresh engine instead - see this file's own
// class comment), so this test demonstrates the invariant the flag depends on directly: an engine
// that has an OR'd subscription present alongside single-clause ones (forcing the slow
// translate/dedup path for every search) must still produce results for the single-clause
// subscriptions IDENTICAL to an engine built with only those single-clause subscriptions (the
// fast path) - proving the two code paths are equivalent, not just that neither crashes.
TEST(matching_engine, pstree_search_results_identical_whether_or_not_synthetic_clauses_are_present) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto fast_engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    fast_engine->insert(1, "trade_price > 100.0");
    fast_engine->insert(2, "trade_volume > 1000");
    fast_engine->insert(3, "symbol = \"AAPL\"");

    auto slow_engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    // Inserted FIRST, before any single-clause subscription - forces m_hasSyntheticClauses = true
    // for the rest of this engine's lifetime, exactly as it would for a real fleet that has ever
    // accepted one OR'd expression.
    slow_engine->insert(100, "trade_price > 9999.0 or symbol = \"ZZZZ\"");
    slow_engine->insert(1, "trade_price > 100.0");
    slow_engine->insert(2, "trade_volume > 1000");
    slow_engine->insert(3, "symbol = \"AAPL\"");

    auto search_one = [](sidecar::matching_engine& engine, double price, int64_t volume,
                          std::string_view symbol) {
        auto sink = engine.make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", volume);
        sink->with_string("symbol", symbol);
        return engine.search(*sink);
    };

    for (auto [price, volume, symbol] :
         {std::tuple{150.0, 500, "AAPL"}, std::tuple{600.0, 3000, "MSFT"},
          std::tuple{1.0, 1, "GOOG"}}) {
        auto fast_result = search_one(*fast_engine, price, volume, symbol);
        auto slow_result = search_one(*slow_engine, price, volume, symbol);
        for (uint64_t id : {1, 2, 3}) {
            EXPECT_EQ(contains(fast_result, id), contains(slow_result, id))
                << "sub " << id << " disagreed between fast/slow search() paths for price="
                << price << " volume=" << volume << " symbol=" << symbol;
        }
    }
}

// search_count() fast path (no OR'd subscriptions - m_hasSyntheticClauses stays false, same
// gating as search()'s own fast path above): confirms search_count() always agrees with
// search().size(), the actual correctness contract - see matching_engine.hpp's own comment for
// why this method exists at all (a caller that only needs the count, not the ids).
TEST(matching_engine, pstree_search_count_matches_search_size_fast_path) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_volume > 1000");
    engine->insert(3, "symbol = \"AAPL\"");
    engine->insert(4, "trade_price > 500.0 and trade_volume > 2000");

    ASSERT_TRUE(engine->supports_count());

    for (auto [price, volume, symbol] :
         {std::tuple{150.0, 500, "AAPL"}, std::tuple{600.0, 3000, "MSFT"},
          std::tuple{1.0, 1, "GOOG"}}) {
        auto count_sink = engine->make_event();
        count_sink->with_float("trade_price", price);
        count_sink->with_integer("trade_volume", volume);
        count_sink->with_string("symbol", symbol);
        std::size_t count = engine->search_count(*count_sink);

        auto search_sink = engine->make_event();
        search_sink->with_float("trade_price", price);
        search_sink->with_integer("trade_volume", volume);
        search_sink->with_string("symbol", symbol);
        auto result = engine->search(*search_sink);

        EXPECT_EQ(count, result.size())
            << "search_count() disagreed with search().size() for price=" << price
            << " volume=" << volume << " symbol=" << symbol;
    }
}

// search_count() slow (OR-clause) path - the one that must NOT just call search()/matchEvent()
// and count the result (see search_count()'s own comment in matching_engine.cpp for why that
// would defeat the whole feature). Includes an event that hits TWO clauses of the SAME OR'd
// subscription (sub 5 below matches via both its trade_price and trade_volume clauses at once) -
// the actual dedup case search_count()'s seen_count set exists to get right.
TEST(matching_engine, pstree_search_count_matches_search_size_with_or_subscriptions) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_price > 50.0 and trade_volume > 100");
    engine->insert(3, "trade_price > 200.0 or trade_volume > 900");
    engine->insert(4, "symbol = \"AAPL\" or trade_price < 10.0");
    // Both clauses of sub 5 match the same event below (trade_price=250 > 150 AND
    // trade_volume=950 > 700) - proves the dedup path collapses a double clause-match to one
    // result, not two.
    engine->insert(5, "trade_price > 150.0 or symbol = \"MSFT\" or trade_volume > 700");

    for (auto [price, volume, symbol] :
         {std::tuple{250.0, 950, "AAPL"}, std::tuple{1.0, 1, "GOOG"}, std::tuple{600.0, 50, "MSFT"}}) {
        auto count_sink = engine->make_event();
        count_sink->with_float("trade_price", price);
        count_sink->with_integer("trade_volume", volume);
        count_sink->with_string("symbol", symbol);
        std::size_t count = engine->search_count(*count_sink);

        auto search_sink = engine->make_event();
        search_sink->with_float("trade_price", price);
        search_sink->with_integer("trade_volume", volume);
        search_sink->with_string("symbol", symbol);
        auto result = engine->search(*search_sink);

        EXPECT_EQ(count, result.size())
            << "search_count() disagreed with search().size() for price=" << price
            << " volume=" << volume << " symbol=" << symbol;
    }
}

// Zero-risk contract for a-tree/be-tree: neither overrides supports_count()/search_count(), so
// both inherit the base class's defaults - locks that in directly rather than relying on it
// never being exercised by accident.
TEST_P(matching_engine_test, search_count_unsupported_by_default) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_price > 100.0");
    EXPECT_FALSE(engine->supports_count());

    auto sink = engine->make_event();
    sink->with_float("trade_price", 150.0);
    sink->with_integer("trade_volume", 100);
    sink->with_string("symbol", "AAPL");
    sink->with_boolean("active", true);
    sink->with_string_list("tags", {});
    EXPECT_THROW(engine->search_count(*sink), sidecar::matching_engine_error);
}

// Mirrors pstree_concurrent_search_with_or_subscriptions_does_not_race above exactly, but calling
// search_count() from every thread instead of search() - the actual regression guard for
// search_count()'s own seen_count thread_local (see its own comment in matching_engine.cpp for
// why it must stay thread_local, not a member, for the identical reason search()'s `seen` already
// documents). Real purpose is running under ThreadSanitizer (build-sanitizer with
// SIDECAR_SANITIZER=thread) - may not reliably reproduce a race on a plain build even if broken.
TEST(matching_engine, pstree_search_count_concurrent_with_or_subscriptions_does_not_race) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_price > 50.0 and trade_volume > 100");
    engine->insert(3, "trade_price > 200.0 or trade_volume > 900");
    engine->insert(4, "symbol = \"AAPL\" or trade_price < 10.0");
    engine->insert(5, "trade_price > 150.0 or symbol = \"MSFT\" or trade_volume > 700");

    auto count_one = [&engine](double price, int64_t volume, std::string_view symbol) {
        auto sink = engine->make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", volume);
        sink->with_string("symbol", symbol);
        return engine->search_count(*sink);
    };

    constexpr int kThreads = 8;
    constexpr int kIterationsPerThread = 2000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&count_one, t]() {
            for (int i = 0; i < kIterationsPerThread; ++i) {
                count_one(static_cast<double>((t * 37 + i) % 300), (t * 53 + i) % 1000,
                          (i % 2 == 0) ? "AAPL" : "MSFT");
            }
        });
    }
    for (auto& th : threads) th.join();
    // Reaching here without crashing/hanging under TSan is the actual assertion; also confirm
    // ordinary counting still works correctly afterward.
    EXPECT_GE(count_one(250.0, 50, "AAPL"), std::size_t{1});
}

// remove() - a-tree/be-tree zero-risk contract, mirroring search_count_unsupported_by_default's
// own shape.
TEST_P(matching_engine_test, remove_unsupported_by_default) {
    auto engine = sidecar::build_matching_engine(GetParam(), trade_attributes());
    engine->insert(1, "trade_price > 100.0");
    EXPECT_FALSE(engine->supports_remove());
    EXPECT_THROW(engine->remove(1), sidecar::matching_engine_error);
}

TEST(matching_engine, pstree_supports_remove_returns_true) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    EXPECT_TRUE(engine->supports_remove());
}

TEST(matching_engine, pstree_remove_single_clause_subscription_stops_matching) {
    // Deliberately NOT trade_attributes() (has a "tags" string_list attribute) - pstree rejects
    // list-valued attributes outright, same reason the existing OR/concurrent pstree tests below
    // already use their own narrower schema instead.
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 100.0");
    engine->insert(2, "trade_volume > 1000");

    auto search_one = [&engine](double price, int64_t volume) {
        auto sink = engine->make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", volume);
        return engine->search(*sink);
    };

    EXPECT_TRUE(contains(search_one(150.0, 500), 1));
    engine->remove(1);
    EXPECT_FALSE(contains(search_one(150.0, 500), 1))
        << "removed subscription must never match again";
    EXPECT_TRUE(contains(search_one(150.0, 2000), 2))
        << "an unrelated subscription must be entirely unaffected by another one's removal";
}

// The m_sub_to_clauses path - an OR'd subscription expands into multiple underlying PSTDynamic
// clause ids at insert() time; remove() must delete ALL of them, not just one, so the
// subscription stops matching on EVERY one of its own original OR branches, not just some.
TEST(matching_engine, pstree_remove_or_subscription_stops_matching_on_all_clauses) {
    std::vector<sidecar::attribute_def> attrs = {
        {"trade_price",  sidecar::attribute_type::float_val},
        {"trade_volume", sidecar::attribute_type::integer},
        {"symbol",       sidecar::attribute_type::string},
    };
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, attrs);
    engine->insert(1, "trade_price > 500.0 or symbol = \"ZZZZ\"");

    auto search_one = [&engine](double price, std::string_view symbol) {
        auto sink = engine->make_event();
        sink->with_float("trade_price", price);
        sink->with_integer("trade_volume", 0);
        sink->with_string("symbol", symbol);
        return engine->search(*sink);
    };

    // Confirm both OR branches genuinely match before removal.
    EXPECT_TRUE(contains(search_one(600.0, "AAPL"), 1)) << "first OR branch (price) should match";
    EXPECT_TRUE(contains(search_one(1.0, "ZZZZ"), 1)) << "second OR branch (symbol) should match";

    engine->remove(1);

    EXPECT_FALSE(contains(search_one(600.0, "AAPL"), 1))
        << "first OR branch must no longer match after remove()";
    EXPECT_FALSE(contains(search_one(1.0, "ZZZZ"), 1))
        << "second OR branch must no longer match after remove() either - not just one of the two "
           "underlying clause ids";
}

TEST(matching_engine, pstree_remove_unknown_id_throws_matching_engine_error) {
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::pstree, trade_attributes());
    engine->insert(1, "trade_price > 100.0");
    EXPECT_THROW(engine->remove(999), sidecar::matching_engine_error);
}

// NOTE: no "remove() concurrent with search()" test at THIS layer, deliberately. An earlier
// version of this test called both directly on a raw matching_engine with no external
// synchronization at all, and immediately crashed (std::bad_alloc from PSTDynamic's own internal
// state, corrupted by the unsynchronized concurrent mutation) - a real finding, but about the
// test's own premise, not a production bug: matching_engine was never designed to be safely
// mutated and read concurrently on its own. subscription_manager::m_mutex is what actually
// serializes remove() (exclusive lock) against search() (shared lock) in production - see that
// class's own "Deliberately NOT lock-free for readers" doc comment - and every existing
// concurrent test AT THIS layer (pstree_concurrent_search_with_or_subscriptions_does_not_race,
// pstree_search_count_concurrent_with_or_subscriptions_does_not_race) only exercises concurrent
// READS for exactly this reason. The real concurrency test for remove() belongs one layer up,
// where the actual locking lives - see test_subscription_manager.cpp's own
// pstree_remove_lease_concurrent_with_search_does_not_race.
