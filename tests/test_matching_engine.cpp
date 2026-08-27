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

TEST(matching_engine, atree_accepts_is_not_empty) {
    // a-tree has a real IsNotEmpty token; only be-tree lacks the rule.
    auto engine = sidecar::build_matching_engine(sidecar::engine_type::atree, trade_attributes());
    EXPECT_NO_THROW(engine->insert(1, "tags is not empty"));
}
