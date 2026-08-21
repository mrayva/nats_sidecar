// Systematic cross-engine differential test: for a fixed battery of
// expressions and event fixtures, a-tree and be-tree must agree on whether
// each event matches. tests/test_matching_engine.cpp already covers both
// engines with identical hand-picked test bodies (TEST_P over engine_type);
// this file goes further by directly diffing the two engines' search()
// results against each other, case by case, for a much broader operator x
// type matrix - so a future engine upgrade that silently changes semantics
// for a case neither file's authors thought to hand-pick still gets caught.
//
// Deliberately table-driven and fully deterministic (no RNG) - this runs in
// CI and a flaky differential test would be worse than no differential test.
//
// "is not empty" is deliberately excluded: be-tree has no rule for it at all
// (see dialect.hpp) and rejects it outright, which is an already-known,
// intentional asymmetry - not something a differential test should flag.

#include "matching_engine.hpp"
#include <gtest/gtest.h>
#include <algorithm>
#include <optional>
#include <ostream>

namespace {

using sidecar::attribute_def;
using sidecar::attribute_type;
using sidecar::engine_type;

std::vector<attribute_def> differential_attributes() {
    return {
        {"flag",    attribute_type::boolean},
        {"count",   attribute_type::integer},
        {"amount",  attribute_type::float_val},
        {"name",    attribute_type::string},
        {"tags",    attribute_type::string_list},
        {"scores",  attribute_type::integer_list},
    };
}

// One event fixture. Every field is optional - fields left unset are sent
// via event_sink::with_undefined(), matching how subscription_manager
// actually populates a partial event from a real message's fields.
struct differential_event {
    std::optional<bool> flag;
    std::optional<int64_t> count;
    std::optional<double> amount;
    std::optional<std::string> name;
    std::optional<std::vector<std::string>> tags;
    std::optional<std::vector<int64_t>> scores;
};

void populate(sidecar::event_sink& sink, const differential_event& ev) {
    ev.flag   ? sink.with_boolean("flag", *ev.flag)      : sink.with_undefined("flag");
    ev.count  ? sink.with_integer("count", *ev.count)    : sink.with_undefined("count");
    ev.amount ? sink.with_float("amount", *ev.amount)    : sink.with_undefined("amount");
    ev.name   ? sink.with_string("name", *ev.name)       : sink.with_undefined("name");
    ev.tags   ? sink.with_string_list("tags", *ev.tags)  : sink.with_undefined("tags");
    ev.scores ? sink.with_integer_list("scores", *ev.scores) : sink.with_undefined("scores");
}

bool matches(engine_type type, const std::string& expr, const differential_event& ev) {
    auto engine = sidecar::build_matching_engine(type, differential_attributes());
    engine->insert(1, expr);
    auto sink = engine->make_event();
    populate(*sink, ev);
    auto result = engine->search(*sink);
    return std::find(result.begin(), result.end(), uint64_t{1}) != result.end();
}

struct diff_case {
    std::string label;   // for readable test-instance names
    std::string expr;
    differential_event event;
    bool expect_match;   // the *agreed* expected result, so a divergence and
                          // a shared-wrong-answer both get caught, not just
                          // disagreement between the two engines.
};

std::ostream& operator<<(std::ostream& os, const diff_case& c) {
    return os << c.label;
}

class matching_engine_differential : public ::testing::TestWithParam<diff_case> {};

TEST_P(matching_engine_differential, engines_agree) {
    const auto& c = GetParam();
    bool atree_result  = matches(engine_type::atree, c.expr, c.event);
    bool betree_result = matches(engine_type::betree, c.expr, c.event);

    EXPECT_EQ(atree_result, betree_result)
        << "a-tree and be-tree disagree on \"" << c.expr << "\" for case \"" << c.label << "\"";
    EXPECT_EQ(atree_result, c.expect_match)
        << "a-tree gave an unexpected result for \"" << c.expr << "\" (case \"" << c.label << "\")";
    EXPECT_EQ(betree_result, c.expect_match)
        << "be-tree gave an unexpected result for \"" << c.expr << "\" (case \"" << c.label << "\")";
}

std::vector<diff_case> differential_cases() {
    return {
        // --- integer comparisons ---
        {"int_gt_true",  "count > 100",  {.count = 150}, true},
        {"int_gt_false", "count > 100",  {.count = 50},  false},
        {"int_lt_true",  "count < 100",  {.count = 50},  true},
        {"int_lt_false", "count < 100",  {.count = 150}, false},
        {"int_ge_boundary_true",  "count >= 100", {.count = 100}, true},
        {"int_le_boundary_true",  "count <= 100", {.count = 100}, true},
        {"int_eq_true",  "count = 42",   {.count = 42},  true},
        {"int_eq_false", "count = 42",   {.count = 43},  false},
        {"int_ne_true",  "count <> 42",  {.count = 43},  true},
        {"int_ne_false", "count <> 42",  {.count = 42},  false},

        // --- float comparisons ---
        {"float_gt_true",  "amount > 99.5",  {.amount = 100.0}, true},
        {"float_gt_false", "amount > 99.5",  {.amount = 99.0},  false},
        {"float_eq_true",  "amount = 3.5",   {.amount = 3.5},   true},
        {"float_le_boundary_true", "amount <= 3.5", {.amount = 3.5}, true},

        // --- string comparisons ---
        {"string_eq_true",  "name = \"AAPL\"",  {.name = std::string("AAPL")}, true},
        {"string_eq_false", "name = \"AAPL\"",  {.name = std::string("MSFT")}, false},
        {"string_ne_true",  "name <> \"AAPL\"", {.name = std::string("MSFT")}, true},
        {"string_ne_false", "name <> \"AAPL\"", {.name = std::string("AAPL")}, false},

        // --- boolean truthiness ---
        {"bool_direct_true",  "flag",       {.flag = true},  true},
        {"bool_direct_false", "flag",       {.flag = false}, false},
        {"bool_not_true",     "not flag",   {.flag = false}, true},
        {"bool_not_false",    "not flag",   {.flag = true},  false},

        // --- and / or combinators ---
        {"and_both_true",   "count > 10 and amount > 1.0", {.count = 20, .amount = 2.0}, true},
        {"and_one_false",   "count > 10 and amount > 1.0", {.count = 20, .amount = 0.5}, false},
        {"or_one_true",     "count > 1000 or name = \"AAPL\"", {.count = 1, .name = std::string("AAPL")}, true},
        {"or_both_false",   "count > 1000 or name = \"AAPL\"", {.count = 1, .name = std::string("MSFT")}, false},
        {"nested_and_or",   "(count > 10 and amount > 1.0) or name = \"X\"",
            {.count = 5, .amount = 0.1, .name = std::string("X")}, true},

        // --- in / not in ---
        {"in_true",     "count in (1, 2, 3)",     {.count = 2},  true},
        {"in_false",    "count in (1, 2, 3)",     {.count = 9},  false},
        {"not_in_true", "count not in (1, 2, 3)", {.count = 9},  true},
        {"not_in_false","count not in (1, 2, 3)", {.count = 2},  false},

        // --- is null / is not null ---
        {"is_null_true",      "name is null",     {}, true},
        {"is_null_false",     "name is null",     {.name = std::string("AAPL")}, false},
        {"is_not_null_true",  "name is not null", {.name = std::string("AAPL")}, true},
        {"is_not_null_false", "name is not null", {}, false},

        // --- is empty (list attribute) ---
        {"is_empty_true",  "tags is empty", {.tags = std::vector<std::string>{}}, true},
        {"is_empty_false", "tags is empty", {.tags = std::vector<std::string>{"a"}}, false},

        // --- list-attribute membership: one of / none of ---
        // one_of/none_of apply to list-typed attributes (the literal list is
        // compared against the *attribute's* list value) - not to scalar
        // attributes, confirmed against both engines' own grammars (a-tree's
        // ListOperator::evaluate() requires an AttributeValue::StringList/
        // IntegerList on the left; be-tree's parser.y's list_expr rule is
        // identically shaped: `ident TONEOF list_value`). Both engines agree
        // here: true iff the attribute's list and the literal list share at
        // least one element (a-tree's one_of()/none_of() do a sorted-merge
        // intersection check with no length-direction dependency).
        {"one_of_true",  "tags one of (\"x\", \"y\")",  {.tags = std::vector<std::string>{"x", "z"}}, true},
        {"one_of_false", "tags one of (\"x\", \"y\")",  {.tags = std::vector<std::string>{"z"}}, false},
        {"none_of_true", "tags none of (\"x\", \"y\")", {.tags = std::vector<std::string>{"z"}}, true},
        {"none_of_false","tags none of (\"x\", \"y\")", {.tags = std::vector<std::string>{"x"}}, false},

        // --- all of: reconciled to be-tree's native semantic (the event's
        // attribute list must contain every listed value - "the event has
        // ALL OF these") via dialect.cpp's translate_to_atree_dialect(),
        // which rewrites "X all of (lits)" into a conjunction of singleton
        // "one of" checks before handing it to a-tree - see matching_engine
        // .all_of_reconciled_across_engines below for why this exists.
        {"all_of_true",       "scores all of (1, 2)",    {.scores = std::vector<int64_t>{1, 2, 3}}, true},
        {"all_of_missing_one","scores all of (1, 2)",    {.scores = std::vector<int64_t>{1, 3}},     false},
        {"all_of_string_true","tags all of (\"x\", \"y\")", {.tags = std::vector<std::string>{"x", "y", "z"}}, true},
        {"all_of_string_missing", "tags all of (\"x\", \"y\")", {.tags = std::vector<std::string>{"x"}}, false},
        {"all_of_single_element", "scores all of (2)",   {.scores = std::vector<int64_t>{1, 2, 3}}, true},
    };
}

INSTANTIATE_TEST_SUITE_P(
    differential_matrix, matching_engine_differential,
    ::testing::ValuesIn(differential_cases()),
    [](const ::testing::TestParamInfo<diff_case>& info) { return info.param.label; });

// Real divergence found by this file's own differential matrix during
// development (not hypothetical), now reconciled - kept here as a
// dedicated regression test (in addition to the "all_of_*" cases folded
// into the main matrix above) because the root cause is worth keeping
// documented even though it's fixed. Read directly out of both engines'
// vendored evaluator source:
//
//   - a-tree (predicates.rs, all_of(left=attr, right=literal)): requires
//     left.len() <= right.len(), then checks every element of the
//     ATTRIBUTE appears in the LITERAL - i.e. natively, "attribute is a
//     subset of the literal set".
//   - be-tree (ast_match_shared.hpp, ast_match_all_of_int/string(variable,
//     list_expr)): requires literal_count <= attribute_count, then checks
//     every element of the LITERAL appears in the ATTRIBUTE - i.e.
//     "attribute is a superset of the literal set" (the intuitive
//     reading matching the operator's own name, and the opposite
//     direction from a-tree's native one).
//
// be-tree's (superset) semantic is treated as canonical. matching_engine's
// atree_matching_engine::insert() runs every expression through
// dialect.cpp's translate_to_atree_dialect() first, which rewrites
// "X all of (lits)" into a conjunction of singleton "one of" checks -
// which both engines' one_of() already agree on (a symmetric
// non-empty-intersection test) - so a-tree's own native (wrong-for-us)
// "all of" token is never actually used for this construct. This test
// exercises that end-to-end, through matching_engine::insert() (not
// dialect.cpp's translate function directly - that's covered by
// tests/test_dialect.cpp), for both engines against the exact scores=[1,2,3]
// fixture that originally exposed the divergence.
TEST(matching_engine, all_of_reconciled_across_engines) {
    std::vector<attribute_def> attrs = {{"scores", attribute_type::integer_list}};
    std::vector<int64_t> scores{1, 2, 3};

    for (auto type : {engine_type::atree, engine_type::betree}) {
        auto engine = sidecar::build_matching_engine(type, attrs);
        engine->insert(1, "scores all of (1, 2)");
        auto sink = engine->make_event();
        sink->with_integer_list("scores", scores);
        auto result = engine->search(*sink);
        EXPECT_FALSE(result.empty())
            << "engine " << (type == engine_type::atree ? "atree" : "betree")
            << " should match scores=[1,2,3] against \"scores all of (1, 2)\" "
               "now that all_of is reconciled to be-tree's superset semantic";
    }
}

} // namespace
