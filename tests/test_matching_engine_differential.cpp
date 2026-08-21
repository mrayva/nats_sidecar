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

        // "all of" is deliberately NOT in this matrix - see
        // matching_engine_all_of_semantics_diverge below. Unlike
        // one_of/none_of, the two engines disagree on what "X all of (lits)"
        // even means, so there is no single expected answer to assert here.
    };
}

INSTANTIATE_TEST_SUITE_P(
    differential_matrix, matching_engine_differential,
    ::testing::ValuesIn(differential_cases()),
    [](const ::testing::TestParamInfo<diff_case>& info) { return info.param.label; });

// Real, confirmed divergence found by this file's own differential matrix
// during development (not hypothetical): "X all of (lits)" means opposite
// things on the two engines, and NEITHER errors - a query that switches
// engine=atree to engine=betree (or vice versa) silently changes which
// messages match, unlike every other operator this file tests. Root cause,
// read directly out of both engines' vendored evaluator source:
//
//   - a-tree (predicates.rs, all_of(left=attr, right=literal)): requires
//     left.len() <= right.len(), then checks every element of the
//     ATTRIBUTE appears in the LITERAL - i.e. "attribute is a subset of
//     the literal set".
//   - be-tree (ast_match_shared.hpp, ast_match_all_of_int/string(variable,
//     list_expr)): requires literal_count <= attribute_count, then checks
//     every element of the LITERAL appears in the ATTRIBUTE - i.e.
//     "attribute is a superset of the literal set" (the intuitive
//     reading, and the opposite direction from a-tree's).
//
// scores = [1, 2, 3], expression "scores all of (1, 2)": a-tree says false
// (the 3-element attribute can't be a subset of the 2-element literal);
// be-tree says true (the attribute contains every literal element). This
// is a real gap in matching_engine's engine-agnostic contract - the two
// engines are not swappable for any expression using "all of" - not
// something a test can "fix", so it's pinned here to keep the divergence
// visible rather than silently rediscovered later.
TEST(matching_engine, all_of_semantics_diverge_between_engines) {
    std::vector<attribute_def> attrs = {{"scores", attribute_type::integer_list}};
    std::vector<int64_t> scores{1, 2, 3};

    auto atree_engine = sidecar::build_matching_engine(engine_type::atree, attrs);
    atree_engine->insert(1, "scores all of (1, 2)");
    auto atree_sink = atree_engine->make_event();
    atree_sink->with_integer_list("scores", scores);
    auto atree_result = atree_engine->search(*atree_sink);
    EXPECT_TRUE(atree_result.empty())
        << "a-tree's all_of requires the attribute to be a SUBSET of the literal";

    auto betree_engine = sidecar::build_matching_engine(engine_type::betree, attrs);
    betree_engine->insert(1, "scores all of (1, 2)");
    auto betree_sink = betree_engine->make_event();
    betree_sink->with_integer_list("scores", scores);
    auto betree_result = betree_engine->search(*betree_sink);
    EXPECT_FALSE(betree_result.empty())
        << "be-tree's all_of requires the attribute to be a SUPERSET of the literal";
}

} // namespace
