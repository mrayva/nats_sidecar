#include "dialect.hpp"
#include <gtest/gtest.h>

// a-tree and be-tree agree on keyword spelling for every multi-word
// operator (both use space-separated "not in", "one of", "is null", etc -
// confirmed against both engines' actual lexers). The only real gap is
// "is not empty", which be-tree's grammar has no rule for at all. These
// tests reflect that: translate_to_betree_dialect leaves expressions
// unchanged and only rejects the one unsupported construct.

TEST(dialect, leaves_expressions_unchanged) {
    EXPECT_EQ(sidecar::translate_to_betree_dialect("price > 100.0 and active"),
              "price > 100.0 and active");
}

TEST(dialect, leaves_space_separated_keywords_unchanged) {
    EXPECT_EQ(sidecar::translate_to_betree_dialect("value not in (1, 2, 3)"),
              "value not in (1, 2, 3)");
    EXPECT_EQ(sidecar::translate_to_betree_dialect("tags one of (\"a\", \"b\")"),
              "tags one of (\"a\", \"b\")");
    EXPECT_EQ(sidecar::translate_to_betree_dialect("location is null"), "location is null");
    EXPECT_EQ(sidecar::translate_to_betree_dialect("location is not null"),
              "location is not null");
    EXPECT_EQ(sidecar::translate_to_betree_dialect("tags is empty"), "tags is empty");
}

TEST(dialect, rejects_is_not_empty) {
    EXPECT_THROW(sidecar::translate_to_betree_dialect("tags is not empty"), std::runtime_error);
}

TEST(dialect, rejects_is_not_empty_among_other_clauses) {
    EXPECT_THROW(
        sidecar::translate_to_betree_dialect("active and tags is not empty and price > 1.0"),
        std::runtime_error);
}

TEST(dialect, does_not_reject_is_not_empty_split_across_unrelated_tokens) {
    // "is" ... "not" ... "empty" as three unrelated tokens (not the
    // consecutive keyword phrase) must not trip the check.
    EXPECT_NO_THROW(sidecar::translate_to_betree_dialect("is_valid and not empty_field"));
}

TEST(dialect, does_not_reject_is_not_empty_inside_double_quoted_strings) {
    EXPECT_EQ(sidecar::translate_to_betree_dialect("symbol = \"is not empty\""),
              "symbol = \"is not empty\"");
}

TEST(dialect, does_not_reject_is_not_empty_inside_single_quoted_strings) {
    EXPECT_EQ(sidecar::translate_to_betree_dialect("symbol = 'is not empty'"),
              "symbol = 'is not empty'");
}

TEST(dialect, respects_escaped_quotes_inside_string_literals) {
    // The escaped quote must not terminate the string early, so the
    // "is not empty" inside it stays inert, and the trailing clause
    // outside the literal is preserved as-is.
    EXPECT_EQ(sidecar::translate_to_betree_dialect(R"(symbol = "a \"is not empty\" b" and active)"),
              R"(symbol = "a \"is not empty\" b" and active)");
}

// translate_to_atree_dialect: a-tree's native "all of" means the opposite
// of be-tree's (and the operator's own name) - see dialect.hpp. These
// rewrite "<ident> all of (v1, ..., vn)" into an equivalent conjunction of
// singleton "one of" checks, which both engines already agree on.

TEST(dialect_atree, leaves_expressions_without_all_of_unchanged) {
    EXPECT_EQ(sidecar::translate_to_atree_dialect("price > 100.0 and active"),
              "price > 100.0 and active");
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags one of (\"a\", \"b\")"),
              "tags one of (\"a\", \"b\")");
}

TEST(dialect_atree, rewrites_single_element_all_of) {
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags all of (\"a\")"),
              "(tags one of (\"a\"))");
}

TEST(dialect_atree, rewrites_multi_element_all_of) {
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags all of (\"a\", \"b\", \"c\")"),
              "(tags one of (\"a\") and tags one of (\"b\") and tags one of (\"c\"))");
}

TEST(dialect_atree, rewrites_integer_all_of) {
    EXPECT_EQ(sidecar::translate_to_atree_dialect("codes all of (1, 2, 3)"),
              "(codes one of (1) and codes one of (2) and codes one of (3))");
}

TEST(dialect_atree, rewrites_all_of_within_a_larger_expression) {
    EXPECT_EQ(
        sidecar::translate_to_atree_dialect("active and tags all of (\"a\", \"b\") and price > 1.0"),
        "active and (tags one of (\"a\") and tags one of (\"b\")) and price > 1.0");
}

TEST(dialect_atree, rewrites_negated_all_of_preserving_negation_semantics) {
    // Structural substitution under "not (...)" is correct by construction:
    // the rewritten conjunction has the intended (be-tree-matching) meaning
    // before negation is even applied, so "not (X)" over it is still the
    // logical negation of that intended meaning - no special-casing needed.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("not (tags all of (\"a\", \"b\"))"),
              "not ((tags one of (\"a\") and tags one of (\"b\")))");
}

TEST(dialect_atree, rewrites_multiple_independent_all_of_occurrences) {
    EXPECT_EQ(
        sidecar::translate_to_atree_dialect("tags all of (\"a\") and other_tags all of (\"b\", \"c\")"),
        "(tags one of (\"a\")) and (other_tags one of (\"b\") and other_tags one of (\"c\"))");
}

TEST(dialect_atree, leaves_all_of_inside_quoted_strings_untouched) {
    EXPECT_EQ(sidecar::translate_to_atree_dialect("symbol = \"tags all of (1, 2)\""),
              "symbol = \"tags all of (1, 2)\"");
}

TEST(dialect_atree, leaves_malformed_all_of_unchanged_for_the_real_parser_to_reject) {
    // No identifier before "all of" - not valid syntax on either engine;
    // don't rewrite, let a-tree's own parser produce its normal error.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("all of (1, 2)"), "all of (1, 2)");
    // Empty list - also not valid syntax on either engine's grammar.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags all of ()"), "tags all of ()");
    // Unterminated list.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags all of (1, 2"), "tags all of (1, 2");
}

TEST(dialect_atree, does_not_reject_all_and_of_as_unrelated_tokens) {
    // "all" and "of" appearing as ordinary, unrelated identifiers (not the
    // consecutive "all of" phrase right after an ident) must pass through.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("all and of_field"), "all and of_field");
}

TEST(dialect_atree, does_not_reject_all_of_not_followed_by_parenthesized_list) {
    // "all of" with no "(" immediately after isn't the list operator at
    // all (e.g. two coincidental identifiers) - pass through unchanged.
    EXPECT_EQ(sidecar::translate_to_atree_dialect("tags all of thing"), "tags all of thing");
}
