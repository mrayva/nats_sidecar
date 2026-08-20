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
