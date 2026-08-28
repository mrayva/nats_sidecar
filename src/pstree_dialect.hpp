#pragma once

#include <cstdint>
#include <vector>
#include <pstree/predicate.hpp>

struct ast_node; // be-tree's own parsed-expression AST node (src/ast.h)

namespace sidecar {

// One conjunctive clause - a single PSTDynamic-representable subscription's worth of
// predicates. A full expression translates to a LIST of these (Disjunctive Normal Form):
// the original expression matches iff at least one clause's predicates ALL hold.
using pstree_clause = std::vector<pstree::SubPredicate>;

// Translates a be-tree ast_node - already parsed AND variable-resolved against a real
// betree config, via betree_make_sub() - into DNF.
//
// Why be-tree's own AST, not a fresh parser: PSTDynamic's own model (the paper's Section
// 2.1) is a pure conjunction of predicates - no AND/OR/NOT combinators at all - but
// nats_sidecar's actual expression language (confirmed against
// tests/test_matching_engine_differential.cpp) supports the full thing: "and"/"or"/"not",
// parens, "in"/"not in", "is null"/"is not null". Rather than write a second, independent
// parser for that same grammar (a real, error-prone undertaking - exactly the risk this
// whole project has otherwise avoided by transcribing existing, tested designs instead of
// inventing new ones), this reuses be-tree's OWN already-linked, already-tested parser:
// betree_make_sub() parses AND resolves variable ids WITHOUT needing betree_insert_sub() to
// ever be called, so a throwaway be-tree instance (built purely to parse, never searched)
// gives a structured, type-checked AST for free. Negation is pushed down to the leaves via
// De Morgan's laws (see the .cpp); OR is expanded into separate DNF clauses, each becoming
// its own PSTDynamic subscription sharing the original id (see pstree_matching_engine in
// matching_engine.cpp for how the results get merged/deduplicated back together).
//
// Deliberately NOT supported (throws sidecar::matching_engine_error), all things the
// paper's own model and PSTDynamic's per-dimension PS-Tree design have no way to
// represent, not omissions of convenience:
//   - list-valued attributes and their operators (one of/none of/all of, is empty) -
//     an event attribute in PSTDynamic's model is always a single value.
//   - be-tree's "special" expressions (frequency caps, segments, geo-radius, string
//     pattern match) - out of scope, not part of Section 2.1's predicate language at all.
//   - a set (in/not in) or equality expression with a variable on either side other than
//     the expected "attribute compared against literal(s)" shape.
//   - a `not (X all of (...))`/`not (X is empty)` construct in particular can't be pushed
//     to a leaf even in principle (negating ALL_OF's implicit "AND of contains" would need
//     a DISJUNCTION over LIST elements, which needs list-valued-attribute support this
//     doesn't have) - moot in practice since list operators are rejected outright above.
//   - a pathologically large OR-of-AND expansion (more than a generous clause cap) -
//     rejected rather than silently allowed to blow up memory/insertion time.
std::vector<pstree_clause> ast_to_pstree_dnf(const struct ast_node* node);

} // namespace sidecar
