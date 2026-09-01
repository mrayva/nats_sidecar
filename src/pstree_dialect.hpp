#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <pstree/predicate.hpp>

struct ast_node; // be-tree's own parsed-expression AST node (src/ast.h)

namespace sidecar {

// One conjunctive clause - a single PSTDynamic-representable subscription's worth of
// predicates. A full expression translates to a LIST of these (Disjunctive Normal Form):
// the original expression matches iff at least one clause's predicates ALL hold.
using pstree_clause = std::vector<pstree::SubPredicate>;

// attribute name -> that attribute's own canonical decimal_scale (config.hpp's
// attribute_def::decimal_scale / pstree/pst_dynamic.hpp's AttrSchema::decimalScale) - entries
// exist only for decimal-typed attributes. Deliberately a plain map, not
// matching_engine.hpp's own string_view_lookup_map<> - this header stays decoupled from
// matching_engine.hpp, matching its existing narrow dependency on just pstree/predicate.hpp.
using decimal_scale_map = std::unordered_map<std::string, std::int32_t>;

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
//   - `in`/`not in` against a decimal-typed attribute - confirmed EMPIRICALLY, not assumed:
//     decimal attributes are declared via add_float in the parsing-only be-tree schema
//     (matching_engine.cpp's build_betree_for_pstree_parsing), and be-tree's own semantic
//     binding already rejects "in"/"not in" against ANY float-typed attribute (BETREE_FLOAT
//     never matches an integer literal list) - a real, pre-existing be-tree limitation decimal
//     simply inherits, not something new this support added. Fails to PARSE (a clean
//     matching_engine_error from betree_make_sub itself), never reaching this function's own
//     to_dnf_set_expr at all.
//
// `decimalScales` (added for native DECIMAL32/64/128/256 support): be-tree's own grammar has no
// decimal literal kind at all (num_comp_value/eq_value are int64/double/string only), so a
// literal compared against a decimal-typed attribute arrives here as an ordinary AST int64 or
// double, indistinguishable from a real integer/float attribute's own literal. `decimalScales`
// is what lets this function tell the difference and PROMOTE that int64/double into the target
// attribute's own canonical-scale pstree::Int256 instead of constructing an int64_t/double
// Value that pstree's own kDecimal dispatch would later reject with a type mismatch. This
// promotion is where the query side's own precision cap actually lives: the literal was already
// rounded to a double by be-tree's lexer before this function ever sees it, so the resulting
// Int256 is exact for THAT already-approximated double, not for whatever exact decimal text a
// human originally typed - see the project plan's own "Design decision 2" for why this is an
// accepted phase-1 scope boundary, not a bug.
std::vector<pstree_clause> ast_to_pstree_dnf(const struct ast_node* node,
                                              const decimal_scale_map& decimalScales);

} // namespace sidecar
