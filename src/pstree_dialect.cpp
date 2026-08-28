#include "pstree_dialect.hpp"
#include "matching_engine.hpp"

// ast.h/value.h wrap their own declarations in `#ifdef __cplusplus extern "C" {` internally -
// no outer wrapper needed (and wrapping one here would also catch memoize.h's earlier,
// unrelated include in an extern "C" block it isn't meant for - see matching_engine.cpp's own
// tree.h include comment for the identical issue).
#include <ast.h>
#include <value.h>

#include <string>

namespace sidecar {

namespace {

// One partial result of the DNF walk: a list of AND-only clauses, ORed together. An empty
// `clauses` vector means "always false" (nothing can satisfy this branch); a `clauses`
// containing exactly one EMPTY clause means "always true" (no constraint at all, satisfied
// by every event) - both arise from a literal `true`/`false` (or its negation) somewhere in
// the expression, which do occur in practice (e.g. a subscription temporarily disabled by
// rewriting its expression to "false" rather than removed).
struct dnf_result {
    std::vector<pstree_clause> clauses;
};

// Generous but real: an expression nesting ORs inside ANDs multiplies clause counts
// together (e.g. (a or b) and (c or d) and (e or f) already yields 8) - this catches a
// pathological expression before it silently spends unbounded time/memory on
// PSTDynamic::insertSubscription() calls, one per clause.
constexpr std::size_t kMaxDnfClauses = 4096;

void check_clause_cap(std::size_t count) {
    if (count > kMaxDnfClauses) {
        throw matching_engine_error(
            "pstree: expression's OR/AND expansion exceeds " + std::to_string(kMaxDnfClauses) +
            " clauses - too large to index");
    }
}

dnf_result cross_product(const dnf_result& a, const dnf_result& b) {
    dnf_result out;
    out.clauses.reserve(a.clauses.size() * b.clauses.size());
    for (const auto& ca : a.clauses) {
        for (const auto& cb : b.clauses) {
            pstree_clause combined = ca;
            combined.insert(combined.end(), cb.begin(), cb.end());
            out.clauses.push_back(std::move(combined));
        }
    }
    check_clause_cap(out.clauses.size());
    return out;
}

dnf_result union_of(dnf_result a, dnf_result b) {
    a.clauses.insert(a.clauses.end(), std::make_move_iterator(b.clauses.begin()),
                      std::make_move_iterator(b.clauses.end()));
    check_clause_cap(a.clauses.size());
    return a;
}

dnf_result single_predicate(pstree::SubPredicate pred) {
    dnf_result out;
    out.clauses.push_back({std::move(pred)});
    return out;
}

dnf_result always(bool value) {
    dnf_result out;
    if (value) out.clauses.push_back({}); // one empty clause: trivially true
    // else: zero clauses, trivially false
    return out;
}

dnf_result to_dnf(const struct ast_node* node, bool negate);

dnf_result to_dnf_bool_expr(const struct ast_bool_expr& b, bool negate) {
    switch (b.op) {
        case AST_BOOL_AND: {
            auto lhs = to_dnf(b.binary.lhs, negate);
            auto rhs = to_dnf(b.binary.rhs, negate);
            // De Morgan: NOT(A and B) = NOT(A) or NOT(B) - OR'd, not cross-multiplied.
            return negate ? union_of(std::move(lhs), std::move(rhs))
                          : cross_product(lhs, rhs);
        }
        case AST_BOOL_OR: {
            auto lhs = to_dnf(b.binary.lhs, negate);
            auto rhs = to_dnf(b.binary.rhs, negate);
            // De Morgan: NOT(A or B) = NOT(A) and NOT(B) - cross-multiplied, not OR'd.
            return negate ? cross_product(lhs, rhs)
                          : union_of(std::move(lhs), std::move(rhs));
        }
        case AST_BOOL_NOT:
            return to_dnf(b.unary.expr, !negate);
        case AST_BOOL_VARIABLE: {
            // A bare boolean identifier ("flag") means "flag = true"; negated, "flag =
            // false" - both directly representable as kEq, no need for kNe here.
            pstree::SubPredicate pred;
            pred.attr = b.variable.attr;
            pred.op = pstree::CmpOp::kEq;
            pred.vals = {pstree::Value(!negate)};
            return single_predicate(std::move(pred));
        }
        case AST_BOOL_LITERAL:
            return always(negate ? !b.literal : b.literal);
    }
    throw matching_engine_error("pstree: unreachable ast_bool_e");
}

dnf_result to_dnf_compare_expr(const struct ast_compare_expr& c, bool negate) {
    pstree::CmpOp op;
    switch (c.op) {
        // De Morgan on ordering: NOT(x<v)=x>=v, NOT(x<=v)=x>v, NOT(x>v)=x<=v, NOT(x>=v)=x<v.
        case AST_COMPARE_LT: op = negate ? pstree::CmpOp::kGe : pstree::CmpOp::kLt; break;
        case AST_COMPARE_LE: op = negate ? pstree::CmpOp::kGt : pstree::CmpOp::kLe; break;
        case AST_COMPARE_GT: op = negate ? pstree::CmpOp::kLe : pstree::CmpOp::kGt; break;
        case AST_COMPARE_GE: op = negate ? pstree::CmpOp::kLt : pstree::CmpOp::kGe; break;
        default: throw matching_engine_error("pstree: unreachable ast_compare_e");
    }
    pstree::Value val = (c.value.value_type == AST_COMPARE_VALUE_INTEGER)
        ? pstree::Value(c.value.integer_value)
        : pstree::Value(c.value.float_value);
    pstree::SubPredicate pred{c.attr_var.attr, op, {val}};
    return single_predicate(std::move(pred));
}

dnf_result to_dnf_equality_expr(const struct ast_equality_expr& e, bool negate) {
    if (e.value.value_type == AST_EQUALITY_VALUE_INTEGER_ENUM) {
        throw matching_engine_error("pstree: integer-enum equality values are not supported");
    }
    pstree::CmpOp op;
    if (e.op == AST_EQUALITY_EQ) op = negate ? pstree::CmpOp::kNe : pstree::CmpOp::kEq;
    else op = negate ? pstree::CmpOp::kEq : pstree::CmpOp::kNe;
    pstree::Value val;
    switch (e.value.value_type) {
        case AST_EQUALITY_VALUE_INTEGER: val = pstree::Value(e.value.integer_value); break;
        case AST_EQUALITY_VALUE_FLOAT: val = pstree::Value(e.value.float_value); break;
        case AST_EQUALITY_VALUE_STRING: val = pstree::Value(std::string(e.value.string_value.string)); break;
        default: throw matching_engine_error("pstree: unreachable ast_equality_value_e");
    }
    pstree::SubPredicate pred{e.attr_var.attr, op, {val}};
    return single_predicate(std::move(pred));
}

dnf_result to_dnf_set_expr(const struct ast_set_expr& s, bool negate) {
    if (s.left_value.value_type != AST_SET_LEFT_VALUE_VARIABLE) {
        throw matching_engine_error("pstree: 'in'/'not in' with a literal (not an attribute) on the left is not supported");
    }
    bool is_in = (s.op == AST_SET_IN);
    pstree::CmpOp op = (is_in != negate) ? pstree::CmpOp::kElemOf : pstree::CmpOp::kNotElemOf;
    std::vector<pstree::Value> vals;
    switch (s.right_value.value_type) {
        case AST_SET_RIGHT_VALUE_INTEGER_LIST: {
            const auto* list = s.right_value.integer_list_value;
            vals.reserve(list->count);
            for (std::size_t i = 0; i < list->count; ++i) vals.push_back(pstree::Value(list->integers[i]));
            break;
        }
        case AST_SET_RIGHT_VALUE_STRING_LIST: {
            const auto* list = s.right_value.string_list_value;
            vals.reserve(list->count);
            for (std::size_t i = 0; i < list->count; ++i) vals.push_back(pstree::Value(std::string(list->strings[i].string)));
            break;
        }
        case AST_SET_RIGHT_VALUE_VARIABLE:
            throw matching_engine_error("pstree: 'in'/'not in' against another attribute (not a literal list) is not supported");
    }
    pstree::SubPredicate pred{s.left_value.variable_value.attr, op, std::move(vals)};
    return single_predicate(std::move(pred));
}

dnf_result to_dnf_is_null_expr(const struct ast_is_null_expr& n, bool negate) {
    if (n.type == AST_IS_EMPTY) {
        throw matching_engine_error("pstree: 'is empty' (list-valued attributes) is not supported");
    }
    bool is_null = (n.type == AST_IS_NULL);
    pstree::CmpOp op = (is_null != negate) ? pstree::CmpOp::kIsNull : pstree::CmpOp::kIsNotNull;
    pstree::SubPredicate pred{n.attr_var.attr, op, {}};
    return single_predicate(std::move(pred));
}

dnf_result to_dnf(const struct ast_node* node, bool negate) {
    switch (node->type) {
        case AST_TYPE_BOOL_EXPR: return to_dnf_bool_expr(node->bool_expr, negate);
        case AST_TYPE_COMPARE_EXPR: return to_dnf_compare_expr(node->compare_expr, negate);
        case AST_TYPE_EQUALITY_EXPR: return to_dnf_equality_expr(node->equality_expr, negate);
        case AST_TYPE_SET_EXPR: return to_dnf_set_expr(node->set_expr, negate);
        case AST_TYPE_LIST_EXPR:
            throw matching_engine_error("pstree: list-attribute operators (one of/none of/all of) are not supported");
        case AST_TYPE_SPECIAL_EXPR:
            throw matching_engine_error("pstree: special expressions (frequency caps/segments/geo/string pattern) are not supported");
        case AST_TYPE_IS_NULL_EXPR: return to_dnf_is_null_expr(node->is_null_expr, negate);
    }
    throw matching_engine_error("pstree: unreachable ast_node_type_e");
}

} // namespace

std::vector<pstree_clause> ast_to_pstree_dnf(const struct ast_node* node) {
    dnf_result result = to_dnf(node, false);
    return std::move(result.clauses);
}

} // namespace sidecar
