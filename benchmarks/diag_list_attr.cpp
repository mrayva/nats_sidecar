// Diagnostic tool for the list-valued-attribute blind spot deliberately left out of every other
// benchmark in this project (matching_engine_bench.cpp, diag_engine_diff.cpp, perf_search_loop.cpp
// all say so explicitly): pstree has no representation for list-valued attributes at all. This is
// NOT a throughput comparison (a 3-way comparison would be meaningless when one engine structurally
// can't participate) - it demonstrates and cross-checks the actual blind spot at both of its two
// distinct rejection surfaces:
//
//   1. Parse-time: ast_to_pstree_dnf() throws on any "one of"/"none of"/"all of" expression
//      (AST_TYPE_LIST_EXPR), regardless of whether the referenced attribute is even declared
//      list-typed in the schema - see pstree_dialect.cpp.
//   2. Event-time: pstree's event_sink::with_string_list()/with_integer_list() throw
//      unconditionally - see matching_engine.cpp - so even a pstree tree holding only
//      list-attribute-free subscriptions can't have a list-typed attribute populated on it.
//
// atree and betree both handle list attributes correctly but disagree on what "all of" itself
// means (a-tree: event's list is a SUBSET of the literal list; be-tree: event's list is a
// SUPERSET of the literal list - be-tree's reading is treated as canonical project-wide and
// dialect.cpp rewrites "X all of (...)" into an equivalent "one of" conjunction before handing it
// to a-tree - see dialect.hpp's own comment). tests/test_matching_engine_differential.cpp already
// covers this reconciliation with 12 small hand-picked cases; every other real bug this project
// has found in a-tree/be-tree's matching (not parsing) surfaced only "at scale" with many
// subscriptions sharing structure, never in a small hand-picked test - so this tool additionally
// cross-checks atree vs. betree agreement across a much larger, generated list-attribute
// subscription population, the same methodology diag_engine_diff already used successfully for
// scalar/range/OR predicates.
//
// Usage: diag_list_attr [count: subscription count, default 3000] [seed: default 42]
#include "matching_engine.hpp"
#include "config.hpp"

#include <algorithm>
#include <cstdio>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace sidecar;

namespace {

// A modest, real-ish universe - large enough that "one of"/"all of" clauses genuinely overlap
// and share structure across many subscriptions (the condition every prior at-scale atree/betree
// bug in this project needed to manifest), small enough that random small subsets collide often.
const std::vector<std::string> kTagUniverse = {
    "tech", "energy", "finance", "healthcare", "industrial", "materials", "utilities",
    "consumer", "telecom", "reit", "biotech", "semis", "retail", "airlines", "shipping",
    "mining", "defense", "media", "insurance", "banks",
};
const std::vector<int64_t> kScoreUniverse = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

std::vector<attribute_def> list_attributes() {
    return {
        {"symbol", attribute_type::string},
        {"tags",   attribute_type::string_list},
        {"scores", attribute_type::integer_list},
    };
}

template <class T>
std::vector<T> sample_subset(std::mt19937& rng, const std::vector<T>& universe, std::size_t n) {
    std::vector<T> pool = universe;
    std::shuffle(pool.begin(), pool.end(), rng);
    n = std::min(n, pool.size());
    return std::vector<T>(pool.begin(), pool.begin() + n);
}

std::string quote_string_list(const std::vector<std::string>& vals) {
    std::ostringstream os;
    for (std::size_t i = 0; i < vals.size(); ++i) {
        if (i) os << ", ";
        os << '"' << vals[i] << '"';
    }
    return os.str();
}

std::string int_list(const std::vector<int64_t>& vals) {
    std::ostringstream os;
    for (std::size_t i = 0; i < vals.size(); ++i) {
        if (i) os << ", ";
        os << vals[i];
    }
    return os.str();
}

// One generated list-attribute expression, e.g. `tags one of ("tech", "energy")` or
// `scores all of (3, 7)`. Deliberately never mixes tags/scores clauses with AND/OR in the same
// expression - the point is stressing the list-attribute machinery itself at scale, not
// re-exercising the AND/OR precedence already covered by the blind-spot benchmark.
std::string gen_list_expr(std::mt19937& rng) {
    std::uniform_int_distribution<int> kind_dist(0, 2); // one of / none of / all of
    std::uniform_int_distribution<int> attr_dist(0, 1); // tags / scores
    const char* kinds[] = {"one of", "none of", "all of"};
    const char* kind = kinds[kind_dist(rng)];

    if (attr_dist(rng) == 0) {
        std::uniform_int_distribution<std::size_t> width_dist(1, 4);
        auto vals = sample_subset(rng, kTagUniverse, width_dist(rng));
        return "tags " + std::string(kind) + " (" + quote_string_list(vals) + ")";
    }
    std::uniform_int_distribution<std::size_t> width_dist(1, 4);
    auto vals = sample_subset(rng, kScoreUniverse, width_dist(rng));
    return "scores " + std::string(kind) + " (" + int_list(vals) + ")";
}

struct sample_row {
    std::string symbol;
    std::vector<std::string> tags;
    std::vector<int64_t> scores;
};

std::vector<sample_row> gen_sample_rows(std::mt19937& rng, std::size_t n) {
    std::vector<sample_row> rows;
    std::uniform_int_distribution<std::size_t> width_dist(0, 5);
    for (std::size_t i = 0; i < n; ++i) {
        sample_row row;
        row.symbol = "SYM" + std::to_string(i);
        row.tags = sample_subset(rng, kTagUniverse, width_dist(rng));
        row.scores = sample_subset(rng, kScoreUniverse, width_dist(rng));
        rows.push_back(std::move(row));
    }
    return rows;
}

std::set<uint64_t> search_row(matching_engine& engine, const sample_row& row) {
    auto ev = engine.make_event();
    ev->with_string("symbol", row.symbol);
    ev->with_string_list("tags", row.tags);
    ev->with_integer_list("scores", row.scores);
    auto matched = engine.search(*ev);
    return std::set<uint64_t>(matched.begin(), matched.end());
}

} // namespace

int main(int argc, char** argv) {
    std::size_t count = argc > 1 ? std::stoul(argv[1]) : 3000;
    unsigned seed = argc > 2 ? static_cast<unsigned>(std::stoul(argv[2])) : 42;
    std::mt19937 rng(seed);

    std::vector<std::string> exprs;
    exprs.reserve(count);
    for (std::size_t i = 0; i < count; ++i) exprs.push_back(gen_list_expr(rng));

    std::fprintf(stderr, "generated %zu list-attribute expressions (tags/scores, one of/none "
                          "of/all of)\n\n", count);

    // --- Part 1: parse-time rejection surface (ast_to_pstree_dnf's AST_TYPE_LIST_EXPR throw) ---
    std::fprintf(stderr, "=== Part 1: insert()-time acceptance ===\n");
    auto atree_tree = build_matching_engine(engine_type::atree, list_attributes());
    auto betree_tree = build_matching_engine(engine_type::betree, list_attributes());
    auto pstree_tree = build_matching_engine(engine_type::pstree, list_attributes());

    std::size_t atree_rejected = 0, betree_rejected = 0, pstree_rejected = 0;
    uint64_t id = 1;
    for (const auto& e : exprs) {
        try { atree_tree->insert(id, e); } catch (const std::exception&) { ++atree_rejected; }
        try { betree_tree->insert(id, e); } catch (const std::exception&) { ++betree_rejected; }
        try { pstree_tree->insert(id, e); } catch (const std::exception&) { ++pstree_rejected; }
        ++id;
    }
    std::fprintf(stderr, "  atree:  inserted %zu, rejected %zu\n", count - atree_rejected, atree_rejected);
    std::fprintf(stderr, "  betree: inserted %zu, rejected %zu\n", count - betree_rejected, betree_rejected);
    std::fprintf(stderr, "  pstree: inserted %zu, rejected %zu %s\n\n", count - pstree_rejected,
                 pstree_rejected,
                 pstree_rejected == count ? "(confirmed: 100% structural rejection)" : "(UNEXPECTED - see below)");
    if (pstree_rejected != count) {
        std::fprintf(stderr, "  WARNING: pstree accepted %zu list-attribute expression(s) it "
                              "should have structurally rejected - this contradicts the documented "
                              "ast_to_pstree_dnf() behavior and needs investigation.\n\n",
                     count - pstree_rejected);
    }

    // --- Part 2: event-time rejection surface (with_string_list/with_integer_list) ---
    // Independent of Part 1: even a pstree tree holding zero list-attribute *subscriptions* still
    // can't have a list-typed attribute *populated* on an event, because the schema itself
    // declares tags/scores as list-typed and event population always visits every declared
    // attribute (see event_bridge.hpp) - this is what actually breaks a real pstree deployment
    // the moment its schema includes any list-valued column, not just when a client subscribes
    // with a list-operator expression.
    std::fprintf(stderr, "=== Part 2: event-time (with_string_list/with_integer_list) ===\n");
    auto pstree_empty = build_matching_engine(engine_type::pstree, list_attributes());
    pstree_empty->insert(1, "symbol in (\"SYM0\")"); // one ordinary, list-free subscription
    bool pstree_event_threw = false;
    try {
        auto ev = pstree_empty->make_event();
        ev->with_string("symbol", "SYM0");
        ev->with_string_list("tags", {"tech"});
        ev->with_integer_list("scores", {1});
    } catch (const std::exception& ex) {
        pstree_event_threw = true;
        std::fprintf(stderr, "  pstree: with_string_list/with_integer_list threw as expected: %s\n",
                     ex.what());
    }
    if (!pstree_event_threw) {
        std::fprintf(stderr, "  WARNING: pstree's event_sink accepted list values without "
                              "throwing - this contradicts the documented behavior.\n");
    }
    std::fprintf(stderr, "\n");

    // --- Part 3: atree vs. betree cross-check at scale ---
    // pstree is excluded here (Part 1 already established it accepts none of these expressions,
    // so it has nothing to search against) - this is specifically re-running diag_engine_diff's
    // own "small hand-picked tests miss scale-dependent bugs" methodology against the one
    // predicate family (list attributes) that tool never covered.
    std::fprintf(stderr, "=== Part 3: atree vs. betree agreement at scale (%zu subscriptions, "
                          "%zu inserted into each) ===\n", count, count - atree_rejected);
    auto rows = gen_sample_rows(rng, 200);
    int disagreements = 0;
    for (const auto& row : rows) {
        auto atree_result = search_row(*atree_tree, row);
        auto betree_result = search_row(*betree_tree, row);
        if (atree_result != betree_result) {
            ++disagreements;
            std::fprintf(stderr, "  DISAGREE symbol=%s tags=[%s] scores=[%s]: atree=%zu betree=%zu\n",
                         row.symbol.c_str(), quote_string_list(row.tags).c_str(),
                         int_list(row.scores).c_str(), atree_result.size(), betree_result.size());
            std::vector<uint64_t> only_atree, only_betree;
            std::set_difference(atree_result.begin(), atree_result.end(), betree_result.begin(),
                                 betree_result.end(), std::back_inserter(only_atree));
            std::set_difference(betree_result.begin(), betree_result.end(), atree_result.begin(),
                                 atree_result.end(), std::back_inserter(only_betree));
            for (std::size_t k = 0; k < std::min<std::size_t>(3, only_atree.size()); ++k) {
                std::fprintf(stderr, "    in atree but not betree: expr=%s\n",
                             exprs[only_atree[k] - 1].c_str());
            }
            for (std::size_t k = 0; k < std::min<std::size_t>(3, only_betree.size()); ++k) {
                std::fprintf(stderr, "    in betree but not atree: expr=%s\n",
                             exprs[only_betree[k] - 1].c_str());
            }
        }
    }
    std::fprintf(stderr, "  %d/%zu sample rows disagreed\n\n", disagreements, rows.size());

    std::fprintf(stderr, "=== summary ===\n");
    std::fprintf(stderr, "  pstree list-attribute blind spot confirmed at both surfaces: "
                          "insert()-time (%zu/%zu rejected) and event-time (%s).\n",
                 pstree_rejected, count, pstree_event_threw ? "throws as documented" : "DID NOT THROW");
    std::fprintf(stderr, "  atree/betree agreement at scale: %d/%zu disagreed.\n", disagreements,
                 rows.size());

    bool ok = (pstree_rejected == count) && pstree_event_threw && (disagreements == 0) &&
              (atree_rejected == 0) && (betree_rejected == 0);
    return ok ? 0 : 1;
}
