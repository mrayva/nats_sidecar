// Correctness bridge for the string-vs-integer matching-cost experiment (see
// nyse-matrix/translate_to_int_attrs.py and sql/build_intattrs_lookup.sql). Before trusting any
// throughput delta between a string-attribute expression file and its translated integer-
// attribute equivalent, this confirms the translation actually preserved semantics exactly:
//
//   1. Cross-engine agreement on the STRING-schema tree (exchange/symbol as strings) - same
//      check diag_engine_diff already does, repeated here as a baseline.
//   2. Cross-engine agreement on the INT-schema tree (exchange_id/symbol_id as integers).
//   3. Match-COUNT equality between the string tree and the int tree, per sample row - if the
//      translation script had any bug (wrong lookup id, an off-by-one, a dropped clause), this
//      is what would catch it: the two trees hold different-looking but semantically-identical
//      subscription populations, so every real sample row must match exactly the same NUMBER of
//      subscriptions in both (not the same ids - ids are assigned by expression file position
//      and both files are byte-for-byte parallel anyway, so same ids too, checked as a bonus).
//
// Usage: diag_int_attrs <string_expr_file> <int_expr_file>
#include "matching_engine.hpp"

#include <cstdio>
#include <fstream>
#include <set>
#include <string>
#include <vector>

using namespace sidecar;

namespace {

std::vector<std::string> read_exprs(const std::string& path) {
    std::vector<std::string> out;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (!line.empty()) out.push_back(line);
    }
    return out;
}

struct sample_row {
    std::string exchange;
    std::string symbol;
    int64_t exchange_id;
    int64_t symbol_id;
    int64_t trade_volume;
    double trade_price;
};

// Same 19 real (exchange, symbol, trade_volume, trade_price) rows diag_engine_diff.cpp uses,
// plus their real exchange_id/symbol_id from exchange_lookup/symbol_lookup (see
// sql/build_intattrs_lookup.sql) - fetched directly via psql against the real lookup tables, not
// invented.
std::vector<sample_row> sample_rows() {
    return {
        {"A", "ACCO", 1, 84, 100, 118.65}, {"B", "XOP", 2, 11716, 70, 5.77},
        {"C", "SVC", 3, 10155, 100, 24.71}, {"D", "AXTI", 4, 925, 100, 22.1175},
        {"G", "XRT", 5, 11748, 100, 122.49}, {"H", "SQQQ", 6, 9995, 100, 55.87},
        {"J", "GDXJ", 7, 4379, 17, 37.64}, {"K", "ZYME", 8, 11951, 1, 46.68},
        {"L", "MEC", 9, 6715, 1, 150.26}, {"M", "EVO", 10, 3566, 43, 13.25},
        {"N", "XPO", 11, 11728, 3, 63.73}, {"P", "LVRO", 12, 6527, 10, 1.23},
        {"Q", "KOPN", 13, 6113, 33, 101.93}, {"T", "ZSL", 14, 11927, 100, 118.48},
        {"U", "MSTU", 15, 7064, 400, 4.1}, {"V", "INFY", 16, 5482, 500, 211.15},
        {"X", "XPEV", 17, 11724, 12, 90.13}, {"Y", "OKE", 18, 7768, 13, 349.16},
        {"Z", "DY", 19, 3090, 4, 326.39},
    };
}

std::set<uint64_t> search_str_row(matching_engine& engine, const sample_row& row) {
    auto ev = engine.make_event();
    ev->with_string("exchange", row.exchange);
    ev->with_string("symbol", row.symbol);
    ev->with_integer("trade_volume", row.trade_volume);
    ev->with_float("trade_price", row.trade_price);
    auto matched = engine.search(*ev);
    return std::set<uint64_t>(matched.begin(), matched.end());
}

std::set<uint64_t> search_int_row(matching_engine& engine, const sample_row& row) {
    auto ev = engine.make_event();
    ev->with_integer("exchange_id", row.exchange_id);
    ev->with_integer("symbol_id", row.symbol_id);
    ev->with_integer("trade_volume", row.trade_volume);
    ev->with_float("trade_price", row.trade_price);
    auto matched = engine.search(*ev);
    return std::set<uint64_t>(matched.begin(), matched.end());
}

std::vector<std::unique_ptr<matching_engine>> build_all(const std::vector<attribute_def>& attrs,
                                                         const std::vector<std::string>& exprs,
                                                         const char* label) {
    std::vector<std::pair<engine_type, const char*>> engines = {
        {engine_type::atree, "atree"}, {engine_type::betree, "betree"}, {engine_type::pstree, "pstree"},
    };
    std::vector<std::unique_ptr<matching_engine>> trees;
    for (auto& [type, name] : engines) {
        auto tree = build_matching_engine(type, attrs);
        uint64_t id = 1;
        std::size_t rejected = 0;
        for (const auto& e : exprs) {
            try { tree->insert(id, e); } catch (const std::exception&) { ++rejected; }
            ++id;
        }
        std::fprintf(stderr, "  [%s] %s: inserted %zu, rejected %zu\n", label, name,
                     exprs.size() - rejected, rejected);
        trees.push_back(std::move(tree));
    }
    return trees;
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: diag_int_attrs <string_expr_file> <int_expr_file>\n");
        return 2;
    }
    auto str_exprs = read_exprs(argv[1]);
    auto int_exprs = read_exprs(argv[2]);
    if (str_exprs.size() != int_exprs.size()) {
        std::fprintf(stderr, "error: string file has %zu lines, int file has %zu - not "
                              "byte-for-byte parallel, translation is suspect\n",
                     str_exprs.size(), int_exprs.size());
        return 2;
    }
    std::fprintf(stderr, "loaded %zu expressions (string + int, parallel)\n\n", str_exprs.size());

    std::vector<attribute_def> str_attrs = {
        {"exchange", attribute_type::string}, {"symbol", attribute_type::string},
        {"trade_volume", attribute_type::integer}, {"trade_price", attribute_type::float_val},
    };
    std::vector<attribute_def> int_attrs = {
        {"exchange_id", attribute_type::integer}, {"symbol_id", attribute_type::integer},
        {"trade_volume", attribute_type::integer}, {"trade_price", attribute_type::float_val},
    };

    std::fprintf(stderr, "=== insert() acceptance ===\n");
    auto str_trees = build_all(str_attrs, str_exprs, "string");
    auto int_trees = build_all(int_attrs, int_exprs, "int   ");
    std::fprintf(stderr, "\n");

    auto rows = sample_rows();
    int str_disagreements = 0, int_disagreements = 0, cross_mismatches = 0;

    std::fprintf(stderr, "=== per-row cross-checks ===\n");
    for (const auto& row : rows) {
        std::vector<std::set<uint64_t>> str_results, int_results;
        for (auto& t : str_trees) str_results.push_back(search_str_row(*t, row));
        for (auto& t : int_trees) int_results.push_back(search_int_row(*t, row));

        bool str_agree = str_results[0] == str_results[1] && str_results[1] == str_results[2];
        bool int_agree = int_results[0] == int_results[1] && int_results[1] == int_results[2];
        bool cross_match = str_results[0] == int_results[0]; // ids are file-position-parallel
        if (!str_agree) ++str_disagreements;
        if (!int_agree) ++int_disagreements;
        if (!cross_match) ++cross_mismatches;

        std::fprintf(stderr,
                     "%s/%s: str(atree=%zu betree=%zu pstree=%zu %s)  "
                     "int(atree=%zu betree=%zu pstree=%zu %s)  cross=%s\n",
                     row.exchange.c_str(), row.symbol.c_str(), str_results[0].size(),
                     str_results[1].size(), str_results[2].size(), str_agree ? "AGREE" : "DISAGREE",
                     int_results[0].size(), int_results[1].size(), int_results[2].size(),
                     int_agree ? "AGREE" : "DISAGREE", cross_match ? "MATCH" : "MISMATCH");
    }

    std::fprintf(stderr, "\n=== summary ===\n");
    std::fprintf(stderr, "  string-schema cross-engine disagreements: %d/%zu\n", str_disagreements,
                 rows.size());
    std::fprintf(stderr, "  int-schema cross-engine disagreements: %d/%zu\n", int_disagreements,
                 rows.size());
    std::fprintf(stderr, "  string-vs-int translation mismatches: %d/%zu\n", cross_mismatches,
                 rows.size());

    bool ok = str_disagreements == 0 && int_disagreements == 0 && cross_mismatches == 0;
    if (ok) {
        std::fprintf(stderr, "\nOK: string and int workloads are verified semantically "
                              "equivalent - any throughput delta measured between them reflects "
                              "attribute-type cost only.\n");
    }
    return ok ? 0 : 1;
}
