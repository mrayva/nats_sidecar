// Cross-engine differential checker: loads a REAL expression file (e.g. one of the
// exchange/symbol set-membership benchmark files under nyse-matrix/logs-setmember/), inserts
// every expression into all three engines, then searches each with a handful of real sample
// (exchange, symbol) rows and confirms atree/betree/pstree agree on the exact set of matched
// subscription ids for every row.
//
// This exists because the three existing correctness safety nets - matching_engine_differential
// (46 gtest cases, small hand-picked expressions) and each engine's own unit tests - all missed a
// real bug: atree silently under-matched `attribute in (...)` at real subscription-count scale
// (found here, fixed in mrayva/a-tree - see that repo's commit history for the bug and fix). Small
// hand-written test expressions can't reproduce a bug that only manifests once enough real
// subscriptions accumulate enough distinct, cross-referencing string values - this tool runs the
// exact real workload instead. Kept as a reusable tool for the next such investigation, not a
// one-off.
//
// Usage: diag_engine_diff <expr_file> [rows_file: exchange,symbol per line]
//   (rows_file optional - defaults to a real 19-row sample, one per NYSE exchange letter,
//   originally pulled via `psql ... TABLESAMPLE SYSTEM (2)` against the live table)
#include "matching_engine.hpp"
#include "config.hpp"

#include <algorithm>
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

std::set<uint64_t> search_row(matching_engine& engine, const std::string& exchange,
                               const std::string& symbol) {
    auto ev = engine.make_event();
    ev->with_string("exchange", exchange);
    ev->with_string("symbol", symbol);
    auto matched = engine.search(*ev);
    return std::set<uint64_t>(matched.begin(), matched.end());
}

} // namespace

int main(int argc, char** argv) {
    std::string expr_path = argc > 1 ? argv[1] : "";
    std::string rows_path = argc > 2 ? argv[2] : "";
    if (expr_path.empty()) {
        std::fprintf(stderr, "usage: diag_engine_diff <expr_file> [rows_file: exchange,symbol per line]\n");
        return 2;
    }
    auto exprs = read_exprs(expr_path);
    std::fprintf(stderr, "loaded %zu expressions from %s\n", exprs.size(), expr_path.c_str());

    std::vector<attribute_def> attrs = {
        {"exchange", attribute_type::string},
        {"symbol", attribute_type::string},
    };

    std::vector<std::pair<engine_type, const char*>> engines = {
        {engine_type::atree, "atree"},
        {engine_type::betree, "betree"},
        {engine_type::pstree, "pstree"},
    };

    std::vector<std::unique_ptr<matching_engine>> trees;
    for (auto& [type, name] : engines) {
        std::fprintf(stderr, "building %s...\n", name);
        auto tree = build_matching_engine(type, attrs);
        uint64_t id = 1;
        std::size_t rejected = 0;
        for (const auto& e : exprs) {
            try {
                tree->insert(id, e);
            } catch (const std::exception& ex) {
                ++rejected;
            }
            ++id;
        }
        std::fprintf(stderr, "  %s: inserted %zu, rejected %zu\n", name,
                     exprs.size() - rejected, rejected);
        trees.push_back(std::move(tree));
    }

    // Real (exchange, symbol) pairs sampled directly from the actual published table (one per
    // real distinct exchange letter).
    std::vector<std::pair<std::string, std::string>> rows = {
        {"A", "AKAN"}, {"B", "ZTS"}, {"C", "IWM"}, {"D", "USB"}, {"G", "MDLZ"},
        {"H", "NVDA"}, {"J", "IAU"}, {"K", "SOXL"}, {"L", "ANIP"}, {"M", "ZTS"},
        {"N", "ET"}, {"P", "UNH"}, {"Q", "MRVL"}, {"T", "NYT"}, {"U", "BBAI"},
        {"V", "TREX"}, {"X", "IREN"}, {"Y", "ZURA"}, {"Z", "FLNC"},
    };
    if (!rows_path.empty()) {
        rows.clear();
        std::ifstream f(rows_path);
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            auto comma = line.find(',');
            if (comma == std::string::npos) continue;
            rows.emplace_back(line.substr(0, comma), line.substr(comma + 1));
        }
    }

    int disagreements = 0;
    for (auto& [exch, sym] : rows) {
        std::vector<std::set<uint64_t>> results;
        for (std::size_t i = 0; i < trees.size(); ++i) {
            results.push_back(search_row(*trees[i], exch, sym));
        }
        bool all_equal = std::all_of(results.begin() + 1, results.end(),
                                      [&](const auto& s) { return s == results[0]; });
        std::fprintf(stderr, "%s/%s: atree=%zu betree=%zu pstree=%zu  %s\n",
                     exch.c_str(), sym.c_str(), results[0].size(), results[1].size(),
                     results[2].size(), all_equal ? "AGREE" : "DISAGREE");
        if (!all_equal) {
            ++disagreements;
            // Print a few ids present in one but not another.
            for (std::size_t i = 0; i < results.size(); ++i) {
                for (std::size_t j = 0; j < results.size(); ++j) {
                    if (i == j) continue;
                    std::vector<uint64_t> only_i;
                    std::set_difference(results[i].begin(), results[i].end(),
                                         results[j].begin(), results[j].end(),
                                         std::back_inserter(only_i));
                    if (!only_i.empty()) {
                        std::fprintf(stderr, "  in %s but not %s: %zu ids, first few: ",
                                     engines[i].second, engines[j].second, only_i.size());
                        for (std::size_t k = 0; k < std::min<std::size_t>(5, only_i.size()); ++k) {
                            std::fprintf(stderr, "%llu(expr=%s) ",
                                         static_cast<unsigned long long>(only_i[k]),
                                         exprs[only_i[k] - 1].substr(0, 60).c_str());
                        }
                        std::fprintf(stderr, "\n");
                    }
                }
            }
        }
    }

    std::fprintf(stderr, "\n%d/%zu rows disagreed across engines\n", disagreements, rows.size());
    return disagreements > 0 ? 1 : 0;
}
