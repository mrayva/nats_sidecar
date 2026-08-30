// Cross-engine differential checker: loads a REAL expression file (e.g. one of the
// exchange/symbol(/trade_volume) set-membership benchmark files under
// nyse-matrix/logs-setmember/), inserts every expression into all three engines, then searches
// each with a handful of real sample (exchange, symbol, trade_volume) rows and confirms
// atree/betree/pstree agree on the exact set of matched subscription ids for every row.
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
// trade_volume (added 2026-08-29, alongside gen_set_membership_subs.py's own --volume-fraction):
// every row now also carries a real "Trade Volume" sample value, so this tool can validate the
// new `trade_volume >= lo and trade_volume <= hi` range-predicate shape (including volume-only
// subscriptions) across all three engines, not just the atree-only manual smoke test that
// exercised it first.
//
// trade_price (added 2026-08-30, alongside gen_set_membership_subs.py's own --price-fraction):
// same idea as trade_volume, against the real "Trade Price" column - a FLOAT attribute, so this
// also validates the double-precision comparison path (vs. trade_volume's int64_t one) across
// all three engines.
//
// narrow_metric (added 2026-08-30, alongside gen_blindspot_subs.py's own --narrow-fraction):
// a SYNTHETIC float attribute (no real NYSE column has this narrow a domain - see that script's
// own docs) with a deliberately tiny real-world range ([-0.01, 0.01]), targeting a real
// upstream-documented be-tree weakness (narrow float domains split poorly with its integer-style
// partitioning). Values here are computed with the exact same per-symbol hash formula
// run_cycle_blindspot.sh's own publish SQL uses, so they're real (deterministic, not made up),
// just not sourced from the live table.
//
// Usage: diag_engine_diff <expr_file> [rows_file: exchange,symbol,trade_volume,trade_price,narrow_metric per line]
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

struct sample_row {
    std::string exchange;
    std::string symbol;
    int64_t trade_volume;
    double trade_price;
    double narrow_metric;
};

std::set<uint64_t> search_row(matching_engine& engine, const sample_row& row) {
    auto ev = engine.make_event();
    ev->with_string("exchange", row.exchange);
    ev->with_string("symbol", row.symbol);
    ev->with_integer("trade_volume", row.trade_volume);
    ev->with_float("trade_price", row.trade_price);
    ev->with_float("narrow_metric", row.narrow_metric);
    auto matched = engine.search(*ev);
    return std::set<uint64_t>(matched.begin(), matched.end());
}

} // namespace

int main(int argc, char** argv) {
    std::string expr_path = argc > 1 ? argv[1] : "";
    std::string rows_path = argc > 2 ? argv[2] : "";
    if (expr_path.empty()) {
        std::fprintf(stderr, "usage: diag_engine_diff <expr_file> [rows_file: exchange,symbol,trade_volume,trade_price per line]\n");
        return 2;
    }
    auto exprs = read_exprs(expr_path);
    std::fprintf(stderr, "loaded %zu expressions from %s\n", exprs.size(), expr_path.c_str());

    std::vector<attribute_def> attrs = {
        {"exchange", attribute_type::string},
        {"symbol", attribute_type::string},
        {"trade_volume", attribute_type::integer},
        {"trade_price", attribute_type::float_val},
        {"narrow_metric", attribute_type::float_val},
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

    // Real (exchange, symbol, trade_volume, trade_price) quadruples sampled directly from the
    // actual published table (one per real distinct exchange letter) - trade_volume and
    // trade_price were sampled independently (different rows), so this isn't one single real
    // row per exchange, but every individual value is real.
    std::vector<sample_row> rows = {
        {"A", "ACCO", 100, 118.65, 0.003}, {"B", "XOP", 70, 5.77, 0.00839},
        {"C", "SVC", 100, 24.71, 0.009}, {"D", "AXTI", 100, 22.1175, 0.00594},
        {"G", "XRT", 100, 122.49, -0.00106}, {"H", "SQQQ", 100, 55.87, -0.0047},
        {"J", "GDXJ", 17, 37.64, 0.00366}, {"K", "ZYME", 1, 46.68, -0.00539},
        {"L", "MEC", 1, 150.26, 0.00928}, {"M", "EVO", 43, 13.25, 0.00855},
        {"N", "XPO", 3, 63.73, 0.00407}, {"P", "LVRO", 10, 1.23, -0.00424},
        {"Q", "KOPN", 33, 101.93, 0.00702}, {"T", "ZSL", 100, 118.48, -0.00869},
        {"U", "MSTU", 400, 4.1, 0.00715}, {"V", "INFY", 500, 211.15, -0.00234},
        {"X", "XPEV", 12, 90.13, -0.00615}, {"Y", "OKE", 13, 349.16, -0.00698},
        {"Z", "DY", 4, 326.39, -0.00632},
    };
    if (!rows_path.empty()) {
        rows.clear();
        std::ifstream f(rows_path);
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            auto comma1 = line.find(',');
            if (comma1 == std::string::npos) continue;
            auto comma2 = line.find(',', comma1 + 1);
            if (comma2 == std::string::npos) continue;
            auto comma3 = line.find(',', comma2 + 1);
            if (comma3 == std::string::npos) continue;
            auto comma4 = line.find(',', comma3 + 1);
            if (comma4 == std::string::npos) continue;
            rows.push_back({line.substr(0, comma1), line.substr(comma1 + 1, comma2 - comma1 - 1),
                             std::stoll(line.substr(comma2 + 1, comma3 - comma2 - 1)),
                             std::stod(line.substr(comma3 + 1, comma4 - comma3 - 1)),
                             std::stod(line.substr(comma4 + 1))});
        }
    }

    int disagreements = 0;
    for (auto& row : rows) {
        std::vector<std::set<uint64_t>> results;
        for (std::size_t i = 0; i < trees.size(); ++i) {
            results.push_back(search_row(*trees[i], row));
        }
        bool all_equal = std::all_of(results.begin() + 1, results.end(),
                                      [&](const auto& s) { return s == results[0]; });
        std::fprintf(stderr, "%s/%s/%lld/%.4f/%.5f: atree=%zu betree=%zu pstree=%zu  %s\n",
                     row.exchange.c_str(), row.symbol.c_str(),
                     static_cast<long long>(row.trade_volume), row.trade_price, row.narrow_metric,
                     results[0].size(), results[1].size(), results[2].size(),
                     all_equal ? "AGREE" : "DISAGREE");
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
