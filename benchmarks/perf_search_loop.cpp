// Perf-profiling target: builds ONE engine's matching_engine from a REAL expression file (e.g.
// one of the exchange/symbol set-membership benchmark files under nyse-matrix/logs-setmember/),
// then repeatedly calls search() against a small, cyclic set of real sample events for a fixed
// wall-clock duration - isolating pure matching_engine::search() cost with zero NATS/queue/
// publish/I-O noise, so `perf record -o file -- ./perf_search_loop ...` (launching and owning the
// process directly, no external attach/stop-signal needed at all - see nyse-matrix's own
// profile_fleet_instance*.sh scripts for why a live-fleet `-p PID` attach needs the `-- sleep N`
// companion-command form instead; this tool sidesteps that entirely by being self-bounding) gives
// a clean call graph for exactly one question: where does search() itself spend time, and why
// does that cost grow the way it does with K. Mirrors matching_engine_bench.cpp's own "isolate
// pure matching-engine cost from transport costs" rationale, just profiler-friendly instead of
// throughput-numbers-friendly. Used to find and verify two real fixes this way already: a-tree's
// string-sort bug and pstree's kElemOf/kNotElemOf linear-scan bug (see both repos' own commit
// history) - kept here as a reusable tool for the next one, not a one-off.
//
// Usage: perf_search_loop <atree|betree|pstree> <expr_file> [seconds=10] [--int-attrs]
//   perf record -F 999 --call-graph dwarf -o out.data -- \
//       ./bin/perf_search_loop betree path/to/exprs.txt 15
//   perf report -i out.data --stdio -g none | less
//
// --int-attrs (added 2026-08-30, for the string-vs-integer attribute matching-cost experiment):
// swaps the exchange/symbol schema from string-typed to integer-typed (exchange_id/symbol_id),
// matching nyse-matrix/translate_to_int_attrs.py's own output and sql/build_intattrs_lookup.sql's
// surrogate keys - point this at a *_intattrs expr file, not a plain string one, or every
// expression will fail to insert (undeclared exchange_id/symbol_id attributes vs. exchange/
// symbol). See diag_int_attrs.cpp for the correctness bridge confirming a string file and its
// translated int file are semantically identical before trusting any throughput delta measured
// between them here.
#include "matching_engine.hpp"
#include "config.hpp"
#include "match_timing.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
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

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: perf_search_loop <atree|betree|pstree> <expr_file> "
                              "[seconds=10] [--int-attrs]\n");
        return 2;
    }
    std::string engine_name = argv[1];
    std::string expr_path = argv[2];
    int seconds = 10;
    bool int_attrs = false;
    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--int-attrs") int_attrs = true;
        else seconds = std::atoi(argv[i]);
    }

    engine_type type;
    if (engine_name == "atree") type = engine_type::atree;
    else if (engine_name == "betree") type = engine_type::betree;
    else if (engine_name == "pstree") type = engine_type::pstree;
    else { std::fprintf(stderr, "unknown engine %s\n", engine_name.c_str()); return 2; }

    auto exprs = read_exprs(expr_path);
    std::fprintf(stderr, "loaded %zu expressions from %s\n", exprs.size(), expr_path.c_str());

    std::vector<attribute_def> attrs = {
        int_attrs ? attribute_def{"exchange_id", attribute_type::integer}
                  : attribute_def{"exchange", attribute_type::string},
        int_attrs ? attribute_def{"symbol_id", attribute_type::integer}
                  : attribute_def{"symbol", attribute_type::string},
        {"trade_volume", attribute_type::integer},
        {"trade_price", attribute_type::float_val},
        {"narrow_metric", attribute_type::float_val},
    };

    auto build_start = std::chrono::steady_clock::now();
    auto tree = build_matching_engine(type, attrs);
    uint64_t id = 1;
    std::size_t rejected = 0;
    for (const auto& e : exprs) {
        try {
            tree->insert(id, e);
        } catch (const std::exception&) {
            ++rejected;
        }
        ++id;
    }
    auto build_end = std::chrono::steady_clock::now();
    std::fprintf(stderr, "%s: inserted %zu, rejected %zu, build took %lldms\n", engine_name.c_str(),
                 exprs.size() - rejected, rejected,
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(
                     build_end - build_start).count()));

    // Real (exchange, symbol, trade_volume, trade_price) rows sampled directly from the actual
    // published table (one per real distinct exchange letter) - same exchange/symbol set used
    // by the earlier differential check, trade_volume/trade_price values from diag_engine_diff's
    // own real sample. narrow_metric (added 2026-08-30, alongside gen_blindspot_subs.py) is
    // SYNTHETIC - computed via the exact same per-symbol hash formula as diag_engine_diff.cpp's
    // own narrow_metric and run_cycle_blindspot.sh's publish SQL, so this file's own literal
    // values stay real/deterministic (not made up) despite the underlying attribute itself
    // being synthetic - see those files' own comments for why.
    // exchange_id/symbol_id (added 2026-08-30, string-vs-integer attribute matching-cost
    // experiment) are this same symbol set's real surrogate keys from exchange_lookup/
    // symbol_lookup - see sql/build_intattrs_lookup.sql - fetched directly via psql, not
    // invented, so --int-attrs exercises the identical real entities as the string path, just
    // referenced by integer id instead of by name.
    struct sample_row {
        std::string exchange;
        std::string symbol;
        int64_t exchange_id;
        int64_t symbol_id;
        int64_t trade_volume;
        double trade_price;
        double narrow_metric;
    };
    std::vector<sample_row> rows = {
        {"A", "AKAN", 1, 366, 100, 118.65, 0.00176}, {"B", "ZTS", 2, 11937, 70, 5.77, -0.00609},
        {"C", "IWM", 3, 5708, 100, 24.71, -0.0009}, {"D", "USB", 4, 10957, 100, 22.1175, -0.00114},
        {"G", "MDLZ", 5, 6700, 100, 122.49, -0.00196}, {"H", "NVDA", 6, 7597, 100, 55.87, 0.00495},
        {"J", "IAU", 7, 5186, 17, 37.64, -0.00384}, {"K", "SOXL", 8, 9865, 1, 46.68, -0.00205},
        {"L", "ANIP", 9, 535, 1, 150.26, -0.00675}, {"M", "ZTS", 10, 11937, 43, 13.25, -0.00609},
        {"N", "ET", 11, 3485, 3, 63.73, -0.00869}, {"P", "UNH", 12, 10902, 10, 1.23, -0.00257},
        {"Q", "MRVL", 13, 7007, 33, 101.93, 0.0006}, {"T", "NYT", 14, 7677, 100, 118.48, -0.00366},
        {"U", "BBAI", 15, 1010, 400, 4.1, -0.00677}, {"V", "TREX", 16, 10611, 500, 211.15, -0.00214},
        {"X", "IREN", 17, 5598, 12, 90.13, 0.00053}, {"Y", "ZURA", 18, 11941, 13, 349.16, 0.00895},
        {"Z", "FLNC", 19, 3970, 4, 326.39, -0.00965},
    };

    // reuses_events()==true for all three engines - one event_sink object reused across every
    // search() call (matches the real event_bridge.hpp reuse pattern this project established
    // for columnar batches), but EACH row's own with_string() calls must still happen before
    // EVERY search(), not just once: search() (betree/atree both, via their own reset()/
    // recycle_event()) clears a reused event's touched slots back to undefined right after each
    // search - exactly like a real per-row populate_event() call in the real pipeline, which
    // populates fresh every row even when reusing the underlying event_sink object.
    auto event = tree->make_event();

    std::fprintf(stderr, "searching for %ds...\n", seconds);
    uint64_t iterations = 0;
    uint64_t total_matches = 0;
    auto loop_start = std::chrono::steady_clock::now();
    auto deadline = loop_start + std::chrono::seconds(seconds);
    std::size_t row_idx = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        auto& row = rows[row_idx];
        if (int_attrs) {
            event->with_integer("exchange_id", row.exchange_id);
            event->with_integer("symbol_id", row.symbol_id);
        } else {
            event->with_string("exchange", row.exchange);
            event->with_string("symbol", row.symbol);
        }
        event->with_integer("trade_volume", row.trade_volume);
        event->with_float("trade_price", row.trade_price);
        event->with_float("narrow_metric", row.narrow_metric);
        const uint64_t c0 = read_cycles();
        auto matched = tree->search(*event);
        record_match_cycles(read_cycles() - c0);
        total_matches += matched.size();
        row_idx = (row_idx + 1) % rows.size();
        ++iterations;
    }
    auto search_end = std::chrono::steady_clock::now();

    std::fprintf(stderr, "%s: %llu search() calls in %ds (%.0f/s), %llu total matches (%.1f avg/call)\n",
                 engine_name.c_str(), static_cast<unsigned long long>(iterations), seconds,
                 static_cast<double>(iterations) / seconds,
                 static_cast<unsigned long long>(total_matches),
                 static_cast<double>(total_matches) / iterations);

    // Ground-truth cross-check for match_timing.hpp's RDTSC-based avg_match_us: this loop's own
    // wall-clock time / iteration count is an independent, non-sampled measurement of average
    // search() cost (no perf/dwarf sampling skid to second-guess) - see match_timing.hpp's own
    // comment for why this replaced the old clock_gettime + 1-in-8-sampling design.
    auto [match_cycles, match_count] = drain_match_timing();
    double wall_avg_us =
        std::chrono::duration<double, std::micro>(search_end - loop_start).count()
        / static_cast<double>(iterations);
    double rdtsc_avg_us = match_count > 0
        ? (double(match_cycles) / cycles_per_microsecond()) / double(match_count)
        : 0.0;
    std::fprintf(stderr, "ground truth: wall_clock_avg_us=%.4f  rdtsc_avg_us=%.4f  ratio=%.3f\n",
                 wall_avg_us, rdtsc_avg_us, rdtsc_avg_us / wall_avg_us);
    return 0;
}
