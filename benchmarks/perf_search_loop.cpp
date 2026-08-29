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
// Usage: perf_search_loop <atree|betree|pstree> <expr_file> [seconds=10]
//   perf record -F 999 --call-graph dwarf -o out.data -- \
//       ./bin/perf_search_loop betree path/to/exprs.txt 15
//   perf report -i out.data --stdio -g none | less
#include "matching_engine.hpp"
#include "config.hpp"

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
        std::fprintf(stderr, "usage: perf_search_loop <atree|betree|pstree> <expr_file> [seconds=10]\n");
        return 2;
    }
    std::string engine_name = argv[1];
    std::string expr_path = argv[2];
    int seconds = argc > 3 ? std::atoi(argv[3]) : 10;

    engine_type type;
    if (engine_name == "atree") type = engine_type::atree;
    else if (engine_name == "betree") type = engine_type::betree;
    else if (engine_name == "pstree") type = engine_type::pstree;
    else { std::fprintf(stderr, "unknown engine %s\n", engine_name.c_str()); return 2; }

    auto exprs = read_exprs(expr_path);
    std::fprintf(stderr, "loaded %zu expressions from %s\n", exprs.size(), expr_path.c_str());

    std::vector<attribute_def> attrs = {
        {"exchange", attribute_type::string},
        {"symbol", attribute_type::string},
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

    // Real (exchange, symbol) pairs sampled directly from the actual published table (one per
    // real distinct exchange letter) - same set used by the earlier differential check.
    std::vector<std::pair<std::string, std::string>> rows = {
        {"A", "AKAN"}, {"B", "ZTS"}, {"C", "IWM"}, {"D", "USB"}, {"G", "MDLZ"},
        {"H", "NVDA"}, {"J", "IAU"}, {"K", "SOXL"}, {"L", "ANIP"}, {"M", "ZTS"},
        {"N", "ET"}, {"P", "UNH"}, {"Q", "MRVL"}, {"T", "NYT"}, {"U", "BBAI"},
        {"V", "TREX"}, {"X", "IREN"}, {"Y", "ZURA"}, {"Z", "FLNC"},
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
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    std::size_t row_idx = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        auto& [exch, sym] = rows[row_idx];
        event->with_string("exchange", exch);
        event->with_string("symbol", sym);
        auto matched = tree->search(*event);
        total_matches += matched.size();
        row_idx = (row_idx + 1) % rows.size();
        ++iterations;
    }

    std::fprintf(stderr, "%s: %llu search() calls in %ds (%.0f/s), %llu total matches (%.1f avg/call)\n",
                 engine_name.c_str(), static_cast<unsigned long long>(iterations), seconds,
                 static_cast<double>(iterations) / seconds,
                 static_cast<unsigned long long>(total_matches),
                 static_cast<double>(total_matches) / iterations);
    return 0;
}
