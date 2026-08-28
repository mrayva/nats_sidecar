// Standalone matching-engine throughput microbenchmark: a-tree vs be-tree vs pstree, in-process,
// no NATS/Postgres/OS I/O involved at all - specifically to isolate pure matching-engine cost
// (index construction time, matching throughput) from the NATS-transport/row-unpacking costs that
// dominated this project's earlier end-to-end fleet benchmarks (see README.md's "be-tree
// comparison" section: "Processing capacity is statistically indistinguishable between engines...
// A matching-engine choice would be expected to matter more under conditions where search itself
// is the bottleneck (e.g. many more concurrent expressions per instance) - not measured here").
// This is exactly that "not measured here" case, and the direct, independent check of the PS-Tree
// paper's own self-reported claim (PSTDynamic beats both BE-Tree and A-Tree on matching time and
// index construction time) that the project plan called for.
//
// Methodology:
//   - One fixed, non-list attribute schema shared by all three engines (pstree has no
//     list-attribute support at all - see matching_engine.cpp/pstree_dialect.hpp - so list
//     attributes are left out of this benchmark's schema entirely, for a fair 3-way comparison
//     rather than a 2-way-plus-asterisk one).
//   - Subscription and event generators are seeded deterministically (std::mt19937, fixed seeds)
//     and generate the *same* subscription set / event stream for every engine at a given K - the
//     comparison is "same input, different engine", not three independent random samples.
//   - Every generated expression avoids the two documented pstree-specific rejections (bare
//     "X is null" as a subscription's only predicate, and any list-attribute reference) - not
//     because those are hidden, but because a throughput comparison needs every engine to accept
//     every subscription identically; the acceptance-boundary behavior itself is already covered
//     by test_matching_engine.cpp's pstree-specific tests, not re-tested here.
//   - Run-order mitigation (see the project's own feedback_benchmark_run_order_confound lesson):
//     two full passes per K, alternating engine order (atree,betree,pstree then
//     pstree,betree,atree), averaged - this is an in-process CPU benchmark with no external I/O,
//     so the drift mechanism that motivated that lesson (page-cache/OS state across separate
//     process invocations) mostly doesn't apply here, but alternating order is cheap insurance
//     against warmup/cache-locality bias between consecutive engine runs in the same process.
//
// Usage: matching_engine_bench [K...]  (defaults to 1000 10000 50000 if none given)

#include "matching_engine.hpp"
#include "config.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using sidecar::attribute_def;
using sidecar::attribute_type;
using sidecar::engine_type;

namespace {

std::vector<attribute_def> bench_schema() {
    return {
        {"trade_price",  attribute_type::float_val},
        {"trade_volume", attribute_type::integer},
        {"symbol",       attribute_type::string},
        {"active",       attribute_type::boolean},
    };
}

std::vector<std::string> symbol_pool() {
    static const char* kBase[] = {
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO", "AMD",  "NFLX",
        "COIN", "PLTR", "SNOW", "SHOP", "SQ",   "UBER", "ABNB", "CRWD", "DDOG", "NET",
    };
    return std::vector<std::string>(std::begin(kBase), std::end(kBase));
}

// One subscription per generated expression, avoiding pstree's two documented rejections
// (list-attribute references, bare "is null" as the only predicate) - see file header.
std::vector<std::string> generate_subscriptions(std::size_t count, unsigned seed) {
    std::mt19937 rng(seed);
    auto symbols = symbol_pool();
    std::uniform_int_distribution<int> kind(0, 9);
    std::uniform_real_distribution<double> price(0.0, 1000.0);
    std::uniform_int_distribution<int64_t> volume(0, 100000);
    std::uniform_int_distribution<std::size_t> symbol_idx(0, symbols.size() - 1);

    std::vector<std::string> exprs;
    exprs.reserve(count);
    char buf[256];
    for (std::size_t i = 0; i < count; ++i) {
        int k = kind(rng);
        if (k < 4) {
            // Equality on symbol - highest-selectivity shape (a-tree/be-tree/pstree all
            // agree this is the cheapest to index).
            std::snprintf(buf, sizeof(buf), "symbol = \"%s\"", symbols[symbol_idx(rng)].c_str());
        } else if (k < 7) {
            // Single-predicate range comparison.
            std::snprintf(buf, sizeof(buf), "trade_price > %.2f", price(rng));
        } else if (k < 9) {
            // Two-predicate conjunction.
            std::snprintf(buf, sizeof(buf), "trade_price > %.2f and trade_volume > %lld",
                          price(rng), static_cast<long long>(volume(rng)));
        } else {
            // Disjunction - exercises pstree's DNF-clause-expansion path specifically.
            std::snprintf(buf, sizeof(buf), "trade_price > %.2f or symbol = \"%s\"",
                          price(rng), symbols[symbol_idx(rng)].c_str());
        }
        exprs.emplace_back(buf);
    }
    return exprs;
}

struct bench_event {
    double price;
    int64_t volume;
    std::string symbol;
    bool active;
};

std::vector<bench_event> generate_events(std::size_t count, unsigned seed) {
    std::mt19937 rng(seed);
    auto symbols = symbol_pool();
    std::uniform_real_distribution<double> price(0.0, 1000.0);
    std::uniform_int_distribution<int64_t> volume(0, 100000);
    std::uniform_int_distribution<std::size_t> symbol_idx(0, symbols.size() - 1);
    std::bernoulli_distribution active_dist(0.5);

    std::vector<bench_event> events;
    events.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        events.push_back({price(rng), volume(rng), symbols[symbol_idx(rng)], active_dist(rng)});
    }
    return events;
}

void populate(sidecar::event_sink& sink, const bench_event& ev) {
    sink.with_float("trade_price", ev.price);
    sink.with_integer("trade_volume", ev.volume);
    sink.with_string("symbol", ev.symbol);
    sink.with_boolean("active", ev.active);
}

const char* engine_name(engine_type t) {
    switch (t) {
        case engine_type::atree:  return "atree";
        case engine_type::betree: return "betree";
        case engine_type::pstree: return "pstree";
    }
    return "?";
}

struct bench_result {
    double insert_ms = 0.0;
    double search_ms = 0.0;
    std::size_t rejected = 0;
    std::uint64_t total_matches = 0;
};

bench_result run_once(engine_type type, const std::vector<std::string>& exprs,
                       const std::vector<bench_event>& events)
{
    using clock = std::chrono::steady_clock;
    bench_result r;

    auto engine = sidecar::build_matching_engine(type, bench_schema());

    auto t0 = clock::now();
    for (std::size_t i = 0; i < exprs.size(); ++i) {
        try {
            engine->insert(static_cast<uint64_t>(i + 1), exprs[i]);
        } catch (const std::exception&) {
            ++r.rejected;
        }
    }
    auto t1 = clock::now();
    r.insert_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (engine->reuses_events()) {
        auto sink = engine->make_event();
        auto t2 = clock::now();
        for (const auto& ev : events) {
            populate(*sink, ev);
            r.total_matches += engine->search(*sink).size();
        }
        auto t3 = clock::now();
        r.search_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();
    } else {
        auto t2 = clock::now();
        for (const auto& ev : events) {
            auto sink = engine->make_event();
            populate(*sink, ev);
            r.total_matches += engine->search(*sink).size();
        }
        auto t3 = clock::now();
        r.search_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();
    }
    return r;
}

} // namespace

int main(int argc, char** argv) {
    std::vector<std::size_t> ks;
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) ks.push_back(static_cast<std::size_t>(std::stoul(argv[i])));
    } else {
        ks = {1000, 10000, 50000};
    }
    constexpr std::size_t kEventCount = 20000;
    constexpr unsigned kSubSeed = 42;
    constexpr unsigned kEventSeed = 1337;

    const std::vector<engine_type> forward  = {engine_type::atree, engine_type::betree, engine_type::pstree};
    const std::vector<engine_type> backward = {engine_type::pstree, engine_type::betree, engine_type::atree};

    for (std::size_t k : ks) {
        auto exprs = generate_subscriptions(k, kSubSeed);
        auto events = generate_events(kEventCount, kEventSeed);

        std::printf("\n=== K = %zu subscriptions, M = %zu events ===\n", k, kEventCount);

        // Two full rounds, engines visited in opposite order each round (see file header) -
        // round 1 runs atree/betree/pstree in that position order, round 2 runs the same three
        // in the reverse position order, so no engine sits in the same "how much prior work has
        // this process already done" slot twice before being averaged.
        std::vector<bench_result> round1, round2;
        for (auto type : forward)  round1.push_back(run_once(type, exprs, events));
        for (auto type : backward) round2.push_back(run_once(type, exprs, events));

        for (auto type : {engine_type::atree, engine_type::betree, engine_type::pstree}) {
            const bench_result& a = round1[static_cast<std::size_t>(
                std::find(forward.begin(), forward.end(), type) - forward.begin())];
            const bench_result& b = round2[static_cast<std::size_t>(
                std::find(backward.begin(), backward.end(), type) - backward.begin())];

            double insert_ms = (a.insert_ms + b.insert_ms) / 2.0;
            double search_ms = (a.search_ms + b.search_ms) / 2.0;
            double insert_rate = insert_ms > 0 ? (double)k / (insert_ms / 1000.0) : 0.0;
            double search_rate = search_ms > 0 ? (double)kEventCount / (search_ms / 1000.0) : 0.0;

            std::printf(
                "%-8s insert: %8.2f ms (%10.0f subs/s)  search: %8.2f ms (%10.0f events/s)  "
                "matches(round1/round2): %llu/%llu  rejected: %zu\n",
                engine_name(type), insert_ms, insert_rate, search_ms, search_rate,
                (unsigned long long)a.total_matches, (unsigned long long)b.total_matches,
                a.rejected);
        }
    }
    return 0;
}
