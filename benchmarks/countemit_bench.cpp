// Isolated microbenchmark: does splitting matchEvent's candidate walk into a cheap "count only"
// pass (used for the backpressure/estimated_bytes decision) plus a second "emit" pass that only
// runs for rows that survive backpressure, beat today's single "always evaluate, always write"
// pass - given the real K=8000/s=1 workload's own measured ~94% backpressure-drop rate?
//
// Models GroupEntry as-is (id + subPtr + accIdx + accIdxSkippable + onlyPredicateIsAccess, see
// pstree/include/pstree/pst_dynamic.hpp:873-879) and the real benchmark's own subscription shape
// (single-predicate price-threshold subs, gen_price_threshold_subs_realistic.py) - which reaches
// GroupEntry's "zero-dereference fast path" (accIdxSkippable && onlyPredicateIsAccess), i.e. a
// cheap, branch-predictable compare with no subPtr chase. The expensive step being isolated here
// is specifically the push_back's own cold-memory write into `matchingSubs` - already reserve()'d
// (pstree@3093b3f), so this measures the write itself, not a reallocation cost.
//
// Variant A (today): for every row, walk all N candidates, evaluate + push_back matches into a
// reserve()'d vector - unconditionally, whether or not the row will later survive backpressure.
// For the ~6% of rows marked "kept" (matching s=1's real measured survival rate), also do a read
// pass over the written vector (simulating worker_pool.cpp's dedup loop, which only ever runs for
// surviving rows).
//
// Variant B (proposed): for every row, walk all N candidates but only COUNT matches (no vector
// write) - this is what would feed estimated_bytes/the backpressure decision. Only for rows
// marked "kept" (6%), walk the SAME N candidates a SECOND time, this time evaluating + writing
// into a reserve()'d vector (identical to Variant A's per-row work), then the same read pass.
//
// Same established methodology as this session's other microbenchmarks: 64MB cache-eviction
// distractor between runs (host L3 is 32MB, confirmed via lscpu), interleaved A/B repeats with a
// discarded warm-up, checksums to prevent dead-code elimination.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <vector>

struct GroupEntry {
    std::uint64_t id;
    const void* subPtr;
    std::size_t accIdx;
    bool accIdxSkippable;
    bool onlyPredicateIsAccess;
};

constexpr std::size_t kCandidatesPerRow = 8000;   // K
constexpr double kMatchRate = 0.95;               // s=1: near-total selectivity
constexpr double kKeptFraction = 0.06;             // real measured s=1 survival rate
constexpr std::size_t kRowsPerTrial = 500;         // one benchmark batch's worth of rows
constexpr std::size_t kDistractorBytes = 64ull * 1024 * 1024;

// Fast-path predicate check: exactly the branch GroupEntry's own comment describes as reaching
// "zero dereference into subPtr/subscriptions_ at all" - a compare against a threshold baked into
// the candidate itself, no pointer chase. threshold_ok encodes match/no-match for this run.
inline bool evaluate(const GroupEntry& e) {
    return e.accIdxSkippable && e.onlyPredicateIsAccess;
}

std::vector<GroupEntry> make_candidates(std::mt19937_64& rng) {
    std::vector<GroupEntry> v(kCandidatesPerRow);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    for (std::size_t i = 0; i < v.size(); ++i) {
        bool matches = u(rng) < kMatchRate;
        v[i] = GroupEntry{static_cast<std::uint64_t>(i), nullptr, 0, matches, matches};
    }
    return v;
}

// Defeats cache residency between runs, same as this session's other microbenchmarks.
void evict_cache(std::vector<char>& distractor, std::mt19937_64& rng) {
    std::uniform_int_distribution<std::size_t> idx(0, distractor.size() - 1);
    for (std::size_t i = 0; i < distractor.size(); i += 64) {
        distractor[i] = static_cast<char>(distractor[i] + 1);
    }
    distractor[idx(rng)] = static_cast<char>(rng());
}

// Variant A: always evaluate + always write; kept rows additionally get a read pass.
std::uint64_t run_variant_a(const std::vector<std::vector<GroupEntry>>& rows,
                             const std::vector<bool>& kept) {
    std::uint64_t checksum = 0;
    std::vector<std::uint64_t> matchingSubs;
    for (std::size_t r = 0; r < rows.size(); ++r) {
        matchingSubs.clear();
        matchingSubs.reserve(rows[r].size());
        for (const auto& e : rows[r]) {
            if (evaluate(e)) matchingSubs.push_back(e.id);
        }
        if (kept[r]) {
            for (auto id : matchingSubs) checksum += id;
        } else {
            checksum += matchingSubs.size(); // still "use" the count, matches real estimated_bytes read
        }
    }
    return checksum;
}

// Variant B: count-only pass for every row; emit (evaluate+write) pass only for kept rows.
std::uint64_t run_variant_b(const std::vector<std::vector<GroupEntry>>& rows,
                             const std::vector<bool>& kept) {
    std::uint64_t checksum = 0;
    std::vector<std::uint64_t> matchingSubs;
    for (std::size_t r = 0; r < rows.size(); ++r) {
        std::size_t count = 0;
        for (const auto& e : rows[r]) {
            if (evaluate(e)) ++count;
        }
        checksum += count; // stands in for the real estimated_bytes computation, both variants pay this

        if (kept[r]) {
            matchingSubs.clear();
            matchingSubs.reserve(count);
            for (const auto& e : rows[r]) {
                if (evaluate(e)) matchingSubs.push_back(e.id);
            }
            for (auto id : matchingSubs) checksum += id;
        }
    }
    return checksum;
}

int main() {
    std::mt19937_64 rng(42);
    std::vector<std::vector<GroupEntry>> rows;
    rows.reserve(kRowsPerTrial);
    for (std::size_t i = 0; i < kRowsPerTrial; ++i) rows.push_back(make_candidates(rng));

    std::vector<bool> kept(kRowsPerTrial);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    for (std::size_t i = 0; i < kRowsPerTrial; ++i) kept[i] = u(rng) < kKeptFraction;

    std::vector<char> distractor(kDistractorBytes, 0);

    constexpr int kRepeats = 6;
    for (int rep = 0; rep < kRepeats; ++rep) {
        bool a_first = (rep % 2 == 0);

        auto time_a = [&]() {
            evict_cache(distractor, rng);
            auto t0 = std::chrono::steady_clock::now();
            auto cs = run_variant_a(rows, kept);
            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
            std::printf("A(always-write): total=%.1fus checksum=%llu\n", us,
                        (unsigned long long)cs);
        };
        auto time_b = [&]() {
            evict_cache(distractor, rng);
            auto t0 = std::chrono::steady_clock::now();
            auto cs = run_variant_b(rows, kept);
            auto t1 = std::chrono::steady_clock::now();
            double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
            std::printf("B(count-then-emit): total=%.1fus checksum=%llu\n", us,
                        (unsigned long long)cs);
        };

        if (a_first) { time_a(); time_b(); } else { time_b(); time_a(); }
    }
    return 0;
}
