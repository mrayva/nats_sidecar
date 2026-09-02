#include "match_timing.hpp"
#include <gtest/gtest.h>
#include <thread>

// Sanity checks on the low-level RDTSC/calibration primitives themselves -
// not the full worker_pool pipeline (that's covered end-to-end by the
// pipeline benchmark's own perf-cross-check, done manually, not as an
// automated test).

TEST(match_timing, read_cycles_is_monotonically_increasing) {
    const std::uint64_t a = sidecar::read_cycles();
    // A tiny bit of real work between reads - back-to-back calls with
    // nothing between them can tie on some platforms' clock resolution.
    volatile int spin = 0;
    for (int i = 0; i < 1000; ++i) spin += i;
    const std::uint64_t b = sidecar::read_cycles();
    EXPECT_GT(b, a);
}

TEST(match_timing, cycles_per_microsecond_is_a_plausible_positive_ratio) {
    // Modern x86_64 CPUs run at roughly 1-5 GHz, i.e. 1000-5000 cycles per
    // microsecond. The non-x86 fallback uses steady_clock ticks directly, so
    // this ratio is close to 1 there instead - just check it's positive and
    // finite, not a specific range, to stay portable.
    const double ratio = sidecar::cycles_per_microsecond();
    EXPECT_GT(ratio, 0.0);
}

TEST(match_timing, cycles_per_microsecond_reproduces_a_known_sleep_duration) {
    const std::uint64_t start = sidecar::read_cycles();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const std::uint64_t end = sidecar::read_cycles();

    const double measured_us =
        static_cast<double>(end - start) / sidecar::cycles_per_microsecond();

    // Generous tolerance: this is a sanity check on the primitive's rough
    // accuracy, not a precise timing assertion - scheduler jitter on a
    // loaded CI box can easily add tens of milliseconds to a sleep_for.
    EXPECT_GT(measured_us, 25000.0);
    EXPECT_LT(measured_us, 500000.0);
}

TEST(match_timing, drain_match_timing_reads_and_resets) {
    // Drain any accumulation left over from other tests running on this
    // thread first, so this test's own assertions aren't polluted by them.
    sidecar::drain_match_timing();

    sidecar::record_match_cycles(100);
    sidecar::record_match_cycles(200);

    auto [cycles, count] = sidecar::drain_match_timing();
    EXPECT_EQ(cycles, 300u);
    EXPECT_EQ(count, 2u);

    auto [cycles2, count2] = sidecar::drain_match_timing();
    EXPECT_EQ(cycles2, 0u);
    EXPECT_EQ(count2, 0u);
}
