#pragma once

// Per-row matching_engine::search() timing, replacing the old avg_match_us
// (removed - see worker_pool.hpp's stats::match_time_cycles_total comment
// for why). The old design sampled 1-in-8 rows via clock_gettime and was
// both inaccurate (~100-140x off from a real perf profile) and expensive on
// its own (~7.6% CPU). This one uses a raw cycle-counter register (RDTSC)
// instead, which is cheap enough (a handful of cycles per call, no vDSO
// call/syscall) to time every row unconditionally rather than sample.
//
// Accumulation is via thread_local counters rather than threading an output
// parameter through match_message()/deserialize_and_match*() (the old
// design's approach): worker_pool::worker_loop() runs each worker on one
// fixed OS thread and calls deserialize_and_match*() synchronously on that
// same thread, so a thread_local written inside match_message() and read
// back by the same thread immediately after the call returns is safe without
// touching any of those functions' signatures.

#include <chrono>
#include <cstdint>
#include <utility>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#define SIDECAR_HAVE_RDTSC 1
#else
#define SIDECAR_HAVE_RDTSC 0
#endif

namespace sidecar {

// Reads the CPU's cycle counter on x86(-64) via RDTSC. On any other
// architecture, falls back to a std::chrono::steady_clock tick count - not a
// real cycle count, but still a cheap, monotonically increasing counter that
// cycles_per_microsecond() calibrates against consistently.
inline std::uint64_t read_cycles() {
#if SIDECAR_HAVE_RDTSC
    return __rdtsc();
#else
    return static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// One-time calibration: compares a read_cycles() delta against a
// steady_clock delta over a short busy-wait, computed once for the whole
// process on first call. Assumes TSC-invariance across cores (constant_tsc/
// nonstop_tsc, standard on any modern x86_64 Linux host) - reading on
// whichever thread happens to call this first is fine under that assumption.
inline double cycles_per_microsecond() {
    static const double value = [] {
        const auto wall_start = std::chrono::steady_clock::now();
        const std::uint64_t cycle_start = read_cycles();

        // ~20ms is long enough for a stable ratio without meaningfully
        // delaying whichever caller triggers calibration first.
        const auto busy_until = wall_start + std::chrono::milliseconds(20);
        while (std::chrono::steady_clock::now() < busy_until) {
            // Busy-wait deliberately (not sleep): calibrating against wall
            // time actually elapsed on this core, not scheduler latency.
        }

        const auto wall_end = std::chrono::steady_clock::now();
        const std::uint64_t cycle_end = read_cycles();

        const double wall_us = std::chrono::duration<double, std::micro>(
            wall_end - wall_start).count();
        const double cycles = static_cast<double>(cycle_end - cycle_start);
        return wall_us > 0.0 ? cycles / wall_us : 1.0;
    }();
    return value;
}

namespace detail {
inline thread_local std::uint64_t t_match_cycles_total = 0;
inline thread_local std::uint64_t t_match_count = 0;
} // namespace detail

// Called around each matching_engine::search() call in event_bridge.hpp.
inline void record_match_cycles(std::uint64_t cycles) {
    detail::t_match_cycles_total += cycles;
    ++detail::t_match_count;
}

// Reads and resets this thread's accumulators. Called once per
// deserialize_and_match/deserialize_and_match_columnar() call from
// worker_pool.cpp, on the same worker thread that accumulated them.
inline std::pair<std::uint64_t, std::uint64_t> drain_match_timing() {
    std::pair<std::uint64_t, std::uint64_t> result(
        detail::t_match_cycles_total, detail::t_match_count);
    detail::t_match_cycles_total = 0;
    detail::t_match_count = 0;
    return result;
}

} // namespace sidecar
