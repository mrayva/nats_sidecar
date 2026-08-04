#pragma once

// Shared helpers for driving asio coroutines/io_context in tests without a
// live server or real background timers. All three poll with short
// run_for() slices rather than a single long run_for()/run() call, because
// a single call can return immediately if nothing is queued yet on a
// fresh/restarted io_context - e.g. a worker thread on another std::thread
// hasn't posted its completion handler at the moment run_for() is called.

#include <asio/co_spawn.hpp>
#include <asio/io_context.hpp>
#include <chrono>
#include <optional>
#include <thread>
#include <utility>

namespace sidecar_test {

// Spawns `awaitable` and polls `ioc` until it completes, returning its
// result. Rethrows any exception the coroutine threw. Asserts (via the
// returned std::optional being empty on timeout -> value() throws) rather
// than silently returning a default if `awaitable` never completes.
template <typename T, typename Awaitable>
T run_to_completion(asio::io_context& ioc, Awaitable&& awaitable,
                    std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    std::optional<T> result;
    asio::co_spawn(ioc, std::forward<Awaitable>(awaitable), [&](std::exception_ptr ep, T value) {
        if (ep) std::rethrow_exception(ep);
        result = std::move(value);
    });
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!result.has_value() && std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(20));
    }
    return std::move(result).value();
}

// Same as run_to_completion, for asio::awaitable<void>. Returns whether it
// completed within `timeout` - callers should assert on this (e.g.
// ASSERT_TRUE(run_void_to_completion(...))) so a hang fails loudly instead
// of silently skipping the rest of the test.
inline bool run_void_to_completion(asio::io_context& ioc, asio::awaitable<void> awaitable,
                                   std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    bool done = false;
    asio::co_spawn(ioc, std::move(awaitable), [&](std::exception_ptr ep) {
        if (ep) std::rethrow_exception(ep);
        done = true;
    });
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!done && std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(20));
    }
    return done;
}

// Polls an arbitrary predicate (e.g. checking a stats counter mutated by a
// worker thread, rather than a coroutine this call spawns itself) while
// driving `ioc`, until it returns true or `timeout` elapses.
template <typename Pred>
bool drive_until(asio::io_context& ioc, Pred&& done, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return done();
}

} // namespace sidecar_test
