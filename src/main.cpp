#include "config.hpp"
#include "sidecar.hpp"
#include <nats_asio/nats_asio.hpp>
#include <asio/io_context.hpp>
#include <asio/signal_set.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <cxxopts.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <iostream>
#include <memory>
#include <thread>

int main(int argc, char* argv[]) {
    cxxopts::Options options("nats_sidecar",
        "Content-based filtering sidecar for NATS");

    options.add_options()
        ("c,config", "Path to YAML config file", cxxopts::value<std::string>())
        ("a,address", "NATS server address (overrides config)", cxxopts::value<std::string>())
        ("p,port", "NATS server port (overrides config)", cxxopts::value<uint16_t>())
        ("v,verbose", "Enable debug logging")
        ("h,help", "Print help");

    auto result = options.parse(argc, argv);

    if (result.count("help") || !result.count("config")) {
        std::cout << options.help() << std::endl;
        return result.count("help") ? 0 : 1;
    }

    // Logger
    auto console = spdlog::stdout_color_mt("sidecar");

    // Load config
    sidecar::config cfg;
    try {
        cfg = sidecar::load_config(result["config"].as<std::string>());
    } catch (const std::exception& e) {
        console->error("Failed to load config: {}", e.what());
        return 1;
    }

    // CLI overrides
    if (result.count("address")) cfg.nats_address = result["address"].as<std::string>();
    if (result.count("port"))    cfg.nats_port = result["port"].as<uint16_t>();
    if (result.count("verbose")) cfg.log_level = "debug";

    // Set log level
    if (cfg.log_level == "debug")      spdlog::set_level(spdlog::level::debug);
    else if (cfg.log_level == "warn")  spdlog::set_level(spdlog::level::warn);
    else if (cfg.log_level == "error") spdlog::set_level(spdlog::level::err);
    else                               spdlog::set_level(spdlog::level::info);

    // Resolve effective worker thread count for logging
    unsigned int effective_workers = cfg.worker_threads > 0
        ? cfg.worker_threads
        : std::thread::hardware_concurrency();
    if (effective_workers == 0) effective_workers = 1;

    console->info("nats_sidecar starting");
    console->info("  server: {}:{}", cfg.nats_address, cfg.nats_port);
    console->info("  input:  {} (format={})", cfg.input_subject, static_cast<int>(cfg.format));
    console->info("  output: {}.<ID>", cfg.output_prefix);
    console->info("  attributes: {}", cfg.attributes.size());
    console->info("  worker threads: {}", effective_workers);
    console->info("  lease bucket: {} (TTL={}s)", cfg.lease_bucket, cfg.lease_ttl_seconds);

    // Single-threaded io_context (NATS I/O + publish coroutines)
    asio::io_context ioc(1);

    // Graceful shutdown
    asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) {
        console->info("Shutting down...");
        ioc.stop();
    });

    // Build the sidecar engine
    auto engine = std::make_shared<sidecar::sidecar_engine>(ioc, cfg, console);

    // Build NATS connect config
    nats_asio::connect_config nats_cfg;
    nats_cfg.address = cfg.nats_address;
    nats_cfg.port = cfg.nats_port;

    // SSL config
    std::optional<nats_asio::ssl_config> ssl_conf;
    if (!cfg.tls_cert.empty()) {
        nats_asio::ssl_config sc;
        sc.cert = cfg.tls_cert;
        sc.key  = cfg.tls_key;
        sc.ca   = cfg.tls_ca;
        sc.verify = true;
        ssl_conf = sc;
    }

    // Callbacks
    auto on_connected = [engine, console](nats_asio::iconnection& /*c*/) -> asio::awaitable<void> {
        console->info("Connected to NATS");
        co_return;
    };

    auto on_disconnected = [console](nats_asio::iconnection& /*c*/) -> asio::awaitable<void> {
        console->warn("Disconnected from NATS");
        co_return;
    };

    auto on_error = [console](nats_asio::iconnection& /*c*/, std::string_view err) -> asio::awaitable<void> {
        console->error("NATS connection error: {}", err);
        co_return;
    };

    auto conn = nats_asio::create_connection(
        ioc, on_connected, on_disconnected, on_error, ssl_conf);

    conn->start(nats_cfg);

    // Start engine once connected
    asio::co_spawn(ioc,
        [engine, c = conn]() mutable -> asio::awaitable<void> {
            asio::steady_timer timer(co_await asio::this_coro::executor);
            while (!c->is_connected()) {
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(asio::use_awaitable);
            }
            co_await engine->start(c);
        },
        asio::detached
    );

    // Run the event loop (single thread)
    ioc.run();

    // Shutdown ordering:
    // 1. Stop worker threads (drain queue + join)
    engine->stop_workers();

    // 2. Flush any remaining co_spawn'd publish coroutines
    ioc.restart();
    ioc.run();

    console->info("nats_sidecar stopped");
    return 0;
}
