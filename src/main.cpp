#include "cli.hpp"
#include "config.hpp"
#include "schema_generator.hpp"
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

namespace {

// Schema generation mode — no config or NATS required.
int run_generate_schema(const cxxopts::ParseResult& result) {
    auto path = result["generate-schema"].as<std::string>();
    std::string fmt_str = result.count("format")
        ? result["format"].as<std::string>() : "msgpack";
    auto fmt = sidecar::parse_format(fmt_str);
    if (!fmt) {
        std::cerr << "error: invalid format '" << fmt_str << "'\n";
        return 1;
    }
    try {
        sidecar::generate_schema(path, *fmt);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}

void set_log_level(const std::string& level) {
    if (level == "debug")      spdlog::set_level(spdlog::level::debug);
    else if (level == "warn")  spdlog::set_level(spdlog::level::warn);
    else if (level == "error") spdlog::set_level(spdlog::level::err);
    else                       spdlog::set_level(spdlog::level::info);
}

void log_startup_banner(spdlog::logger& console, const sidecar::config& cfg) {
    console.info("nats_sidecar starting");
    console.info("  server: {}:{}", cfg.nats_address, cfg.nats_port);
    console.info("  input:  {} (format={})", cfg.input_subject, static_cast<int>(cfg.format));
    console.info("  engine: {}", cfg.engine == sidecar::engine_type::atree ? "atree" : "betree");
    console.info("  output: {}.<ID>", cfg.output_prefix);
    console.info("  attributes: {}", cfg.attributes.size());
    console.info("  worker threads: {}", sidecar::effective_worker_count(cfg));
    console.info("  lease bucket: {} (TTL={}s)", cfg.lease_bucket, cfg.lease_ttl_seconds);
    console.info("  input queue: {} messages / {} bytes",
                 cfg.input_queue_max_messages, cfg.input_queue_max_bytes);
    console.info("  publication tasks: {} max in-flight",
                 cfg.publish_max_inflight);
}

// Builds the NATS connection, starts the sidecar engine once connected, runs
// the event loop, then drains publications and the connection on shutdown.
int run_engine(const sidecar::config& cfg, std::shared_ptr<spdlog::logger> console) {
    // Single-threaded io_context (NATS I/O + publish coroutines)
    asio::io_context ioc(1);

    // Graceful shutdown
    asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) {
        console->info("Shutting down...");
        ioc.stop();
    });

    // Build the sidecar engine
    std::shared_ptr<sidecar::sidecar_engine> engine;
    try {
        engine = std::make_shared<sidecar::sidecar_engine>(ioc, cfg, console);
    } catch (const sidecar::matching_engine_error& e) {
        console->error("Failed to initialize sidecar engine: {}", e.what());
        return 1;
    } catch (const std::exception& e) {
        console->error("Failed to initialize sidecar engine: {}", e.what());
        return 1;
    }

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

    // 2. Resume queued publish coroutines and wait for every accepted task.
    // 3. Drain NATS writes and close the connection deterministically.
    ioc.restart();
    asio::co_spawn(ioc,
        [engine, c = conn, console, &ioc]() -> asio::awaitable<void> {
            if (!co_await engine->wait_for_publications(std::chrono::seconds(30))) {
                console->warn("Timed out waiting for publication tasks to finish");
            }
            auto status = co_await c->drain(std::chrono::seconds(30));
            if (status.failed()) {
                console->warn("NATS connection drain failed: {}", status.error());
            }
            ioc.stop();
        },
        asio::detached
    );
    ioc.run();

    console->info("nats_sidecar stopped");
    return 0;
}

} // namespace

int main(int argc, char* argv[]) {
    auto options = sidecar::build_cli_options();
    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << std::endl;
        return 0;
    }

    if (result.count("generate-schema")) {
        return run_generate_schema(result);
    }

    auto console = spdlog::stdout_color_mt("sidecar");

    // Load config from YAML if provided, otherwise start from defaults
    sidecar::config cfg;
    if (result.count("config")) {
        try {
            cfg = sidecar::load_config(result["config"].as<std::string>());
        } catch (const std::exception& e) {
            console->error("Failed to load config: {}", e.what());
            return 1;
        }
    }

    // CLI overrides (applied on top of YAML or defaults)
    if (auto err = sidecar::apply_cli_overrides(cfg, result)) {
        console->error("{}", *err);
        return 1;
    }
    if (auto err = sidecar::finalize_and_validate_config(cfg)) {
        console->error("{}", *err);
        return 1;
    }

    set_log_level(cfg.log_level);
    log_startup_banner(*console, cfg);

    return run_engine(cfg, console);
}
