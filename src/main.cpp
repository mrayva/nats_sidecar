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
#include <thread>

int main(int argc, char* argv[]) {
    cxxopts::Options options("nats_sidecar",
        "Content-based filtering sidecar for NATS");

    options.add_options()
        ("c,config", "Path to YAML config file", cxxopts::value<std::string>())
        ("a,address", "NATS server address", cxxopts::value<std::string>())
        ("p,port", "NATS server port", cxxopts::value<uint16_t>())
        ("i,input-subject", "Input NATS subject", cxxopts::value<std::string>())
        ("f,format", "Binary format (msgpack|cbor|flexbuffers|zera)", cxxopts::value<std::string>())
        ("output-prefix", "Output subject prefix", cxxopts::value<std::string>())
        ("queue-group", "Input queue group for load balancing", cxxopts::value<std::string>())
        ("subscribe-subject", "Subscription request subject", cxxopts::value<std::string>())
        ("unsubscribe-subject", "Unsubscription request subject", cxxopts::value<std::string>())
        ("lease-bucket", "NATS KV lease bucket name", cxxopts::value<std::string>())
        ("lease-ttl", "Lease TTL in seconds", cxxopts::value<uint32_t>())
        ("lease-check-interval", "Lease check interval in seconds", cxxopts::value<uint32_t>())
        ("attr", "Attribute as name:type (repeatable)", cxxopts::value<std::vector<std::string>>())
        ("workers", "Worker thread count (0 = auto)", cxxopts::value<unsigned int>())
        ("tls-cert", "TLS certificate path", cxxopts::value<std::string>())
        ("tls-key", "TLS key path", cxxopts::value<std::string>())
        ("tls-ca", "TLS CA certificate path", cxxopts::value<std::string>())
        ("stats-interval", "Stats log interval in seconds", cxxopts::value<int>())
        ("log-level", "Log level (debug|info|warn|error)", cxxopts::value<std::string>())
        ("generate-schema", "Infer attributes from a sample binary file", cxxopts::value<std::string>())
        ("v,verbose", "Enable debug logging")
        ("h,help", "Print help");

    auto result = options.parse(argc, argv);

    if (result.count("help")) {
        std::cout << options.help() << std::endl;
        return 0;
    }

    // Schema generation mode — no config or NATS required
    if (result.count("generate-schema")) {
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

    // Logger
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
    if (result.count("address"))              cfg.nats_address = result["address"].as<std::string>();
    if (result.count("port"))                 cfg.nats_port = result["port"].as<uint16_t>();
    if (result.count("input-subject"))        cfg.input_subject = result["input-subject"].as<std::string>();
    if (result.count("output-prefix"))        cfg.output_prefix = result["output-prefix"].as<std::string>();
    if (result.count("queue-group"))          cfg.input_queue_group = result["queue-group"].as<std::string>();
    if (result.count("subscribe-subject"))    cfg.subscribe_subject = result["subscribe-subject"].as<std::string>();
    if (result.count("unsubscribe-subject"))  cfg.unsubscribe_subject = result["unsubscribe-subject"].as<std::string>();
    if (result.count("lease-bucket"))         cfg.lease_bucket = result["lease-bucket"].as<std::string>();
    if (result.count("lease-ttl"))            cfg.lease_ttl_seconds = result["lease-ttl"].as<uint32_t>();
    if (result.count("lease-check-interval")) cfg.lease_check_interval_seconds = result["lease-check-interval"].as<uint32_t>();
    if (result.count("workers"))              cfg.worker_threads = result["workers"].as<unsigned int>();
    if (result.count("tls-cert"))             cfg.tls_cert = result["tls-cert"].as<std::string>();
    if (result.count("tls-key"))              cfg.tls_key = result["tls-key"].as<std::string>();
    if (result.count("tls-ca"))               cfg.tls_ca = result["tls-ca"].as<std::string>();
    if (result.count("stats-interval"))       cfg.stats_interval_seconds = result["stats-interval"].as<int>();
    if (result.count("log-level"))            cfg.log_level = result["log-level"].as<std::string>();
    if (result.count("verbose"))              cfg.log_level = "debug";

    if (result.count("format")) {
        auto fmt = sidecar::parse_format(result["format"].as<std::string>());
        if (!fmt) {
            console->error("Invalid format: {}", result["format"].as<std::string>());
            return 1;
        }
        cfg.format = *fmt;
    }

    // Parse --attr name:type pairs (appended to any YAML-defined attributes)
    if (result.count("attr")) {
        for (const auto& raw : result["attr"].as<std::vector<std::string>>()) {
            auto colon = raw.find(':');
            if (colon == std::string::npos) {
                console->error("Invalid --attr '{}': expected name:type", raw);
                return 1;
            }
            auto name = raw.substr(0, colon);
            auto type_str = raw.substr(colon + 1);
            auto type = sidecar::parse_attribute_type(type_str);
            if (!type) {
                console->error("Invalid attribute type '{}' in --attr '{}'", type_str, raw);
                return 1;
            }
            cfg.attributes.push_back({std::move(name), *type});
        }
    }

    // Default output_prefix to input_subject if still empty
    if (cfg.output_prefix.empty()) cfg.output_prefix = cfg.input_subject;

    // Validate required fields
    if (cfg.input_subject.empty()) {
        console->error("input_subject is required (via config file or --input-subject)");
        return 1;
    }
    if (cfg.attributes.empty()) {
        console->error("At least one attribute is required (via config file or --attr)");
        return 1;
    }

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
