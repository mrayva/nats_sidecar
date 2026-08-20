#include "cli.hpp"
#include <fmt/format.h>
#include <thread>

namespace sidecar {

cxxopts::Options build_cli_options() {
    cxxopts::Options options("nats_sidecar",
        "Content-based filtering sidecar for NATS");

    options.add_options()
        ("c,config", "Path to YAML config file", cxxopts::value<std::string>())
        ("a,address", "NATS server address", cxxopts::value<std::string>())
        ("p,port", "NATS server port", cxxopts::value<uint16_t>())
        ("i,input-subject", "Input NATS subject", cxxopts::value<std::string>())
        ("f,format", "Binary format (msgpack|cbor|flexbuffers|zera|ion|bson|beve)", cxxopts::value<std::string>())
        ("engine", "Matching engine (atree|betree)", cxxopts::value<std::string>())
        ("output-prefix", "Output subject prefix", cxxopts::value<std::string>())
        ("queue-group", "Input queue group for load balancing", cxxopts::value<std::string>())
        ("subscribe-subject", "Subscription request subject", cxxopts::value<std::string>())
        ("unsubscribe-subject", "Unsubscription request subject", cxxopts::value<std::string>())
        ("lease-bucket", "NATS KV lease bucket name", cxxopts::value<std::string>())
        ("lease-ttl", "Lease TTL in seconds", cxxopts::value<uint32_t>())
        ("lease-check-interval", "Lease reconciliation interval in seconds", cxxopts::value<uint32_t>())
        ("attr", "Attribute as name:type (repeatable)", cxxopts::value<std::vector<std::string>>())
        ("workers", "Worker thread count (0 = auto)", cxxopts::value<unsigned int>())
        ("input-queue-max-messages", "Maximum queued input messages", cxxopts::value<std::size_t>())
        ("input-queue-max-bytes", "Maximum queued input bytes", cxxopts::value<std::size_t>())
        ("publish-max-inflight", "Maximum in-flight publication tasks", cxxopts::value<std::size_t>())
        ("publish-backpressure-timeout-ms", "NATS publish backpressure timeout", cxxopts::value<uint32_t>())
        ("tls-cert", "TLS certificate path", cxxopts::value<std::string>())
        ("tls-key", "TLS key path", cxxopts::value<std::string>())
        ("tls-ca", "TLS CA certificate path", cxxopts::value<std::string>())
        ("stats-interval", "Stats log interval in seconds", cxxopts::value<int>())
        ("log-level", "Log level (debug|info|warn|error)", cxxopts::value<std::string>())
        ("generate-schema", "Infer attributes from a sample binary file", cxxopts::value<std::string>())
        ("v,verbose", "Enable debug logging")
        ("h,help", "Print help");

    return options;
}

std::optional<std::string> apply_cli_overrides(config& cfg, const cxxopts::ParseResult& result) {
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
    if (result.count("input-queue-max-messages")) cfg.input_queue_max_messages = result["input-queue-max-messages"].as<std::size_t>();
    if (result.count("input-queue-max-bytes")) cfg.input_queue_max_bytes = result["input-queue-max-bytes"].as<std::size_t>();
    if (result.count("publish-max-inflight")) cfg.publish_max_inflight = result["publish-max-inflight"].as<std::size_t>();
    if (result.count("publish-backpressure-timeout-ms")) cfg.publish_backpressure_timeout_ms = result["publish-backpressure-timeout-ms"].as<uint32_t>();
    if (result.count("tls-cert"))             cfg.tls_cert = result["tls-cert"].as<std::string>();
    if (result.count("tls-key"))              cfg.tls_key = result["tls-key"].as<std::string>();
    if (result.count("tls-ca"))               cfg.tls_ca = result["tls-ca"].as<std::string>();
    if (result.count("stats-interval"))       cfg.stats_interval_seconds = result["stats-interval"].as<int>();
    if (result.count("log-level"))            cfg.log_level = result["log-level"].as<std::string>();
    if (result.count("verbose"))              cfg.log_level = "debug";

    if (result.count("format")) {
        auto fmt_opt = parse_format(result["format"].as<std::string>());
        if (!fmt_opt) {
            return fmt::format("Invalid format: {}", result["format"].as<std::string>());
        }
        cfg.format = *fmt_opt;
    }

    if (result.count("engine")) {
        auto eng_opt = parse_engine_type(result["engine"].as<std::string>());
        if (!eng_opt) {
            return fmt::format("Invalid engine: {}", result["engine"].as<std::string>());
        }
        cfg.engine = *eng_opt;
    }

    // Parse --attr name:type pairs (appended to any YAML-defined attributes)
    if (result.count("attr")) {
        for (const auto& raw : result["attr"].as<std::vector<std::string>>()) {
            auto colon = raw.find(':');
            if (colon == std::string::npos) {
                return fmt::format("Invalid --attr '{}': expected name:type", raw);
            }
            auto name = raw.substr(0, colon);
            auto type_str = raw.substr(colon + 1);
            auto type = parse_attribute_type(type_str);
            if (!type) {
                return fmt::format("Invalid attribute type '{}' in --attr '{}'", type_str, raw);
            }
            cfg.attributes.push_back({std::move(name), *type});
        }
    }

    return std::nullopt;
}

std::optional<std::string> finalize_and_validate_config(config& cfg) {
    // Default output_prefix to input_subject if still empty
    if (cfg.output_prefix.empty()) cfg.output_prefix = cfg.input_subject;

    if (cfg.input_subject.empty()) {
        return "input_subject is required (via config file or --input-subject)";
    }
    if (cfg.attributes.empty()) {
        return "At least one attribute is required (via config file or --attr)";
    }
    if (cfg.lease_ttl_seconds == 0 || cfg.lease_check_interval_seconds == 0 ||
        cfg.input_queue_max_messages == 0 ||
        cfg.input_queue_max_bytes == 0 || cfg.publish_max_inflight == 0 ||
        cfg.publish_backpressure_timeout_ms == 0) {
        return "Lease TTL and all queue/publication limits must be greater than zero";
    }

    return std::nullopt;
}

unsigned int effective_worker_count(const config& cfg) {
    unsigned int workers = cfg.worker_threads > 0
        ? cfg.worker_threads
        : std::thread::hardware_concurrency();
    return workers == 0 ? 1 : workers;
}

} // namespace sidecar
