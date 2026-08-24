#include "cli.hpp"
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <thread>
#include <unordered_map>

namespace sidecar {

cxxopts::Options build_cli_options() {
    cxxopts::Options options("nats_sidecar",
        "Content-based filtering sidecar for NATS");

    options.add_options()
        ("c,config", "Path to YAML config file", cxxopts::value<std::string>())
        ("a,address", "NATS server address", cxxopts::value<std::string>())
        ("p,port", "NATS server port", cxxopts::value<uint16_t>())
        ("i,input-subject", "Input NATS subject (repeatable for multiple inputs; "
                             "replaces any config-file input_subjects entirely if given)",
                             cxxopts::value<std::vector<std::string>>())
        ("f,format", "Binary format (msgpack|cbor|flexbuffers|zera|ion|bson|beve)", cxxopts::value<std::string>())
        ("engine", "Matching engine (atree|betree)", cxxopts::value<std::string>())
        ("output-prefix", "Output subject prefix", cxxopts::value<std::string>())
        ("queue-group", "Input queue group for load balancing", cxxopts::value<std::string>())
        ("input-stream", "JetStream stream name for input; enables the durable-consumer "
                          "(loss-proof) input mode instead of plain queue-group subscribe",
                          cxxopts::value<std::string>())
        ("consumer-durable-name", "Durable JetStream consumer name, shared across every "
                                   "instance (required with --input-stream)",
                                   cxxopts::value<std::string>())
        ("consumer-deliver-subject", "Fixed push-delivery subject shared by every instance "
                                      "(required with --input-stream - never leave unset)",
                                      cxxopts::value<std::string>())
        ("consumer-deliver-group", "Queue group on the deliver subject, for load "
                                    "distribution across instances (JetStream analog of "
                                    "--queue-group)", cxxopts::value<std::string>())
        ("consumer-max-ack-pending", "Max unacked messages in flight to this consumer "
                                      "(flow control)", cxxopts::value<uint64_t>())
        ("consumer-ack-wait", "Ack wait timeout in seconds before redelivery",
                               cxxopts::value<uint32_t>())
        ("input-stream-storage", "JetStream input stream storage backend: file (default, "
                                  "real durability) or memory (throughput-isolation only, "
                                  "loses everything on nats-server restart)",
                                  cxxopts::value<std::string>())
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
    // Single-value input overrides only make sense against the legacy
    // single-connection shape - reject them outright when the config file
    // already defines a 'connections' list, since there's no way to know
    // which named connection a bare flag like --input-stream should target.
    static constexpr const char* legacy_input_flags[] = {
        "input-subject", "queue-group", "input-stream", "input-stream-storage",
        "consumer-durable-name", "consumer-deliver-subject", "consumer-deliver-group",
        "consumer-max-ack-pending", "consumer-ack-wait"
    };
    if (!cfg.connections.empty()) {
        for (const auto* flag : legacy_input_flags) {
            if (result.count(flag)) {
                return fmt::format(
                    "--{} cannot be used together with a config file 'connections' list "
                    "(ambiguous which connection it targets) - edit the config file instead",
                    flag);
            }
        }
    }

    if (result.count("address"))              cfg.nats_address = result["address"].as<std::string>();
    if (result.count("port"))                 cfg.nats_port = result["port"].as<uint16_t>();
    if (result.count("input-subject"))        cfg.input_subjects = result["input-subject"].as<std::vector<std::string>>();
    if (result.count("output-prefix"))        cfg.output_prefix = result["output-prefix"].as<std::string>();
    if (result.count("queue-group"))          cfg.input_queue_group = result["queue-group"].as<std::string>();
    if (result.count("input-stream"))              cfg.input_stream = result["input-stream"].as<std::string>();
    if (result.count("input-stream-storage"))      cfg.input_stream_storage = result["input-stream-storage"].as<std::string>();
    if (result.count("consumer-durable-name"))     cfg.consumer_durable_name = result["consumer-durable-name"].as<std::string>();
    if (result.count("consumer-deliver-subject"))  cfg.consumer_deliver_subject = result["consumer-deliver-subject"].as<std::string>();
    if (result.count("consumer-deliver-group"))    cfg.consumer_deliver_group = result["consumer-deliver-group"].as<std::string>();
    if (result.count("consumer-max-ack-pending"))  cfg.consumer_max_ack_pending = result["consumer-max-ack-pending"].as<uint64_t>();
    if (result.count("consumer-ack-wait"))         cfg.consumer_ack_wait_seconds = result["consumer-ack-wait"].as<uint32_t>();
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
    auto conns = cfg.effective_connections();

    std::size_t total_subjects = 0;
    for (const auto& c : conns) total_subjects += c.subjects.size();

    // Default output_prefix to the single input subject if still empty and
    // unambiguous; with more than one input subject total there's no sane
    // default.
    if (cfg.output_prefix.empty() && total_subjects == 1) {
        for (const auto& c : conns) {
            if (!c.subjects.empty()) { cfg.output_prefix = c.subjects.front(); break; }
        }
    }

    if (conns.empty() || total_subjects == 0) {
        return "at least one input subject is required (via config file's "
               "'connections'/'input_subjects', or --input-subject)";
    }
    if (cfg.output_prefix.empty()) {
        std::vector<std::string> all_subjects;
        for (const auto& c : conns) {
            all_subjects.insert(all_subjects.end(), c.subjects.begin(), c.subjects.end());
        }
        return fmt::format(
            "output_prefix is required when more than one input subject is configured "
            "(got {} input subjects: {}) - there is no unambiguous default to pick among them",
            total_subjects, fmt::join(all_subjects, ", "));
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

    // Per-connection JetStream durable-consumer validation:
    // consumer_durable_name and consumer_deliver_subject are both required,
    // not defaulted. In particular, an unset deliver_subject would make
    // nats_asio generate a random per-connection inbox - each instance would
    // silently rebind the shared durable consumer to a different target,
    // breaking multi-instance load distribution without any error. Fail
    // loudly here instead.
    for (const auto& c : conns) {
        if (!c.jetstream()) continue;
        if (c.consumer_durable_name.empty()) {
            return fmt::format("connection '{}': consumer_durable_name is required in js mode", c.name);
        }
        if (c.consumer_deliver_subject.empty()) {
            return fmt::format(
                "connection '{}': consumer_deliver_subject is required in js mode "
                "(it must be a fixed value shared by every instance - never left "
                "unset, which would silently break multi-instance load distribution)",
                c.name);
        }
        if (c.consumer_max_ack_pending == 0) {
            return fmt::format("connection '{}': consumer_max_ack_pending must be greater than zero", c.name);
        }
        if (c.consumer_ack_wait_seconds == 0) {
            return fmt::format("connection '{}': consumer_ack_wait_seconds must be greater than zero", c.name);
        }
        if (c.stream_storage != "file" && c.stream_storage != "memory") {
            return fmt::format("connection '{}': stream_storage must be 'file' or 'memory'", c.name);
        }
    }

    // Cross-connection uniqueness: a collision in any of these would either
    // silently merge two unrelated durable consumers onto the same stream,
    // or double-enqueue the same message into matching.
    std::unordered_map<std::string, std::string> seen_streams, seen_durables, seen_delivers;
    std::unordered_map<std::string, std::string> seen_subjects;
    for (const auto& c : conns) {
        if (c.jetstream()) {
            if (!c.stream.empty()) {
                auto [it, inserted] = seen_streams.emplace(c.stream, c.name);
                if (!inserted) {
                    return fmt::format("connections '{}' and '{}' both use stream '{}'",
                                        it->second, c.name, c.stream);
                }
            }
            {
                auto [it, inserted] = seen_durables.emplace(c.consumer_durable_name, c.name);
                if (!inserted) {
                    return fmt::format(
                        "connections '{}' and '{}' both use consumer_durable_name '{}'",
                        it->second, c.name, c.consumer_durable_name);
                }
            }
            {
                auto [it, inserted] = seen_delivers.emplace(c.consumer_deliver_subject, c.name);
                if (!inserted) {
                    return fmt::format(
                        "connections '{}' and '{}' both use consumer_deliver_subject '{}'",
                        it->second, c.name, c.consumer_deliver_subject);
                }
            }
        }
        for (const auto& subject : c.subjects) {
            auto [it, inserted] = seen_subjects.emplace(subject, c.name);
            if (!inserted) {
                return fmt::format("connections '{}' and '{}' both subscribe to subject '{}'",
                                    it->second, c.name, subject);
            }
        }
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
