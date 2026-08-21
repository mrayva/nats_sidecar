#include "config.hpp"
#include <yaml-cpp/yaml.h>
#include <stdexcept>

namespace sidecar {

std::optional<binary_format> parse_format(const std::string& s) {
    if (s == "msgpack")     return binary_format::msgpack;
    if (s == "cbor")        return binary_format::cbor;
    if (s == "flexbuffers") return binary_format::flexbuffers;
    if (s == "zera")        return binary_format::zera;
    if (s == "ion")         return binary_format::ion;
    if (s == "bson")        return binary_format::bson;
    if (s == "beve")        return binary_format::beve;
    return std::nullopt;
}

std::optional<engine_type> parse_engine_type(const std::string& s) {
    if (s == "atree")  return engine_type::atree;
    if (s == "betree") return engine_type::betree;
    return std::nullopt;
}

std::optional<attribute_type> parse_attribute_type(const std::string& s) {
    if (s == "boolean" || s == "bool")     return attribute_type::boolean;
    if (s == "integer" || s == "int")      return attribute_type::integer;
    if (s == "float" || s == "double")     return attribute_type::float_val;
    if (s == "string" || s == "str")       return attribute_type::string;
    if (s == "string_list")                return attribute_type::string_list;
    if (s == "integer_list" || s == "int_list") return attribute_type::integer_list;
    return std::nullopt;
}

config load_config(const std::string& path) {
    YAML::Node root = YAML::LoadFile(path);
    config cfg;

    // NATS connection
    if (auto n = root["nats_address"]) cfg.nats_address = n.as<std::string>();
    if (auto n = root["nats_port"])    cfg.nats_port = n.as<uint16_t>();
    if (auto n = root["tls_cert"])     cfg.tls_cert = n.as<std::string>();
    if (auto n = root["tls_key"])      cfg.tls_key  = n.as<std::string>();
    if (auto n = root["tls_ca"])       cfg.tls_ca   = n.as<std::string>();

    // Input
    if (auto n = root["input_subjects"]) {
        if (!n.IsSequence()) throw std::runtime_error("config: 'input_subjects' must be a list");
        for (const auto& item : n) cfg.input_subjects.push_back(item.as<std::string>());
        if (cfg.input_subjects.empty()) {
            throw std::runtime_error("config: 'input_subjects' must not be empty");
        }
    } else {
        throw std::runtime_error("config: 'input_subjects' is required");
    }

    if (auto n = root["format"]) {
        auto fmt = parse_format(n.as<std::string>());
        if (!fmt) throw std::runtime_error("config: invalid 'format': " + n.as<std::string>());
        cfg.format = *fmt;
    }

    if (auto n = root["input_queue_group"]) cfg.input_queue_group = n.as<std::string>();

    if (auto n = root["engine"]) {
        auto eng = parse_engine_type(n.as<std::string>());
        if (!eng) throw std::runtime_error("config: invalid 'engine': " + n.as<std::string>());
        cfg.engine = *eng;
    }

    // Output
    if (auto n = root["output_prefix"]) {
        cfg.output_prefix = n.as<std::string>();
    } else if (cfg.input_subjects.size() == 1) {
        cfg.output_prefix = cfg.input_subjects.front();
    }
    // else: left empty here: finalize_and_validate_config() (shared with the
    // CLI path) raises a clear error for the multi-subject case rather than
    // silently picking one input subject as the default.

    // Subscription subjects
    if (auto n = root["subscribe_subject"])   cfg.subscribe_subject   = n.as<std::string>();
    if (auto n = root["unsubscribe_subject"]) cfg.unsubscribe_subject = n.as<std::string>();

    // Leases
    if (auto n = root["lease_bucket"])                 cfg.lease_bucket = n.as<std::string>();
    if (auto n = root["lease_ttl_seconds"])             cfg.lease_ttl_seconds = n.as<uint32_t>();
    if (auto n = root["lease_check_interval_seconds"])  cfg.lease_check_interval_seconds = n.as<uint32_t>();

    // Attributes (required)
    if (auto attrs = root["attributes"]) {
        if (!attrs.IsSequence()) throw std::runtime_error("config: 'attributes' must be a list");
        for (const auto& item : attrs) {
            attribute_def def;
            def.name = item["name"].as<std::string>();
            auto type = parse_attribute_type(item["type"].as<std::string>());
            if (!type) throw std::runtime_error("config: invalid attribute type: " + item["type"].as<std::string>());
            def.type = *type;
            cfg.attributes.push_back(std::move(def));
        }
    } else {
        throw std::runtime_error("config: 'attributes' is required");
    }

    if (cfg.attributes.empty()) {
        throw std::runtime_error("config: 'attributes' must not be empty");
    }

    // Operational
    if (auto n = root["stats_interval_seconds"]) cfg.stats_interval_seconds = n.as<int>();
    if (auto n = root["log_level"])              cfg.log_level = n.as<std::string>();
    if (auto n = root["worker_threads"])         cfg.worker_threads = n.as<unsigned int>();
    if (auto n = root["input_queue_max_messages"]) cfg.input_queue_max_messages = n.as<std::size_t>();
    if (auto n = root["input_queue_max_bytes"])    cfg.input_queue_max_bytes = n.as<std::size_t>();
    if (auto n = root["publish_max_inflight"])     cfg.publish_max_inflight = n.as<std::size_t>();
    if (auto n = root["publish_backpressure_timeout_ms"]) {
        cfg.publish_backpressure_timeout_ms = n.as<uint32_t>();
    }

    return cfg;
}

} // namespace sidecar
