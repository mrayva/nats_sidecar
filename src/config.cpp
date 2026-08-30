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
    if (s == "arrow")       return binary_format::arrow;
    return std::nullopt;
}

std::optional<engine_type> parse_engine_type(const std::string& s) {
    if (s == "atree")  return engine_type::atree;
    if (s == "betree") return engine_type::betree;
    if (s == "pstree") return engine_type::pstree;
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

std::vector<input_connection> config::effective_connections() const {
    if (!connections.empty()) return connections;

    input_connection c;
    c.name = "default";
    c.mode = input_stream.empty() ? "core" : "js";
    c.subjects = input_subjects;
    c.queue_group = input_queue_group;
    c.columnar = input_columnar;
    c.stream = input_stream;
    c.consumer_durable_name = consumer_durable_name;
    c.consumer_deliver_subject = consumer_deliver_subject;
    c.consumer_deliver_group = consumer_deliver_group;
    c.consumer_max_ack_pending = consumer_max_ack_pending;
    c.consumer_ack_wait_seconds = consumer_ack_wait_seconds;
    c.stream_storage = input_stream_storage;
    return {std::move(c)};
}

namespace {

// Whether the YAML root has any legacy flat input field set - used to
// reject a config file that mixes the new `connections:` block with the
// old flat shape, since merging the two would be ambiguous.
bool has_legacy_input_fields(const YAML::Node& root) {
    static constexpr const char* legacy_keys[] = {
        "input_subjects", "input_queue_group", "input_columnar", "input_stream",
        "consumer_durable_name", "consumer_deliver_subject",
        "consumer_deliver_group", "consumer_max_ack_pending",
        "consumer_ack_wait_seconds", "input_stream_storage"
    };
    for (const auto* key : legacy_keys) {
        if (root[key]) return true;
    }
    return false;
}

input_connection parse_connection(const YAML::Node& node) {
    input_connection c;

    if (auto n = node["name"]) {
        c.name = n.as<std::string>();
    }
    if (c.name.empty()) {
        throw std::runtime_error("config: every entry in 'connections' requires a non-empty 'name'");
    }

    if (auto n = node["mode"]) {
        c.mode = n.as<std::string>();
        if (c.mode != "js" && c.mode != "core") {
            throw std::runtime_error(
                "config: connection '" + c.name + "' has invalid mode '" + c.mode +
                "' (expected 'js' or 'core')");
        }
    }

    if (auto n = node["subjects"]) {
        if (!n.IsSequence()) {
            throw std::runtime_error(
                "config: connection '" + c.name + "' 'subjects' must be a list");
        }
        for (const auto& item : n) c.subjects.push_back(item.as<std::string>());
    }
    if (c.subjects.empty()) {
        throw std::runtime_error(
            "config: connection '" + c.name + "' must have at least one subject");
    }

    if (auto n = node["queue_group"]) c.queue_group = n.as<std::string>();
    if (auto n = node["columnar"])    c.columnar = n.as<bool>();

    if (auto n = node["stream"])                   c.stream = n.as<std::string>();
    if (auto n = node["consumer_durable_name"])    c.consumer_durable_name = n.as<std::string>();
    if (auto n = node["consumer_deliver_subject"]) c.consumer_deliver_subject = n.as<std::string>();
    if (auto n = node["consumer_deliver_group"])   c.consumer_deliver_group = n.as<std::string>();
    if (auto n = node["consumer_max_ack_pending"])  c.consumer_max_ack_pending = n.as<uint64_t>();
    if (auto n = node["consumer_ack_wait_seconds"]) c.consumer_ack_wait_seconds = n.as<uint32_t>();
    if (auto n = node["stream_storage"])            c.stream_storage = n.as<std::string>();

    return c;
}

} // namespace

config load_config(const std::string& path) {
    YAML::Node root = YAML::LoadFile(path);
    config cfg;

    // NATS connection
    if (auto n = root["nats_address"]) cfg.nats_address = n.as<std::string>();
    if (auto n = root["nats_port"])    cfg.nats_port = n.as<uint16_t>();
    if (auto n = root["tls_cert"])     cfg.tls_cert = n.as<std::string>();
    if (auto n = root["tls_key"])      cfg.tls_key  = n.as<std::string>();
    if (auto n = root["tls_ca"])       cfg.tls_ca   = n.as<std::string>();

    // Input - either the new 'connections' list (preferred) or the legacy
    // flat single-connection fields, never both in the same file.
    if (auto n = root["connections"]) {
        if (has_legacy_input_fields(root)) {
            throw std::runtime_error(
                "config: 'connections' cannot be combined with the legacy flat input "
                "fields (input_subjects, input_stream, input_queue_group, "
                "consumer_durable_name, consumer_deliver_subject, consumer_deliver_group, "
                "consumer_max_ack_pending, consumer_ack_wait_seconds, "
                "input_stream_storage) - use one shape or the other");
        }
        if (!n.IsSequence()) throw std::runtime_error("config: 'connections' must be a list");
        for (const auto& item : n) cfg.connections.push_back(parse_connection(item));
        if (cfg.connections.empty()) {
            throw std::runtime_error("config: 'connections' must not be empty");
        }
        for (std::size_t i = 0; i < cfg.connections.size(); ++i) {
            for (std::size_t j = i + 1; j < cfg.connections.size(); ++j) {
                if (cfg.connections[i].name == cfg.connections[j].name) {
                    throw std::runtime_error(
                        "config: duplicate connection name '" + cfg.connections[i].name + "'");
                }
            }
        }
    } else if (auto legacy = root["input_subjects"]) {
        if (!legacy.IsSequence()) throw std::runtime_error("config: 'input_subjects' must be a list");
        for (const auto& item : legacy) cfg.input_subjects.push_back(item.as<std::string>());
        if (cfg.input_subjects.empty()) {
            throw std::runtime_error("config: 'input_subjects' must not be empty");
        }
    } else {
        throw std::runtime_error("config: either 'connections' or 'input_subjects' is required");
    }

    if (auto n = root["format"]) {
        auto fmt = parse_format(n.as<std::string>());
        if (!fmt) throw std::runtime_error("config: invalid 'format': " + n.as<std::string>());
        cfg.format = *fmt;
    }

    if (auto n = root["output_format"]) {
        auto fmt = parse_format(n.as<std::string>());
        if (!fmt) throw std::runtime_error("config: invalid 'output_format': " + n.as<std::string>());
        cfg.output_format = *fmt;
    }

    if (auto n = root["input_queue_group"]) cfg.input_queue_group = n.as<std::string>();
    if (auto n = root["input_columnar"])    cfg.input_columnar = n.as<bool>();

    if (auto n = root["input_stream"])             cfg.input_stream = n.as<std::string>();
    if (auto n = root["consumer_durable_name"])    cfg.consumer_durable_name = n.as<std::string>();
    if (auto n = root["consumer_deliver_subject"]) cfg.consumer_deliver_subject = n.as<std::string>();
    if (auto n = root["consumer_deliver_group"])   cfg.consumer_deliver_group = n.as<std::string>();
    if (auto n = root["consumer_max_ack_pending"]) cfg.consumer_max_ack_pending = n.as<uint64_t>();
    if (auto n = root["consumer_ack_wait_seconds"]) cfg.consumer_ack_wait_seconds = n.as<uint32_t>();
    if (auto n = root["input_stream_storage"])     cfg.input_stream_storage = n.as<std::string>();

    if (auto n = root["engine"]) {
        auto eng = parse_engine_type(n.as<std::string>());
        if (!eng) throw std::runtime_error("config: invalid 'engine': " + n.as<std::string>());
        cfg.engine = *eng;
    }

    // Output - shared by every connection. Defaults to the single subject
    // when exactly one is configured in total (across every connection);
    // finalize_and_validate_config() raises a clear error otherwise rather
    // than silently picking one as the default.
    if (auto n = root["output_prefix"]) {
        cfg.output_prefix = n.as<std::string>();
    } else {
        std::size_t total_subjects = 0;
        std::string only_subject;
        for (const auto& c : cfg.effective_connections()) {
            total_subjects += c.subjects.size();
            if (total_subjects == 1) only_subject = c.subjects.front();
        }
        if (total_subjects == 1) cfg.output_prefix = only_subject;
    }

    // Subscription subjects
    if (auto n = root["subscribe_subject"])   cfg.subscribe_subject   = n.as<std::string>();
    if (auto n = root["unsubscribe_subject"]) cfg.unsubscribe_subject = n.as<std::string>();

    // Leases
    if (auto n = root["lease_bucket"])                 cfg.lease_bucket = n.as<std::string>();
    if (auto n = root["lease_ttl_seconds"])             cfg.lease_ttl_seconds = n.as<uint32_t>();
    if (auto n = root["lease_check_interval_seconds"])  cfg.lease_check_interval_seconds = n.as<uint32_t>();

    // Subscription-id registry
    if (auto n = root["registry_bucket"]) cfg.registry_bucket = n.as<std::string>();

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
    if (auto n = root["publish_chunk_bytes"])      cfg.publish_chunk_bytes = n.as<std::size_t>();

    return cfg;
}

} // namespace sidecar
