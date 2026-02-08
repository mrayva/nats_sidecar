#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <optional>

namespace sidecar {

// Matches atree::AttributeType
enum class attribute_type {
    boolean,
    integer,
    float_val,
    string,
    string_list,
    integer_list
};

struct attribute_def {
    std::string name;
    attribute_type type;
};

// Supported binary serialization formats
enum class binary_format {
    msgpack,
    cbor,
    flexbuffers,
    zera
};

struct config {
    // NATS connection
    std::string nats_address = "127.0.0.1";
    uint16_t nats_port = 4222;
    std::string tls_cert;
    std::string tls_key;
    std::string tls_ca;

    // Input stream - core NATS subject with binary messages
    std::string input_subject;
    binary_format format = binary_format::msgpack;
    std::string input_queue_group;  // optional load-balancing across sidecars

    // Output - matched messages published to <output_prefix>.<BE-ID>
    std::string output_prefix;  // defaults to input_subject if empty

    // Subscription management - clients send requests here
    std::string subscribe_subject = "sidecar.subscribe";
    std::string unsubscribe_subject = "sidecar.unsubscribe";

    // Soft-state leases via NATS KV
    std::string lease_bucket = "sidecar-leases";
    uint32_t lease_ttl_seconds = 3600;
    uint32_t lease_check_interval_seconds = 60;

    // A-Tree attribute schema
    std::vector<attribute_def> attributes;

    // Operational
    int stats_interval_seconds = 10;
    std::string log_level = "info";

    // Worker threads for parallel message processing (0 = hardware_concurrency)
    unsigned int worker_threads = 0;
};

// Parse config from YAML file. Throws on error.
config load_config(const std::string& path);

// Parse binary_format from string. Returns nullopt if invalid.
std::optional<binary_format> parse_format(const std::string& s);

// Parse attribute_type from string. Returns nullopt if invalid.
std::optional<attribute_type> parse_attribute_type(const std::string& s);

} // namespace sidecar
