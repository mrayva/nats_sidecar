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
    zera,
    ion,
    bson,
    beve
};

// Boolean-expression matching engine backing subscription_manager.
enum class engine_type {
    atree,
    betree
};

struct config {
    // NATS connection
    std::string nats_address = "127.0.0.1";
    uint16_t nats_port = 4222;
    std::string tls_cert;
    std::string tls_key;
    std::string tls_ca;

    // Input streams - core NATS subjects with binary messages, all sharing
    // this config's one attribute schema. Every message from every
    // configured subject is matched against the same subscription tree.
    std::vector<std::string> input_subjects;
    binary_format format = binary_format::msgpack;
    std::string input_queue_group;  // optional load-balancing across sidecars

    // Durable JetStream consumer input mode - loss-proof alternative to the
    // plain queue-group subscribe above, additive (input_queue_group stays
    // available and unchanged when this isn't set). Core NATS plain pub/sub
    // is at-most-once by design (no ack, no redelivery); this mode gets a
    // real delivery guarantee via explicit acks and max_ack_pending flow
    // control. Enabled by setting input_stream (non-empty); when enabled,
    // consumer_durable_name and consumer_deliver_subject are both required -
    // deliver_subject in particular must be an explicit, fixed value shared
    // by every instance, never left to auto-generate (nats_asio generates a
    // random per-connection inbox if unset, which would silently break
    // multi-instance load distribution rather than error out).
    std::string input_stream;
    std::string consumer_durable_name;
    std::string consumer_deliver_subject;
    std::string consumer_deliver_group;   // JetStream analog of input_queue_group
    uint64_t consumer_max_ack_pending = 1000;
    uint32_t consumer_ack_wait_seconds = 30;

    // Output - matched messages published to <output_prefix>.<BE-ID>
    // Defaults to the single input subject if there is exactly one; must
    // be set explicitly when input_subjects has more than one entry, since
    // there is no unambiguous default to pick among them.
    std::string output_prefix;

    // Subscription management - clients send requests here
    std::string subscribe_subject = "sidecar.subscribe";
    std::string unsubscribe_subject = "sidecar.unsubscribe";

    // Soft-state leases via NATS KV
    std::string lease_bucket = "sidecar-leases";
    uint32_t lease_ttl_seconds = 3600;
    uint32_t lease_check_interval_seconds = 60;

    // Boolean-expression attribute schema
    std::vector<attribute_def> attributes;

    // Matching engine backing subscription_manager
    engine_type engine = engine_type::atree;

    // Operational
    int stats_interval_seconds = 10;
    std::string log_level = "info";

    // Worker threads for parallel message processing (0 = hardware_concurrency)
    unsigned int worker_threads = 0;

    // Bounded input queue. Newest messages are dropped when either limit is hit.
    std::size_t input_queue_max_messages = 10000;
    std::size_t input_queue_max_bytes = 64ULL * 1024 * 1024;

    // Bounded detached publication work and NATS write backpressure timeout.
    std::size_t publish_max_inflight = 1024;
    uint32_t publish_backpressure_timeout_ms = 5000;

    bool jetstream_consumer_enabled() const { return !input_stream.empty(); }
};

// Parse config from YAML file. Throws on error.
config load_config(const std::string& path);

// Parse binary_format from string. Returns nullopt if invalid.
std::optional<binary_format> parse_format(const std::string& s);

// Parse engine_type from string ("atree" | "betree"). Returns nullopt if invalid.
std::optional<engine_type> parse_engine_type(const std::string& s);

// Parse attribute_type from string. Returns nullopt if invalid.
std::optional<attribute_type> parse_attribute_type(const std::string& s);

} // namespace sidecar
