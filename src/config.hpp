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

// One independent input source: a named group of subjects, feeding either a
// plain core-NATS queue-group subscribe ("core") or a durable JetStream
// consumer ("js", the default - the loss-proof mode). A process can run any
// number of these simultaneously, each choosing its own mode/subjects, all
// funneling into the same shared matching tree and output_prefix (see
// config::output_prefix) - "connections" is purely an input-side concept,
// not a separate NATS TCP connection or a separate output namespace.
struct input_connection {
    // Required, non-empty, unique among connections. Used in logs and to
    // scope stream/durable/deliver_subject collision checks.
    std::string name;

    // "js" (default) or "core".
    std::string mode = "js";

    std::vector<std::string> subjects;

    // core mode only: optional load-balancing group.
    std::string queue_group;

    // js mode only - same semantics as the legacy flat fields below.
    // consumer_durable_name and consumer_deliver_subject are both required;
    // deliver_subject in particular must be an explicit, fixed value shared
    // by every instance, never left to auto-generate (nats_asio generates a
    // random per-connection inbox if unset, which would silently break
    // multi-instance load distribution rather than error out).
    std::string stream;
    std::string consumer_durable_name;
    std::string consumer_deliver_subject;
    std::string consumer_deliver_group;   // JetStream analog of queue_group
    uint64_t consumer_max_ack_pending = 1000;
    uint32_t consumer_ack_wait_seconds = 30;
    // "file" (default, real durability) or "memory" (throughput-isolation
    // tool only - loses everything on nats-server restart/crash).
    std::string stream_storage = "file";

    bool jetstream() const { return mode == "js"; }
};

struct config {
    // NATS connection
    std::string nats_address = "127.0.0.1";
    uint16_t nats_port = 4222;
    std::string tls_cert;
    std::string tls_key;
    std::string tls_ca;

    // Multiple independent input connections (mixed js/core mode), the
    // current, preferred way to configure input. When non-empty,
    // effective_connections() below returns this list verbatim and every
    // legacy flat field below is ignored (load_config() rejects a config
    // file that sets both, to avoid ambiguous merge semantics).
    std::vector<input_connection> connections;

    // --- Legacy single-connection fields (deprecated but still functional) ---
    // Only ever consumed via effective_connections() below, never read
    // directly by sidecar_engine/worker_pool - kept for config-file/CLI
    // back-compat with the pre-multi-connection single-input-source shape.
    std::vector<std::string> input_subjects;
    std::string input_queue_group;  // optional load-balancing across sidecars
    std::string input_stream;
    std::string consumer_durable_name;
    std::string consumer_deliver_subject;
    std::string consumer_deliver_group;   // JetStream analog of input_queue_group
    uint64_t consumer_max_ack_pending = 1000;
    uint32_t consumer_ack_wait_seconds = 30;
    std::string input_stream_storage = "file";

    binary_format format = binary_format::msgpack;

    // Output - matched messages published to <output_prefix>.<BE-ID>,
    // shared by every input connection (a client's boolean expression
    // matches a message regardless of which connection it arrived on).
    // Defaults to the single input subject if there is exactly one across
    // all connections; must be set explicitly otherwise, since there is no
    // unambiguous default to pick among them.
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

    // Returns `connections` verbatim if non-empty; otherwise synthesizes
    // exactly one connection (name "default", mode "js" if input_stream is
    // set else "core") from the legacy flat fields above, reproducing the
    // pre-multi-connection single-input-source behavior exactly. This is
    // the only place the legacy flat fields should be read after config
    // construction - sidecar_engine, worker_pool, and cli.cpp's validation
    // all call this instead of touching the flat fields directly.
    std::vector<input_connection> effective_connections() const;
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
