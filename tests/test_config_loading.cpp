#include "config.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

// Writes `contents` to a uniquely-named temp file for the lifetime of the
// object and removes it on destruction.
class temp_yaml_file {
public:
    explicit temp_yaml_file(const std::string& contents) : m_path(make_path()) {
        std::ofstream out(m_path);
        out << contents;
        m_path_str = m_path.string();
    }
    ~temp_yaml_file() { std::error_code ec; std::filesystem::remove(m_path, ec); }

    temp_yaml_file(const temp_yaml_file&) = delete;
    temp_yaml_file& operator=(const temp_yaml_file&) = delete;

    const std::string& path() const { return m_path_str; }

private:
    static std::filesystem::path make_path() {
        auto dir = std::filesystem::temp_directory_path();
        return dir / ("sidecar_test_config_" + std::to_string(m_counter.fetch_add(1)) + ".yaml");
    }

    static inline std::atomic<int> m_counter{0};
    std::filesystem::path m_path;
    std::string m_path_str;
};

} // namespace

TEST(config_loading, loads_minimal_config_with_defaults) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
attributes:
  - name: temperature
    type: float
)");

    auto cfg = sidecar::load_config(file.path());
    ASSERT_EQ(cfg.input_subjects.size(), 1u);
    EXPECT_EQ(cfg.input_subjects[0], "sensor.data");
    EXPECT_EQ(cfg.output_prefix, "sensor.data");  // defaults to the single input subject
    ASSERT_EQ(cfg.attributes.size(), 1u);
    EXPECT_EQ(cfg.attributes[0].name, "temperature");
    EXPECT_EQ(cfg.attributes[0].type, sidecar::attribute_type::float_val);

    // Defaults untouched by the file
    EXPECT_EQ(cfg.nats_address, "127.0.0.1");
    EXPECT_EQ(cfg.nats_port, 4222);
    EXPECT_EQ(cfg.format, sidecar::binary_format::msgpack);
    EXPECT_EQ(cfg.lease_bucket, "sidecar-leases");
    EXPECT_EQ(cfg.lease_ttl_seconds, 3600u);
    EXPECT_EQ(cfg.registry_bucket, "sidecar-subscriptions");
}

TEST(config_loading, explicit_output_prefix_overrides_default) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
output_prefix: sensor.filtered
attributes:
  - name: temperature
    type: float
)");

    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
}

TEST(config_loading, overrides_operational_and_queue_fields) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
format: cbor
input_queue_group: workers
subscribe_subject: custom.subscribe
unsubscribe_subject: custom.unsubscribe
lease_bucket: my-leases
lease_ttl_seconds: 120
lease_check_interval_seconds: 5
registry_bucket: my-registry
stats_interval_seconds: 30
log_level: debug
worker_threads: 4
input_queue_max_messages: 500
input_queue_max_bytes: 1048576
publish_max_inflight: 16
publish_backpressure_timeout_ms: 2500
attributes:
  - name: active
    type: bool
  - name: tags
    type: string_list
)");

    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.format, sidecar::binary_format::cbor);
    EXPECT_EQ(cfg.input_queue_group, "workers");
    EXPECT_EQ(cfg.subscribe_subject, "custom.subscribe");
    EXPECT_EQ(cfg.unsubscribe_subject, "custom.unsubscribe");
    EXPECT_EQ(cfg.lease_bucket, "my-leases");
    EXPECT_EQ(cfg.lease_ttl_seconds, 120u);
    EXPECT_EQ(cfg.lease_check_interval_seconds, 5u);
    EXPECT_EQ(cfg.registry_bucket, "my-registry");
    EXPECT_EQ(cfg.stats_interval_seconds, 30);
    EXPECT_EQ(cfg.log_level, "debug");
    EXPECT_EQ(cfg.worker_threads, 4u);
    EXPECT_EQ(cfg.input_queue_max_messages, 500u);
    EXPECT_EQ(cfg.input_queue_max_bytes, 1048576u);
    EXPECT_EQ(cfg.publish_max_inflight, 16u);
    EXPECT_EQ(cfg.publish_backpressure_timeout_ms, 2500u);
    ASSERT_EQ(cfg.attributes.size(), 2u);
    EXPECT_EQ(cfg.attributes[0].type, sidecar::attribute_type::boolean);
    EXPECT_EQ(cfg.attributes[1].type, sidecar::attribute_type::string_list);
}

TEST(config_loading, defaults_engine_to_atree) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
attributes:
  - name: temperature
    type: float
)");

    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.engine, sidecar::engine_type::atree);
}

TEST(config_loading, parses_engine_betree) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
engine: betree
attributes:
  - name: temperature
    type: float
)");

    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.engine, sidecar::engine_type::betree);
}

TEST(config_loading, invalid_engine_throws) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
engine: not-a-real-engine
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, missing_input_subjects_throws) {
    temp_yaml_file file(R"(
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, empty_input_subjects_list_throws) {
    temp_yaml_file file(R"(
input_subjects: []
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, multiple_input_subjects_with_explicit_output_prefix) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data, sensor.data.backup]
output_prefix: sensor.filtered
attributes:
  - name: temperature
    type: float
)");
    auto cfg = sidecar::load_config(file.path());
    ASSERT_EQ(cfg.input_subjects.size(), 2u);
    EXPECT_EQ(cfg.input_subjects[0], "sensor.data");
    EXPECT_EQ(cfg.input_subjects[1], "sensor.data.backup");
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
}

TEST(config_loading, multiple_input_subjects_without_output_prefix_leaves_it_empty) {
    // load_config() itself doesn't raise for this - the ambiguous-default
    // check lives in finalize_and_validate_config() (see test_cli.cpp),
    // shared by both the CLI and config-file paths. Confirm load_config()
    // just leaves output_prefix empty rather than guessing.
    temp_yaml_file file(R"(
input_subjects: [sensor.data, sensor.data.backup]
attributes:
  - name: temperature
    type: float
)");
    auto cfg = sidecar::load_config(file.path());
    EXPECT_TRUE(cfg.output_prefix.empty());
}

TEST(config_loading, missing_attributes_throws) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, empty_attributes_list_throws) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
attributes: []
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, invalid_format_throws) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
format: not-a-real-format
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, invalid_attribute_type_throws) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
attributes:
  - name: temperature
    type: not-a-real-type
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, nonexistent_file_throws) {
    EXPECT_THROW(sidecar::load_config("/nonexistent/path/does-not-exist.yaml"), std::exception);
}

// --- effective_connections(): back-compat synthesis from legacy flat fields ---

TEST(config_loading, effective_connections_synthesizes_core_mode_from_legacy_fields) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.input_queue_group = "workers";
    cfg.input_columnar = true;

    auto conns = cfg.effective_connections();
    ASSERT_EQ(conns.size(), 1u);
    EXPECT_EQ(conns[0].name, "default");
    EXPECT_EQ(conns[0].mode, "core");
    EXPECT_FALSE(conns[0].jetstream());
    EXPECT_EQ(conns[0].subjects, (std::vector<std::string>{"sensor.data"}));
    EXPECT_EQ(conns[0].queue_group, "workers");
    EXPECT_TRUE(conns[0].columnar);
}

TEST(config_loading, effective_connections_synthesizes_js_mode_when_input_stream_set) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.input_stream = "sensor-input";
    cfg.consumer_durable_name = "sensor-durable";
    cfg.consumer_deliver_subject = "sensor.deliver";
    cfg.consumer_max_ack_pending = 250;
    cfg.consumer_ack_wait_seconds = 20;
    cfg.input_stream_storage = "memory";

    auto conns = cfg.effective_connections();
    ASSERT_EQ(conns.size(), 1u);
    EXPECT_EQ(conns[0].mode, "js");
    EXPECT_TRUE(conns[0].jetstream());
    EXPECT_EQ(conns[0].stream, "sensor-input");
    EXPECT_EQ(conns[0].consumer_durable_name, "sensor-durable");
    EXPECT_EQ(conns[0].consumer_deliver_subject, "sensor.deliver");
    EXPECT_EQ(conns[0].consumer_max_ack_pending, 250u);
    EXPECT_EQ(conns[0].consumer_ack_wait_seconds, 20u);
    EXPECT_EQ(conns[0].stream_storage, "memory");
}

TEST(config_loading, effective_connections_returns_explicit_list_verbatim) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"x"};
    cfg.connections = {a};

    auto conns = cfg.effective_connections();
    ASSERT_EQ(conns.size(), 1u);
    EXPECT_EQ(conns[0].name, "a");
}

// --- 'connections:' YAML parsing ---

TEST(config_loading, parses_connections_with_mixed_js_and_core_modes) {
    temp_yaml_file file(R"(
connections:
  - name: orders
    subjects: [orders.in]
    stream: ORDERS
    consumer_durable_name: orders-durable
    consumer_deliver_subject: orders.deliver
  - name: telemetry
    mode: core
    subjects: [telemetry.in]
    queue_group: telemetry-workers
    columnar: true
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    auto cfg = sidecar::load_config(file.path());
    ASSERT_EQ(cfg.connections.size(), 2u);

    EXPECT_EQ(cfg.connections[0].name, "orders");
    EXPECT_EQ(cfg.connections[0].mode, "js");  // default
    EXPECT_TRUE(cfg.connections[0].jetstream());
    EXPECT_EQ(cfg.connections[0].subjects, (std::vector<std::string>{"orders.in"}));
    EXPECT_EQ(cfg.connections[0].stream, "ORDERS");
    EXPECT_EQ(cfg.connections[0].consumer_durable_name, "orders-durable");
    EXPECT_EQ(cfg.connections[0].consumer_deliver_subject, "orders.deliver");

    EXPECT_EQ(cfg.connections[1].name, "telemetry");
    EXPECT_EQ(cfg.connections[1].mode, "core");
    EXPECT_FALSE(cfg.connections[1].jetstream());
    EXPECT_EQ(cfg.connections[1].subjects, (std::vector<std::string>{"telemetry.in"}));
    EXPECT_EQ(cfg.connections[1].queue_group, "telemetry-workers");
    EXPECT_TRUE(cfg.connections[1].columnar);
    EXPECT_FALSE(cfg.connections[0].columnar);  // defaults false when omitted
}

TEST(config_loading, legacy_input_columnar_yaml_key_sets_flat_field) {
    temp_yaml_file file(R"(
input_subjects: [sensor.data]
input_columnar: true
attributes:
  - name: temperature
    type: float
)");
    auto cfg = sidecar::load_config(file.path());
    EXPECT_TRUE(cfg.input_columnar);
    EXPECT_TRUE(cfg.effective_connections().front().columnar);
}

TEST(config_loading, connections_and_input_columnar_both_present_throws) {
    temp_yaml_file file(R"(
connections:
  - name: a
    subjects: [a.in]
input_columnar: true
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, connections_combined_with_legacy_input_subjects_throws) {
    temp_yaml_file file(R"(
connections:
  - name: a
    mode: core
    subjects: [a.in]
input_subjects: [legacy.in]
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, connection_missing_name_throws) {
    temp_yaml_file file(R"(
connections:
  - mode: core
    subjects: [a.in]
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, connection_invalid_mode_throws) {
    temp_yaml_file file(R"(
connections:
  - name: a
    mode: not-a-real-mode
    subjects: [a.in]
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, connection_missing_subjects_throws) {
    temp_yaml_file file(R"(
connections:
  - name: a
    mode: core
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, duplicate_connection_names_throw) {
    temp_yaml_file file(R"(
connections:
  - name: a
    mode: core
    subjects: [a.in]
  - name: a
    mode: core
    subjects: [b.in]
output_prefix: matched
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, output_prefix_defaults_across_connections_when_exactly_one_subject_total) {
    temp_yaml_file file(R"(
connections:
  - name: a
    mode: core
    subjects: [only.subject]
attributes:
  - name: temperature
    type: float
)");
    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.output_prefix, "only.subject");
}
