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
input_subject: sensor.data
attributes:
  - name: temperature
    type: float
)");

    auto cfg = sidecar::load_config(file.path());
    EXPECT_EQ(cfg.input_subject, "sensor.data");
    EXPECT_EQ(cfg.output_prefix, "sensor.data");  // defaults to input_subject
    ASSERT_EQ(cfg.attributes.size(), 1u);
    EXPECT_EQ(cfg.attributes[0].name, "temperature");
    EXPECT_EQ(cfg.attributes[0].type, sidecar::attribute_type::float_val);

    // Defaults untouched by the file
    EXPECT_EQ(cfg.nats_address, "127.0.0.1");
    EXPECT_EQ(cfg.nats_port, 4222);
    EXPECT_EQ(cfg.format, sidecar::binary_format::msgpack);
    EXPECT_EQ(cfg.lease_bucket, "sidecar-leases");
    EXPECT_EQ(cfg.lease_ttl_seconds, 3600u);
}

TEST(config_loading, explicit_output_prefix_overrides_default) {
    temp_yaml_file file(R"(
input_subject: sensor.data
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
input_subject: sensor.data
format: cbor
input_queue_group: workers
subscribe_subject: custom.subscribe
unsubscribe_subject: custom.unsubscribe
lease_bucket: my-leases
lease_ttl_seconds: 120
lease_check_interval_seconds: 5
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

TEST(config_loading, missing_input_subject_throws) {
    temp_yaml_file file(R"(
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, missing_attributes_throws) {
    temp_yaml_file file(R"(
input_subject: sensor.data
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, empty_attributes_list_throws) {
    temp_yaml_file file(R"(
input_subject: sensor.data
attributes: []
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, invalid_format_throws) {
    temp_yaml_file file(R"(
input_subject: sensor.data
format: not-a-real-format
attributes:
  - name: temperature
    type: float
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, invalid_attribute_type_throws) {
    temp_yaml_file file(R"(
input_subject: sensor.data
attributes:
  - name: temperature
    type: not-a-real-type
)");
    EXPECT_THROW(sidecar::load_config(file.path()), std::runtime_error);
}

TEST(config_loading, nonexistent_file_throws) {
    EXPECT_THROW(sidecar::load_config("/nonexistent/path/does-not-exist.yaml"), std::exception);
}
