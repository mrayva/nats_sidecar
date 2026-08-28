#include "cli.hpp"
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace {

cxxopts::ParseResult parse_args(cxxopts::Options& options, std::vector<std::string> args) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>("nats_sidecar"));
    for (auto& a : args) argv.push_back(a.data());
    return options.parse(static_cast<int>(argv.size()), argv.data());
}

} // namespace

TEST(cli, apply_cli_overrides_applies_all_flags) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {
        "--address", "10.0.0.5",
        "--port", "4223",
        "--input-subject", "sensor.data",
        "--output-prefix", "sensor.filtered",
        "--queue-group", "workers",
        "--input-columnar",
        "--subscribe-subject", "custom.subscribe",
        "--unsubscribe-subject", "custom.unsubscribe",
        "--lease-bucket", "my-leases",
        "--lease-ttl", "120",
        "--lease-check-interval", "5",
        "--registry-bucket", "my-registry",
        "--workers", "4",
        "--input-queue-max-messages", "500",
        "--input-queue-max-bytes", "1048576",
        "--publish-max-inflight", "16",
        "--publish-backpressure-timeout-ms", "2500",
        "--tls-cert", "/cert.pem",
        "--tls-key", "/key.pem",
        "--tls-ca", "/ca.pem",
        "--stats-interval", "30",
        "--log-level", "warn",
        "--format", "cbor",
    });

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_FALSE(err.has_value()) << *err;

    EXPECT_EQ(cfg.nats_address, "10.0.0.5");
    EXPECT_EQ(cfg.nats_port, 4223);
    ASSERT_EQ(cfg.input_subjects.size(), 1u);
    EXPECT_EQ(cfg.input_subjects[0], "sensor.data");
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
    EXPECT_EQ(cfg.input_queue_group, "workers");
    EXPECT_TRUE(cfg.input_columnar);
    EXPECT_EQ(cfg.subscribe_subject, "custom.subscribe");
    EXPECT_EQ(cfg.unsubscribe_subject, "custom.unsubscribe");
    EXPECT_EQ(cfg.lease_bucket, "my-leases");
    EXPECT_EQ(cfg.lease_ttl_seconds, 120u);
    EXPECT_EQ(cfg.lease_check_interval_seconds, 5u);
    EXPECT_EQ(cfg.registry_bucket, "my-registry");
    EXPECT_EQ(cfg.worker_threads, 4u);
    EXPECT_EQ(cfg.input_queue_max_messages, 500u);
    EXPECT_EQ(cfg.input_queue_max_bytes, 1048576u);
    EXPECT_EQ(cfg.publish_max_inflight, 16u);
    EXPECT_EQ(cfg.publish_backpressure_timeout_ms, 2500u);
    EXPECT_EQ(cfg.tls_cert, "/cert.pem");
    EXPECT_EQ(cfg.tls_key, "/key.pem");
    EXPECT_EQ(cfg.tls_ca, "/ca.pem");
    EXPECT_EQ(cfg.stats_interval_seconds, 30);
    EXPECT_EQ(cfg.log_level, "warn");
    EXPECT_EQ(cfg.format, sidecar::binary_format::cbor);
}

TEST(cli, apply_cli_overrides_accepts_repeated_input_subject_flags) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {
        "--input-subject", "sensor.data",
        "--input-subject", "sensor.data.backup",
    });

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_FALSE(err.has_value()) << *err;
    ASSERT_EQ(cfg.input_subjects.size(), 2u);
    EXPECT_EQ(cfg.input_subjects[0], "sensor.data");
    EXPECT_EQ(cfg.input_subjects[1], "sensor.data.backup");
}

TEST(cli, apply_cli_overrides_leaves_unset_fields_at_default) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--input-subject", "sensor.data"});

    sidecar::config defaults;
    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());

    EXPECT_EQ(cfg.nats_address, defaults.nats_address);
    EXPECT_EQ(cfg.nats_port, defaults.nats_port);
    EXPECT_EQ(cfg.lease_bucket, defaults.lease_bucket);
    EXPECT_EQ(cfg.registry_bucket, defaults.registry_bucket);
    EXPECT_EQ(cfg.input_columnar, defaults.input_columnar);
    EXPECT_EQ(cfg.format, defaults.format);
}

TEST(cli, apply_cli_overrides_verbose_overrides_log_level) {
    auto options = sidecar::build_cli_options();
    // --log-level and --verbose both given; verbose is applied after and
    // must win, matching the flag's documented "shorthand" behavior.
    auto result = parse_args(options, {"--log-level", "error", "--verbose"});

    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());
    EXPECT_EQ(cfg.log_level, "debug");
}

TEST(cli, apply_cli_overrides_applies_engine) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--engine", "betree"});

    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());
    EXPECT_EQ(cfg.engine, sidecar::engine_type::betree);
}

TEST(cli, apply_cli_overrides_engine_defaults_to_atree_when_unset) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--input-subject", "sensor.data"});

    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());
    EXPECT_EQ(cfg.engine, sidecar::engine_type::atree);
}

TEST(cli, apply_cli_overrides_invalid_engine_returns_error) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--engine", "not-a-real-engine"});

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("Invalid engine"), std::string::npos);
}

TEST(cli, apply_cli_overrides_invalid_format_returns_error) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--format", "not-a-format"});

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("Invalid format"), std::string::npos);
}

TEST(cli, apply_cli_overrides_parses_attr_flags_into_attributes) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {
        "--attr", "temperature:float",
        "--attr", "location:string",
    });

    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());

    ASSERT_EQ(cfg.attributes.size(), 2u);
    EXPECT_EQ(cfg.attributes[0].name, "temperature");
    EXPECT_EQ(cfg.attributes[0].type, sidecar::attribute_type::float_val);
    EXPECT_EQ(cfg.attributes[1].name, "location");
    EXPECT_EQ(cfg.attributes[1].type, sidecar::attribute_type::string);
}

TEST(cli, apply_cli_overrides_attr_missing_colon_returns_error) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--attr", "temperature"});

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("expected name:type"), std::string::npos);
}

TEST(cli, apply_cli_overrides_attr_invalid_type_returns_error) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--attr", "temperature:not-a-type"});

    sidecar::config cfg;
    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("Invalid attribute type"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_defaults_output_prefix_to_input_subject) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
    EXPECT_EQ(cfg.output_prefix, "sensor.data");
}

TEST(cli, finalize_and_validate_config_keeps_explicit_output_prefix) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.output_prefix = "sensor.filtered";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
}

TEST(cli, finalize_and_validate_config_missing_input_subjects_errors) {
    sidecar::config cfg;
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("input subject"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_multiple_subjects_require_explicit_output_prefix) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data", "sensor.data.backup"};
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("output_prefix"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_multiple_subjects_with_explicit_output_prefix_succeeds) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data", "sensor.data.backup"};
    cfg.output_prefix = "sensor.filtered";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
}

TEST(cli, finalize_and_validate_config_missing_attributes_errors) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("attribute"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_zero_lease_ttl_errors) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};
    cfg.lease_ttl_seconds = 0;

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("greater than zero"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_zero_publish_max_inflight_errors) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};
    cfg.publish_max_inflight = 0;

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("greater than zero"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_valid_config) {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
}

TEST(cli, finalize_and_validate_config_rejects_columnar_with_bson_format) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::bson;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("columnar"), std::string::npos);
    EXPECT_NE(err->find("bson"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_columnar_with_non_bson_format) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::msgpack;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
}

TEST(cli, finalize_and_validate_config_rejects_arrow_with_non_columnar_connection) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = false;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::arrow;
    cfg.output_format = sidecar::binary_format::msgpack;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("columnar"), std::string::npos);
    EXPECT_NE(err->find("arrow"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_rejects_arrow_without_output_format) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::arrow;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("output_format"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_rejects_arrow_output_format) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::arrow;
    cfg.output_format = sidecar::binary_format::arrow;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("output_format"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_arrow_with_columnar_and_output_format) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::arrow;
    cfg.output_format = sidecar::binary_format::msgpack;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
}

TEST(cli, finalize_and_validate_config_rejects_mismatched_output_format_for_non_arrow) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::msgpack;
    cfg.output_format = sidecar::binary_format::cbor;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("output_format"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_matching_output_format_for_non_arrow) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"a.in"};
    a.columnar = true;
    cfg.connections = {a};
    cfg.format = sidecar::binary_format::msgpack;
    cfg.output_format = sidecar::binary_format::msgpack;
    cfg.attributes = {{"value", sidecar::attribute_type::integer}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
}

TEST(cli, effective_worker_count_uses_configured_value_when_nonzero) {
    sidecar::config cfg;
    cfg.worker_threads = 7;
    EXPECT_EQ(sidecar::effective_worker_count(cfg), 7u);
}

TEST(cli, effective_worker_count_is_at_least_one_when_auto) {
    sidecar::config cfg;
    cfg.worker_threads = 0;
    EXPECT_GE(sidecar::effective_worker_count(cfg), 1u);
}

// --- finalize_and_validate_config: multiple connections ---

TEST(cli, finalize_and_validate_config_validates_every_js_connection) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "js";
    a.subjects = {"a.in"};
    // consumer_durable_name/consumer_deliver_subject intentionally left unset
    cfg.connections = {a};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("consumer_durable_name"), std::string::npos);
    EXPECT_NE(err->find("'a'"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_duplicate_stream_across_connections_errors) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    a.stream = "SHARED";
    a.consumer_durable_name = "a-durable";
    a.consumer_deliver_subject = "a.deliver";
    sidecar::input_connection b = a;
    b.name = "b";
    b.subjects = {"b.in"};
    b.consumer_durable_name = "b-durable";
    b.consumer_deliver_subject = "b.deliver";
    cfg.connections = {a, b};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("stream 'SHARED'"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_duplicate_durable_name_across_connections_errors) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    a.stream = "STREAM_A";
    a.consumer_durable_name = "shared-durable";
    a.consumer_deliver_subject = "a.deliver";
    sidecar::input_connection b = a;
    b.name = "b";
    b.subjects = {"b.in"};
    b.stream = "STREAM_B";
    b.consumer_deliver_subject = "b.deliver";
    cfg.connections = {a, b};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("consumer_durable_name 'shared-durable'"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_duplicate_deliver_subject_across_connections_errors) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    a.stream = "STREAM_A";
    a.consumer_durable_name = "a-durable";
    a.consumer_deliver_subject = "shared.deliver";
    sidecar::input_connection b = a;
    b.name = "b";
    b.subjects = {"b.in"};
    b.stream = "STREAM_B";
    b.consumer_durable_name = "b-durable";
    cfg.connections = {a, b};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("consumer_deliver_subject 'shared.deliver'"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_overlapping_subjects_across_connections_errors) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.mode = "core";
    a.subjects = {"shared.subject"};
    sidecar::input_connection b;
    b.name = "b";
    b.mode = "core";
    b.subjects = {"shared.subject"};
    cfg.connections = {a, b};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("shared.subject"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_valid_mixed_mode_connections) {
    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "orders";
    a.mode = "js";
    a.subjects = {"orders.in"};
    a.stream = "ORDERS";
    a.consumer_durable_name = "orders-durable";
    a.consumer_deliver_subject = "orders.deliver";
    sidecar::input_connection b;
    b.name = "telemetry";
    b.mode = "core";
    b.subjects = {"telemetry.in"};
    cfg.connections = {a, b};
    cfg.output_prefix = "matched";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
}

// --- apply_cli_overrides: legacy input flags rejected alongside 'connections' ---

TEST(cli, apply_cli_overrides_rejects_input_subject_flag_when_connections_configured) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--input-subject", "sensor.data"});

    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    cfg.connections = {a};

    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("input-subject"), std::string::npos);
    EXPECT_NE(err->find("connections"), std::string::npos);
}

TEST(cli, apply_cli_overrides_rejects_input_columnar_flag_when_connections_configured) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--input-columnar"});

    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    cfg.connections = {a};

    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("input-columnar"), std::string::npos);
    EXPECT_NE(err->find("connections"), std::string::npos);
}

TEST(cli, apply_cli_overrides_allows_unrelated_flags_when_connections_configured) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--log-level", "debug"});

    sidecar::config cfg;
    sidecar::input_connection a;
    a.name = "a";
    a.subjects = {"a.in"};
    cfg.connections = {a};

    auto err = sidecar::apply_cli_overrides(cfg, result);
    ASSERT_FALSE(err.has_value()) << *err;
    EXPECT_EQ(cfg.log_level, "debug");
}
