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
        "--subscribe-subject", "custom.subscribe",
        "--unsubscribe-subject", "custom.unsubscribe",
        "--lease-bucket", "my-leases",
        "--lease-ttl", "120",
        "--lease-check-interval", "5",
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
    EXPECT_EQ(cfg.input_subject, "sensor.data");
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
    EXPECT_EQ(cfg.input_queue_group, "workers");
    EXPECT_EQ(cfg.subscribe_subject, "custom.subscribe");
    EXPECT_EQ(cfg.unsubscribe_subject, "custom.unsubscribe");
    EXPECT_EQ(cfg.lease_bucket, "my-leases");
    EXPECT_EQ(cfg.lease_ttl_seconds, 120u);
    EXPECT_EQ(cfg.lease_check_interval_seconds, 5u);
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

TEST(cli, apply_cli_overrides_leaves_unset_fields_at_default) {
    auto options = sidecar::build_cli_options();
    auto result = parse_args(options, {"--input-subject", "sensor.data"});

    sidecar::config defaults;
    sidecar::config cfg;
    ASSERT_FALSE(sidecar::apply_cli_overrides(cfg, result).has_value());

    EXPECT_EQ(cfg.nats_address, defaults.nats_address);
    EXPECT_EQ(cfg.nats_port, defaults.nats_port);
    EXPECT_EQ(cfg.lease_bucket, defaults.lease_bucket);
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
    cfg.input_subject = "sensor.data";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
    EXPECT_EQ(cfg.output_prefix, "sensor.data");
}

TEST(cli, finalize_and_validate_config_keeps_explicit_output_prefix) {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";
    cfg.output_prefix = "sensor.filtered";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    EXPECT_FALSE(sidecar::finalize_and_validate_config(cfg).has_value());
    EXPECT_EQ(cfg.output_prefix, "sensor.filtered");
}

TEST(cli, finalize_and_validate_config_missing_input_subject_errors) {
    sidecar::config cfg;
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("input_subject"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_missing_attributes_errors) {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("attribute"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_zero_lease_ttl_errors) {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};
    cfg.lease_ttl_seconds = 0;

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("greater than zero"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_zero_publish_max_inflight_errors) {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};
    cfg.publish_max_inflight = 0;

    auto err = sidecar::finalize_and_validate_config(cfg);
    ASSERT_TRUE(err.has_value());
    EXPECT_NE(err->find("greater than zero"), std::string::npos);
}

TEST(cli, finalize_and_validate_config_accepts_valid_config) {
    sidecar::config cfg;
    cfg.input_subject = "sensor.data";
    cfg.attributes = {{"temperature", sidecar::attribute_type::float_val}};

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
