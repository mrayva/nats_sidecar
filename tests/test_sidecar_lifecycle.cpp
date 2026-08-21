#include "sidecar.hpp"
#include "fake_connection.hpp"
#include "asio_test_helpers.hpp"
#include <gtest/gtest.h>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <nlohmann/json.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <spdlog/sinks/null_sink.h>

namespace {

using sidecar_test::run_void_to_completion;
using sidecar_test::run_to_completion;

auto make_log() {
    return std::make_shared<spdlog::logger>(
        "test", std::make_shared<spdlog::sinks::null_sink_mt>());
}

sidecar::config sample_config() {
    sidecar::config cfg;
    cfg.input_subjects = {"sensor.data"};
    cfg.output_prefix = "sensor.filtered";
    cfg.stats_interval_seconds = 3600;
    cfg.lease_bucket = "test-leases";
    cfg.lease_ttl_seconds = 60;
    cfg.lease_check_interval_seconds = 60;
    cfg.worker_threads = 1;
    cfg.attributes = {
        {"temperature", sidecar::attribute_type::float_val},
    };
    return cfg;
}

std::vector<char> json_payload(const nlohmann::json& j) {
    auto s = j.dump();
    return std::vector<char>(s.begin(), s.end());
}

} // namespace

namespace sidecar {

struct sidecar_engine_test_access {
    static void start_stats_loop(sidecar::sidecar_engine& engine) {
        engine.m_shutting_down.store(false, std::memory_order_relaxed);
        engine.m_stats_timer = std::make_unique<asio::steady_timer>(engine.m_ioc);
        asio::co_spawn(engine.m_ioc, engine.stats_loop(), asio::detached);
    }

    static bool shutting_down(const sidecar::sidecar_engine& engine) {
        return engine.m_shutting_down.load(std::memory_order_relaxed);
    }

    // Wires up a connection + lease_manager + worker_pool directly, mirroring
    // what start() does but skipping the JetStream ensure_bucket()/
    // restore_leases() handshake and the NATS subject subscriptions, so the
    // message-handling coroutines below can be unit tested against a fake
    // connection without a live server.
    static void inject_dependencies(sidecar::sidecar_engine& engine,
                                     nats_asio::iconnection_sptr conn) {
        engine.m_conn = conn;
        engine.m_lease_mgr = std::make_unique<sidecar::lease_manager>(
            engine.m_ioc, conn, engine.m_sub_mgr, engine.m_cfg.lease_bucket,
            engine.m_cfg.lease_ttl_seconds, engine.m_cfg.lease_check_interval_seconds,
            engine.m_log);
        engine.m_worker_pool = std::make_unique<sidecar::worker_pool>(
            engine.m_ioc, engine.m_cfg, engine.m_schema, engine.m_sub_mgr, conn, engine.m_log);
        engine.m_worker_pool->start();
    }

    static asio::awaitable<bool> subscribe_to_inputs(sidecar::sidecar_engine& engine) {
        return engine.subscribe_to_inputs();
    }

    static sidecar::subscription_manager& sub_mgr(sidecar::sidecar_engine& engine) {
        return engine.m_sub_mgr;
    }

    static sidecar::worker_pool::stats worker_stats(const sidecar::sidecar_engine& engine) {
        return engine.m_worker_pool->get_stats();
    }

    static asio::awaitable<void> on_data_message(sidecar::sidecar_engine& engine,
                                                  std::span<const char> payload) {
        return engine.on_data_message("sensor.data", std::nullopt, payload);
    }

    static asio::awaitable<void> on_subscribe_request(sidecar::sidecar_engine& engine,
                                                        std::optional<std::string> reply_to,
                                                        std::vector<char> payload) {
        return engine.on_subscribe_request("sidecar.subscribe", std::move(reply_to),
                                            std::move(payload));
    }

    static asio::awaitable<void> on_unsubscribe_request(sidecar::sidecar_engine& engine,
                                                          std::optional<std::string> reply_to,
                                                          std::vector<char> payload) {
        return engine.on_unsubscribe_request("sidecar.unsubscribe", std::move(reply_to),
                                              std::move(payload));
    }
};

} // namespace sidecar

TEST(sidecar_engine, stop_workers_cancels_stats_loop) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());

    sidecar::sidecar_engine_test_access::start_stats_loop(engine);

    asio::steady_timer shutdown_timer(ioc);
    shutdown_timer.expires_after(std::chrono::milliseconds(10));
    shutdown_timer.async_wait([&engine](const std::error_code& ec) {
        if (!ec) {
            engine.stop_workers();
        }
    });

    ioc.run_for(std::chrono::milliseconds(200));

    EXPECT_TRUE(ioc.stopped());
    EXPECT_TRUE(sidecar::sidecar_engine_test_access::shutting_down(engine));
}

// --- subscribe_to_inputs (multiple input subjects) ---

TEST(sidecar_engine, subscribe_to_inputs_subscribes_to_every_configured_subject) {
    asio::io_context ioc(1);
    auto cfg = sample_config();
    cfg.input_subjects = {"sensor.data", "sensor.data.backup"};
    sidecar::sidecar_engine engine(ioc, cfg, make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    conn->on_subscribe = [](std::string_view, nats_asio::on_message_cb,
                             nats_asio::subscribe_options)
        -> asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> {
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            std::make_shared<sidecar_test::fake_subscription>(), nats_asio::status{}};
    };

    bool ok = run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs(engine));
    EXPECT_TRUE(ok);
    ASSERT_EQ(conn->subscribed_subjects.size(), 2u);
    EXPECT_EQ(conn->subscribed_subjects[0], "sensor.data");
    EXPECT_EQ(conn->subscribed_subjects[1], "sensor.data.backup");
}

TEST(sidecar_engine, subscribe_to_inputs_stops_at_first_failure) {
    asio::io_context ioc(1);
    auto cfg = sample_config();
    cfg.input_subjects = {"sensor.data", "sensor.data.backup", "sensor.data.third"};
    sidecar::sidecar_engine engine(ioc, cfg, make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    conn->on_subscribe = [](std::string_view subject, nats_asio::on_message_cb,
                             nats_asio::subscribe_options)
        -> asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> {
        if (subject == "sensor.data") {
            co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
                std::make_shared<sidecar_test::fake_subscription>(), nats_asio::status{}};
        }
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            nullptr, nats_asio::status(nats_asio::error_code::operation_failed)};
    };

    bool ok = run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs(engine));
    EXPECT_FALSE(ok);
    // First subject succeeded and the second failed - the third should
    // never have been attempted.
    ASSERT_EQ(conn->subscribed_subjects.size(), 2u);
    EXPECT_EQ(conn->subscribed_subjects[0], "sensor.data");
    EXPECT_EQ(conn->subscribed_subjects[1], "sensor.data.backup");
}

TEST(sidecar_engine, messages_from_every_configured_subject_reach_the_worker_pool) {
    // Proves the same on_data_message callback (and therefore the same
    // shared subscription_manager/matching tree) is wired to every
    // configured input subject, not just the first.
    asio::io_context ioc(1);
    auto cfg = sample_config();
    cfg.input_subjects = {"sensor.data", "sensor.data.backup"};
    sidecar::sidecar_engine engine(ioc, cfg, make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    std::vector<nats_asio::on_message_cb> callbacks;
    conn->on_subscribe = [&callbacks](std::string_view, nats_asio::on_message_cb cb,
                                       nats_asio::subscribe_options)
        -> asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> {
        callbacks.push_back(std::move(cb));
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            std::make_shared<sidecar_test::fake_subscription>(), nats_asio::status{}};
    };

    ASSERT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs(engine)));
    ASSERT_EQ(callbacks.size(), 2u);

    // 0xc1 is reserved/invalid in MessagePack - exercises processing
    // without needing a matching subscription or publish work, same
    // fixture as on_data_message_enqueues_into_worker_pool above.
    std::vector<char> payload{static_cast<char>(0xc1)};
    std::span<const char> payload_span(payload.data(), payload.size());

    ASSERT_TRUE(run_void_to_completion(ioc, callbacks[0]("sensor.data", std::nullopt, payload_span)));
    ASSERT_TRUE(run_void_to_completion(ioc, callbacks[1]("sensor.data.backup", std::nullopt, payload_span)));

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (sidecar::sidecar_engine_test_access::worker_stats(engine).processed < 2 &&
           std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 2u);
}

// --- on_data_message ---

TEST(sidecar_engine, on_data_message_before_worker_pool_ready_is_a_safe_noop) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());

    std::vector<char> payload{'x'};
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_data_message(
                 engine, std::span<const char>(payload.data(), payload.size()))));
    // Reaching here without throwing/crashing is the assertion.
}

TEST(sidecar_engine, on_data_message_skips_empty_payload) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_data_message(engine, {})));

    ioc.restart();
    ioc.run_for(std::chrono::milliseconds(50));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 0u);
}

TEST(sidecar_engine, on_data_message_enqueues_into_worker_pool) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    // 0xc1 is reserved/invalid in MessagePack - exercises processing without
    // needing a matching subscription or publish work.
    std::vector<char> payload{static_cast<char>(0xc1)};
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_data_message(
                 engine, std::span<const char>(payload.data(), payload.size()))));

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (sidecar::sidecar_engine_test_access::worker_stats(engine).processed == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        ioc.restart();
        ioc.run_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 1u);
}

// --- on_subscribe_request ---

TEST(sidecar_engine, on_subscribe_request_valid_returns_subscription_details) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    std::optional<std::string> captured_subject;
    std::string captured_reply;
    conn->on_publish = [&](std::string_view subject,
                           std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_subject = std::string(subject);
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply"), std::move(payload))));

    ASSERT_TRUE(captured_subject.has_value());
    EXPECT_EQ(*captured_subject, "_INBOX.reply");

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_EQ(reply.at("id").get<uint64_t>(), 1u);
    EXPECT_EQ(reply.at("topic").get<std::string>(), "sensor.filtered.1");
    EXPECT_EQ(reply.at("lease_bucket").get<std::string>(), "test-leases");
    EXPECT_EQ(reply.at("lease_key").get<std::string>(), "1.client-1");
    EXPECT_EQ(reply.at("lease_ttl_seconds").get<uint32_t>(), 60u);
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);
}

TEST(sidecar_engine, on_subscribe_request_without_reply_to_is_ignored) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    bool publish_called = false;
    conn->on_publish = [&](std::string_view, std::span<const char>) -> asio::awaitable<nats_asio::status> {
        publish_called = true;
        co_return nats_asio::status{};
    };

    auto payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::nullopt, std::move(payload))));

    EXPECT_FALSE(publish_called);
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
}

TEST(sidecar_engine, on_subscribe_request_invalid_json_replies_with_error) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    std::vector<char> bad_payload{'n', 'o', 't', ' ', 'j', 's', 'o', 'n'};
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply"), std::move(bad_payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_TRUE(reply.contains("error"));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
}

TEST(sidecar_engine, on_subscribe_request_invalid_expression_replies_with_error) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto payload = json_payload({{"expression", "not a valid !! expr"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply"), std::move(payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    ASSERT_TRUE(reply.contains("error"));
    EXPECT_NE(reply.at("error").get<std::string>().find("Invalid expression"), std::string::npos);
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
}

TEST(sidecar_engine, on_subscribe_request_rolls_back_new_subscription_on_persist_failure) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);
    conn->fail_kv_put = [](std::string_view, std::string_view) { return true; };

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply"), std::move(payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_TRUE(reply.contains("error"));
    // A brand-new subscription must be rolled back when its lease can't be
    // persisted - otherwise it would sit in memory with no server-side lease
    // backing it, immune to TTL expiry.
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
}

TEST(sidecar_engine, on_subscribe_request_keeps_already_held_lease_on_refresh_failure) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    auto first_payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply1"), std::move(first_payload))));
    ASSERT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);

    // Now make just this lease's refresh fail.
    conn->fail_kv_put = [](std::string_view, std::string_view key) { return key == "1.client-1"; };

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto second_payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.reply2"), std::move(second_payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_TRUE(reply.contains("error"));
    // An already-held lease must NOT be torn down just because its refresh
    // failed to re-persist - the pre-existing server-side lease is untouched.
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);
}

// --- on_unsubscribe_request ---

TEST(sidecar_engine, on_unsubscribe_request_removes_lease_and_replies_removed) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    auto sub_payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.sub"), std::move(sub_payload))));
    ASSERT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto unsub_payload = json_payload({{"id", 1}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_unsubscribe_request(
                 engine, std::string("_INBOX.unsub"), std::move(unsub_payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_EQ(reply.at("id").get<uint64_t>(), 1u);
    EXPECT_TRUE(reply.at("removed").get<bool>());
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
}

TEST(sidecar_engine, on_unsubscribe_request_delete_failure_replies_with_error) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    auto sub_payload = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine, std::string("_INBOX.sub"), std::move(sub_payload))));
    ASSERT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);

    conn->fail_kv_delete = [](std::string_view, std::string_view) { return true; };

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    auto unsub_payload = json_payload({{"id", 1}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_unsubscribe_request(
                 engine, std::string("_INBOX.unsub"), std::move(unsub_payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_TRUE(reply.contains("error"));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 1u);
}

TEST(sidecar_engine, on_unsubscribe_request_invalid_json_replies_with_error) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    std::string captured_reply;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        captured_reply.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };

    std::vector<char> bad_payload{'n', 'o', 't', ' ', 'j', 's', 'o', 'n'};
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_unsubscribe_request(
                 engine, std::string("_INBOX.unsub"), std::move(bad_payload))));

    auto reply = nlohmann::json::parse(captured_reply);
    EXPECT_TRUE(reply.contains("error"));
}

TEST(sidecar_engine, on_unsubscribe_request_without_reply_to_does_not_publish) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    bool publish_called = false;
    conn->on_publish = [&](std::string_view, std::span<const char>) -> asio::awaitable<nats_asio::status> {
        publish_called = true;
        co_return nats_asio::status{};
    };

    auto payload = json_payload({{"id", 999}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_unsubscribe_request(
                 engine, std::nullopt, std::move(payload))));

    EXPECT_FALSE(publish_called);
}
