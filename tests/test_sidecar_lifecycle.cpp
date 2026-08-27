#include "sidecar.hpp"
#include "fake_connection.hpp"
#include "asio_test_helpers.hpp"
#include <gtest/gtest.h>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <nlohmann/json.hpp>
#include <zerialize/zerialize.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/protocols/msgpack.hpp>
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

nats_asio::message json_message(const nlohmann::json& body) {
    nats_asio::message msg;
    auto s = body.dump();
    msg.payload.assign(s.begin(), s.end());
    return msg;
}

// sample_config()'s one attribute ("temperature", float) is msgpack-encoded
// (cfg.format defaults to binary_format::msgpack) - a JSON-text payload
// (json_payload above) is invalid msgpack and would hit the
// malformed-payload/term() path instead of legitimately matching or not
// matching, which isn't what a "message reaches worker_pool" test wants.
std::vector<char> msgpack_temperature_payload(double value) {
    auto buf = zerialize::serialize<zerialize::MsgPack>(
        zerialize::dyn::map({{"temperature", value}}));
    return std::vector<char>(reinterpret_cast<const char*>(buf.data()),
                             reinterpret_cast<const char*>(buf.data()) + buf.size());
}

sidecar::config jetstream_config() {
    sidecar::config cfg = sample_config();
    cfg.input_stream = "sensor-input";
    cfg.consumer_durable_name = "sensor-durable";
    cfg.consumer_deliver_subject = "sensor.deliver";
    cfg.consumer_deliver_group = "sensor-group";
    cfg.consumer_max_ack_pending = 500;
    cfg.consumer_ack_wait_seconds = 15;
    return cfg;
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
        engine.m_registry = std::make_unique<sidecar::subscription_registry>(
            conn, engine.m_cfg.registry_bucket, engine.m_log);
        engine.m_worker_pool = std::make_unique<sidecar::worker_pool>(
            engine.m_ioc, engine.m_cfg, engine.m_schema, engine.m_sub_mgr, conn, engine.m_log);
        engine.m_worker_pool->start();
    }

    // Tests exercise the single (legacy-synthesized) connection, so this
    // resolves it once via effective_connections() rather than requiring
    // every call site to build an input_connection by hand.
    static sidecar::input_connection only_connection(const sidecar::sidecar_engine& engine) {
        return engine.m_cfg.effective_connections().front();
    }

    static asio::awaitable<bool> subscribe_to_inputs(sidecar::sidecar_engine& engine,
                                                       sidecar::input_connection conn) {
        return engine.subscribe_to_inputs(std::move(conn));
    }

    static asio::awaitable<bool> subscribe_to_inputs(sidecar::sidecar_engine& engine) {
        return subscribe_to_inputs(engine, only_connection(engine));
    }

    // Same as inject_dependencies above, but for JetStream-consumer-mode
    // tests: wires m_input_js_subs in and constructs worker_pool, so
    // worker_pool's own ack/term resolution logic is reachable through the
    // engine exactly as it would be in JetStream-consumer mode.
    static void inject_dependencies_js(sidecar::sidecar_engine& engine,
                                        nats_asio::iconnection_sptr conn,
                                        nats_asio::ijs_subscription_sptr js_sub) {
        engine.m_conn = conn;
        engine.m_input_js_subs.push_back(js_sub);
        engine.m_lease_mgr = std::make_unique<sidecar::lease_manager>(
            engine.m_ioc, conn, engine.m_sub_mgr, engine.m_cfg.lease_bucket,
            engine.m_cfg.lease_ttl_seconds, engine.m_cfg.lease_check_interval_seconds,
            engine.m_log);
        engine.m_registry = std::make_unique<sidecar::subscription_registry>(
            conn, engine.m_cfg.registry_bucket, engine.m_log);
        engine.m_worker_pool = std::make_unique<sidecar::worker_pool>(
            engine.m_ioc, engine.m_cfg, engine.m_schema, engine.m_sub_mgr, conn,
            engine.m_log);
        engine.m_worker_pool->start();
    }

    static asio::awaitable<bool> ensure_input_stream(sidecar::sidecar_engine& engine,
                                                       sidecar::input_connection conn) {
        return engine.ensure_input_stream(std::move(conn));
    }

    static asio::awaitable<bool> ensure_input_stream(sidecar::sidecar_engine& engine) {
        return ensure_input_stream(engine, only_connection(engine));
    }

    // Returns the new js_sub directly (rather than pushing into
    // m_input_js_subs itself) so a multi-connection test can hold onto each
    // connection's own handle distinctly.
    static asio::awaitable<nats_asio::ijs_subscription_sptr> subscribe_to_inputs_jetstream(
        sidecar::sidecar_engine& engine, sidecar::input_connection conn) {
        return engine.subscribe_to_inputs_jetstream(std::move(conn));
    }

    static asio::awaitable<bool> subscribe_to_inputs_jetstream(sidecar::sidecar_engine& engine) {
        auto js_sub = co_await subscribe_to_inputs_jetstream(engine, only_connection(engine));
        if (js_sub) engine.m_input_js_subs.push_back(js_sub);
        co_return js_sub != nullptr;
    }

    static asio::awaitable<void> on_js_data_message(sidecar::sidecar_engine& engine,
                                                      nats_asio::ijs_subscription_sptr js_sub,
                                                      const nats_asio::js_message& msg,
                                                      bool columnar = false) {
        return engine.on_js_data_message(js_sub, msg, columnar);
    }

    static nats_asio::ijs_subscription_sptr input_js_sub(const sidecar::sidecar_engine& engine) {
        return engine.m_input_js_subs.empty() ? nullptr : engine.m_input_js_subs.front();
    }

    static sidecar::subscription_manager& sub_mgr(sidecar::sidecar_engine& engine) {
        return engine.m_sub_mgr;
    }

    static sidecar::worker_pool::stats worker_stats(const sidecar::sidecar_engine& engine) {
        return engine.m_worker_pool->get_stats();
    }

    static asio::awaitable<void> on_data_message(sidecar::sidecar_engine& engine,
                                                  std::span<const char> payload,
                                                  bool columnar = false) {
        return engine.on_data_message("sensor.data", std::nullopt, payload, columnar);
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

// Direct successor of test_subscription_manager.cpp's now-removed
// "independent_instances_restoring_same_id_agree_on_output_topic": proves
// the SAME convergence property, but through the real registry-driven
// subscribe path instead of asserting a property of client-supplied ids
// (which no longer exist in the protocol). Two independent sidecar_engine
// objects, modeling two fleet members, share one fake connection's KV store
// (their one common point of contact, exactly like sharing one real NATS
// server) and independently receive the same expression from different
// clients with no "id" field at all - the registry alone must make them
// converge.
TEST(sidecar_engine, two_independent_instances_converge_on_same_id_via_registry) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();

    sidecar::sidecar_engine engine_a(ioc, sample_config(), make_log());
    sidecar::sidecar_engine_test_access::inject_dependencies(engine_a, conn);
    sidecar::sidecar_engine engine_b(ioc, sample_config(), make_log());
    sidecar::sidecar_engine_test_access::inject_dependencies(engine_b, conn);

    std::string reply_a;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        reply_a.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };
    auto payload_a = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine_a, std::string("_INBOX.reply_a"), std::move(payload_a))));

    std::string reply_b;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        reply_b.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };
    auto payload_b = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-2"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine_b, std::string("_INBOX.reply_b"), std::move(payload_b))));

    auto json_a = nlohmann::json::parse(reply_a);
    auto json_b = nlohmann::json::parse(reply_b);
    ASSERT_FALSE(json_a.contains("error"));
    ASSERT_FALSE(json_b.contains("error"));
    EXPECT_EQ(json_a.at("id").get<uint64_t>(), json_b.at("id").get<uint64_t>());
    EXPECT_EQ(json_a.at("topic").get<std::string>(), json_b.at("topic").get<std::string>());
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine_a).active_count(), 1u);
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine_b).active_count(), 1u);
}

// Order-independence: a third instance processes unrelated subscriptions
// first (simulating divergent per-instance history - exactly the scenario
// that made uncoordinated auto-incrementing local counters unsafe pre-
// redesign), yet still converges with the other instance once it processes
// the same expression, because the registry - not the order an instance
// happens to see things in - is what's authoritative.
TEST(sidecar_engine, instance_with_divergent_local_history_still_converges_via_registry) {
    asio::io_context ioc(1);
    auto conn = std::make_shared<sidecar_test::fake_connection>();

    sidecar::sidecar_engine engine_a(ioc, sample_config(), make_log());
    sidecar::sidecar_engine_test_access::inject_dependencies(engine_a, conn);
    sidecar::sidecar_engine engine_c(ioc, sample_config(), make_log());
    sidecar::sidecar_engine_test_access::inject_dependencies(engine_c, conn);

    std::string reply_a;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        reply_a.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };
    auto payload_a = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-1"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine_a, std::string("_INBOX.reply_a"), std::move(payload_a))));
    auto json_a = nlohmann::json::parse(reply_a);
    ASSERT_FALSE(json_a.contains("error"));

    // engine_c accrues unrelated subscriptions first - through the same
    // on_subscribe_request path real clients use (every id, even for an
    // expression only this instance has ever seen, still comes from the
    // registry - see sidecar.cpp's on_subscribe_request - so this is exactly
    // what divergent per-instance history really looks like post-redesign,
    // not a local-only shortcut).
    conn->on_publish = [](std::string_view, std::span<const char>) -> asio::awaitable<nats_asio::status> {
        co_return nats_asio::status{};
    };
    for (const char* expr : {"temperature > 10.0", "temperature > 20.0"}) {
        auto unrelated_payload = json_payload({{"expression", expr}, {"client_id", "some-other-client"}});
        ASSERT_TRUE(run_void_to_completion(
            ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                     engine_c, std::string("_INBOX.reply_unrelated"), std::move(unrelated_payload))));
    }

    std::string reply_c;
    conn->on_publish = [&](std::string_view, std::span<const char> payload) -> asio::awaitable<nats_asio::status> {
        reply_c.assign(payload.data(), payload.size());
        co_return nats_asio::status{};
    };
    auto payload_c = json_payload({{"expression", "temperature > 30.0"}, {"client_id", "client-3"}});
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_subscribe_request(
                 engine_c, std::string("_INBOX.reply_c"), std::move(payload_c))));
    auto json_c = nlohmann::json::parse(reply_c);
    ASSERT_FALSE(json_c.contains("error"));

    EXPECT_EQ(json_a.at("id").get<uint64_t>(), json_c.at("id").get<uint64_t>());
    EXPECT_EQ(json_a.at("topic").get<std::string>(), json_c.at("topic").get<std::string>());
}

TEST(sidecar_engine, on_subscribe_request_registry_failure_replies_with_error) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, sample_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);
    // A real registry failure (not a benign "already exists" race) - e.g. a
    // transient NATS error on the winning-create attempt.
    conn->fail_kv_create = [](std::string_view, std::string_view) { return true; };

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
    EXPECT_EQ(sidecar::sidecar_engine_test_access::sub_mgr(engine).active_count(), 0u);
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

// --- JetStream-consumer mode: ensure_input_stream() ---

TEST(sidecar_engine, ensure_input_stream_creates_stream_when_missing) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.sensor-input") {
            co_return std::pair{json_message({{"error", {{"code", 404}}}}), nats_asio::status{}};
        }
        if (subject == "$JS.API.STREAM.CREATE.sensor-input") {
            co_return std::pair{json_message(nlohmann::json::object()), nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::ensure_input_stream(engine)));
}

TEST(sidecar_engine, ensure_input_stream_accepts_existing_stream_with_matching_config) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.sensor-input") {
            co_return std::pair{
                json_message({{"config", {{"retention", "workqueue"},
                                          {"subjects", {"sensor.data"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::ensure_input_stream(engine)));
}

TEST(sidecar_engine, ensure_input_stream_refuses_mismatched_retention) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    // Existing stream uses "limits" retention, not "workqueue" - refusing
    // this matters because workqueue retention is what makes ack-based
    // cleanup (and the single-shared-consumer-group design) actually work;
    // silently accepting a mismatched stream would be a real correctness
    // gap, not a cosmetic one.
    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.sensor-input") {
            co_return std::pair{
                json_message({{"config", {{"retention", "limits"},
                                          {"subjects", {"sensor.data"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_FALSE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::ensure_input_stream(engine)));
}

TEST(sidecar_engine, ensure_input_stream_refuses_stream_missing_configured_subject) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    // Existing stream has the right retention policy but doesn't actually
    // capture cfg.input_subjects - messages published to "sensor.data" would
    // silently never reach this stream at all.
    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.sensor-input") {
            co_return std::pair{
                json_message({{"config", {{"retention", "workqueue"},
                                          {"subjects", {"some.other.subject"}}}}}),
                nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };

    EXPECT_FALSE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::ensure_input_stream(engine)));
}

// --- JetStream-consumer mode: subscribe_to_inputs_jetstream() ---

TEST(sidecar_engine, subscribe_to_inputs_jetstream_uses_configured_consumer_settings) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    EXPECT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs_jetstream(engine)));

    ASSERT_EQ(conn->js_subscribe_configs.size(), 1u);
    const auto& used = conn->js_subscribe_configs.front();
    EXPECT_EQ(used.stream, "sensor-input");
    ASSERT_TRUE(used.durable_name.has_value());
    EXPECT_EQ(*used.durable_name, "sensor-durable");
    ASSERT_TRUE(used.deliver_subject.has_value());
    EXPECT_EQ(*used.deliver_subject, "sensor.deliver");
    ASSERT_TRUE(used.deliver_group.has_value());
    EXPECT_EQ(*used.deliver_group, "sensor-group");
    EXPECT_EQ(used.max_ack_pending, 500u);
    EXPECT_EQ(used.ack_wait, std::chrono::seconds(15));
    EXPECT_EQ(used.ack, nats_asio::js_ack_policy::explicit_);

    EXPECT_NE(sidecar::sidecar_engine_test_access::input_js_sub(engine), nullptr);
}

TEST(sidecar_engine, subscribe_to_inputs_jetstream_fails_when_js_subscribe_fails) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);
    conn->fail_js_subscribe = [](const nats_asio::js_consumer_config&) { return true; };

    EXPECT_FALSE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs_jetstream(engine)));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::input_js_sub(engine), nullptr);
}

// --- JetStream-consumer mode: on_js_data_message() ---

TEST(sidecar_engine, on_js_data_message_enqueues_into_worker_pool) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    auto js_sub = std::make_shared<sidecar_test::fake_js_subscription>();
    sidecar::sidecar_engine_test_access::inject_dependencies_js(engine, conn, js_sub);

    nats_asio::js_message msg;
    msg.msg.payload = msgpack_temperature_payload(21.5);
    msg.stream_sequence = 42;

    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_js_data_message(engine, js_sub, msg)));

    EXPECT_TRUE(sidecar_test::drive_until(
        ioc,
        [&] { return sidecar::sidecar_engine_test_access::worker_stats(engine).processed > 0; },
        std::chrono::seconds(2)));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 1u);

    // No active subscriptions in this test - worker_pool's "no active
    // subscriptions" resolution path acks immediately (see
    // js_mode_acks_when_message_does_not_match in test_worker_pool.cpp for
    // the equivalent "legitimately no match" case).
    EXPECT_TRUE(sidecar_test::drive_until(
        ioc, [&] { return !js_sub->acked_stream_seqs.empty(); }, std::chrono::seconds(2)));
    EXPECT_EQ(js_sub->acked_stream_seqs, (std::vector<uint64_t>{42}));
}

TEST(sidecar_engine, on_js_data_message_before_worker_pool_ready_leaves_message_unacked) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto js_sub = std::make_shared<sidecar_test::fake_js_subscription>();

    nats_asio::js_message msg;
    msg.msg.payload = msgpack_temperature_payload(21.5);
    msg.stream_sequence = 7;

    // m_worker_pool is intentionally never set up here - the message must
    // be left unacked (not lost, not force-acked) so ack_wait redelivers it
    // once the pool is actually ready.
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_js_data_message(engine, js_sub, msg)));

    EXPECT_TRUE(js_sub->acked_stream_seqs.empty());
    EXPECT_TRUE(js_sub->termed_stream_seqs.empty());
}

TEST(sidecar_engine, on_js_data_message_acks_empty_payload_immediately) {
    asio::io_context ioc(1);
    sidecar::sidecar_engine engine(ioc, jetstream_config(), make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    auto js_sub = std::make_shared<sidecar_test::fake_js_subscription>();
    sidecar::sidecar_engine_test_access::inject_dependencies_js(engine, conn, js_sub);

    nats_asio::js_message msg;
    msg.stream_sequence = 5;  // msg.msg.payload left empty

    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_js_data_message(engine, js_sub, msg)));

    EXPECT_EQ(js_sub->acked_stream_seqs, (std::vector<uint64_t>{5}));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 0u)
        << "an empty payload must never reach worker_pool at all";
}

// --- Multiple connections: one js-mode, one core-mode, live together ---

TEST(sidecar_engine, mixed_js_and_core_connections_share_matching_tree_and_only_js_acks) {
    asio::io_context ioc(1);

    sidecar::input_connection orders;
    orders.name = "orders";
    orders.mode = "js";
    orders.subjects = {"orders.in"};
    orders.stream = "orders-input";
    orders.consumer_durable_name = "orders-durable";
    orders.consumer_deliver_subject = "orders.deliver";

    sidecar::input_connection telemetry;
    telemetry.name = "telemetry";
    telemetry.mode = "core";
    telemetry.subjects = {"telemetry.in"};

    auto cfg = sample_config();
    cfg.input_subjects.clear();
    cfg.connections = {orders, telemetry};

    sidecar::sidecar_engine engine(ioc, cfg, make_log());
    auto conn = std::make_shared<sidecar_test::fake_connection>();
    sidecar::sidecar_engine_test_access::inject_dependencies(engine, conn);

    conn->on_request = [](std::string_view subject, std::span<const char>, std::chrono::milliseconds)
        -> asio::awaitable<std::pair<nats_asio::message, nats_asio::status>> {
        if (subject == "$JS.API.STREAM.INFO.orders-input") {
            co_return std::pair{json_message({{"error", {{"code", 404}}}}), nats_asio::status{}};
        }
        if (subject == "$JS.API.STREAM.CREATE.orders-input") {
            co_return std::pair{json_message(nlohmann::json::object()), nats_asio::status{}};
        }
        co_return std::pair{nats_asio::message{}, nats_asio::status(nats_asio::error_code::not_found)};
    };
    conn->on_subscribe = [](std::string_view, nats_asio::on_message_cb,
                             nats_asio::subscribe_options)
        -> asio::awaitable<std::pair<nats_asio::isubscription_sptr, nats_asio::status>> {
        co_return std::pair<nats_asio::isubscription_sptr, nats_asio::status>{
            std::make_shared<sidecar_test::fake_subscription>(), nats_asio::status{}};
    };

    ASSERT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::ensure_input_stream(engine, orders)));
    auto js_sub_iface = run_to_completion<nats_asio::ijs_subscription_sptr>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs_jetstream(engine, orders));
    ASSERT_NE(js_sub_iface, nullptr);
    auto js_sub = std::static_pointer_cast<sidecar_test::fake_js_subscription>(js_sub_iface);
    ASSERT_TRUE(run_to_completion<bool>(
        ioc, sidecar::sidecar_engine_test_access::subscribe_to_inputs(engine, telemetry)));

    // One expression, active regardless of which connection a message
    // arrives on - both connections share this engine's one
    // subscription_manager/matching tree and one output_prefix.
    sidecar::sidecar_engine_test_access::sub_mgr(engine).subscribe("temperature > 10.0", "client-1");

    nats_asio::js_message js_msg;
    js_msg.msg.payload = msgpack_temperature_payload(21.5);
    js_msg.stream_sequence = 99;
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_js_data_message(engine, js_sub, js_msg)));

    auto core_payload = msgpack_temperature_payload(30.0);
    ASSERT_TRUE(run_void_to_completion(
        ioc, sidecar::sidecar_engine_test_access::on_data_message(
                 engine, std::span<const char>(core_payload.data(), core_payload.size()))));

    EXPECT_TRUE(sidecar_test::drive_until(
        ioc,
        [&] { return sidecar::sidecar_engine_test_access::worker_stats(engine).processed >= 2; },
        std::chrono::seconds(2)));
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).processed, 2u);
    EXPECT_EQ(sidecar::sidecar_engine_test_access::worker_stats(engine).matched, 2u)
        << "both connections' messages matched the same active subscription";

    // Only the JetStream-mode message has anything to ack; the core-mode
    // message has no ack concept at all (matches today's at-most-once
    // behavior) and js_sub must never see it.
    EXPECT_TRUE(sidecar_test::drive_until(
        ioc, [&] { return !js_sub->acked_stream_seqs.empty(); }, std::chrono::seconds(2)));
    EXPECT_EQ(js_sub->acked_stream_seqs, (std::vector<uint64_t>{99}));
}
