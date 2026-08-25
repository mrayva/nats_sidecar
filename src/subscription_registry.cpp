#include "subscription_registry.hpp"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdio>
#include <string_view>
#include <vector>

namespace sidecar {

subscription_registry::subscription_registry(nats_asio::iconnection_sptr conn,
                                              std::string bucket,
                                              std::shared_ptr<spdlog::logger> log)
    : m_conn(std::move(conn)), m_bucket(std::move(bucket)), m_log(std::move(log))
{}

std::string subscription_registry::registry_key(const std::string& expression) {
    // 64-bit FNV-1a. No hash utility exists elsewhere in this repo, and
    // std::hash<std::string> is implementation-defined - not a contract
    // worth depending on for id-assignment correctness even though every
    // instance in a fleet runs the same binary.
    constexpr uint64_t fnv_offset_basis = 0xcbf29ce484222325ULL;
    constexpr uint64_t fnv_prime = 0x100000001b3ULL;
    uint64_t hash = fnv_offset_basis;
    for (unsigned char c : expression) {
        hash ^= c;
        hash *= fnv_prime;
    }
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(hash));
    return std::string(buf, 16);
}

asio::awaitable<bool> subscription_registry::ensure_bucket() {
    constexpr auto timeout = std::chrono::seconds(5);

    const std::string stream_name = "KV_" + m_bucket;
    const std::string info_subject = "$JS.API.STREAM.INFO." + stream_name;
    const std::string empty_payload = "{}";
    auto [info_reply, info_status] = co_await m_conn->request(
        info_subject, std::span<const char>(empty_payload.data(), empty_payload.size()),
        timeout);

    if (info_status.failed()) {
        m_log->error("subscription_registry: failed to inspect KV bucket '{}': {}",
                     m_bucket, info_status.error());
        co_return false;
    }

    nlohmann::json info;
    bool missing = false;
    try {
        info = nlohmann::json::parse(
            std::string_view(info_reply.payload.data(), info_reply.payload.size()));
        missing = info.contains("error") && info["error"].value("code", 0) == 404;
    } catch (const std::exception& e) {
        m_log->error("subscription_registry: invalid stream info response: {}", e.what());
        co_return false;
    }

    if (!missing) co_return validate_existing_bucket(info);
    co_return co_await create_bucket(timeout);
}

bool subscription_registry::validate_existing_bucket(const nlohmann::json& info) const {
    if (info.contains("error") || !info.contains("config")) {
        auto description = info.contains("error")
            ? info["error"].value("description", "stream info failed")
            : std::string("missing stream config");
        m_log->error("subscription_registry: failed to inspect bucket '{}': {}",
                     m_bucket, description);
        return false;
    }

    const std::string expected_subject = "$KV." + m_bucket + ".>";

    const auto& cfg = info["config"];
    const auto history = cfg.value("max_msgs_per_subject", int64_t{0});
    const auto subjects = cfg.value("subjects", std::vector<std::string>{});
    const bool subject_ok = std::find(subjects.begin(), subjects.end(),
                                      expected_subject) != subjects.end();
    // No max_age check: unlike lease_manager's TTL-bound bucket, this
    // registry has no expiry concept - entries are permanent (see
    // subscription_registry.hpp's class comment).
    if (history != 1 || !subject_ok) {
        m_log->error(
            "subscription_registry: bucket '{}' configuration mismatch "
            "(expected history=1, subject='{}')", m_bucket, expected_subject);
        return false;
    }

    m_log->info("subscription_registry: validated KV bucket '{}'", m_bucket);
    return true;
}

asio::awaitable<bool> subscription_registry::create_bucket(std::chrono::milliseconds timeout) {
    const std::string stream_name = "KV_" + m_bucket;
    const std::string expected_subject = "$KV." + m_bucket + ".>";

    nlohmann::json stream_config = {
        {"name", stream_name},
        {"subjects", {expected_subject}},
        {"retention", "limits"},
        {"storage", "file"},
        {"max_msgs", -1},
        {"max_bytes", -1},
        {"max_age", 0},
        {"max_msgs_per_subject", 1},
        {"max_msg_size", -1},
        {"discard", "old"},
        {"num_replicas", 1},
        {"allow_rollup_hdrs", true},
        // Load-bearing, not copied boilerplate: entries are permanent by
        // design (see subscription_registry.hpp), and this is a safety rail
        // against that invariant being violated later, by this code or by
        // manual ops tooling.
        {"deny_delete", true},
        {"allow_direct", true}
    };
    std::string payload_str = stream_config.dump();
    auto [create_reply, create_status] = co_await m_conn->request(
        "$JS.API.STREAM.CREATE." + stream_name,
        std::span<const char>(payload_str.data(), payload_str.size()), timeout);
    if (create_status.failed()) {
        m_log->error("subscription_registry: failed to create KV bucket '{}': {}",
                     m_bucket, create_status.error());
        co_return false;
    }

    try {
        auto response = nlohmann::json::parse(
            std::string_view(create_reply.payload.data(), create_reply.payload.size()));
        if (response.contains("error")) {
            m_log->error("subscription_registry: failed to create KV bucket '{}': {}",
                         m_bucket, response["error"].value("description", "unknown error"));
            co_return false;
        }
    } catch (const std::exception& e) {
        m_log->error("subscription_registry: invalid bucket creation response: {}", e.what());
        co_return false;
    }

    m_log->info("subscription_registry: created KV bucket '{}'", m_bucket);
    co_return true;
}

asio::awaitable<std::pair<uint64_t, nats_asio::status>> subscription_registry::resolve_id(
    const std::string& expression) {
    constexpr auto timeout = std::chrono::seconds(5);
    const std::string key = registry_key(expression);
    nlohmann::json value = {{"expression", expression}};
    const std::string value_str = value.dump();

    auto [revision, create_status] = co_await m_conn->kv_create(
        m_bucket, key, std::span<const char>(value_str.data(), value_str.size()), timeout);
    if (!create_status.failed()) {
        if (revision == 0) {
            m_log->error("subscription_registry: created entry for '{}' without a revision",
                         expression);
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::operation_failed)};
        }
        m_log->debug("subscription_registry: registered new expression '{}' as id {}",
                     expression, revision);
        co_return std::pair<uint64_t, nats_asio::status>{revision, nats_asio::status{}};
    }

    if (create_status.code() != nats_asio::error_code::already_exists) {
        m_log->error("subscription_registry: failed to register expression '{}': {}",
                     expression, create_status.error());
        co_return std::pair<uint64_t, nats_asio::status>{0, create_status};
    }

    // Someone else already registered this expression - adopt their id.
    // Nothing ever writes to this key again after its one creation (entries
    // are permanent), so its revision is still exactly the winning create's
    // revision.
    auto [entry, get_status] = co_await m_conn->kv_get(m_bucket, key, timeout);
    if (get_status.failed()) {
        m_log->error("subscription_registry: failed to read existing entry for '{}': {}",
                     expression, get_status.error());
        co_return std::pair<uint64_t, nats_asio::status>{0, get_status};
    }

    try {
        auto existing = nlohmann::json::parse(
            std::string_view(entry.value.data(), entry.value.size()));
        const auto existing_expression = existing.at("expression").get<std::string>();
        if (existing_expression != expression) {
            // Astronomically unlikely at 64 bits for realistic subscription
            // counts, but must be a hard error, never a silent misroute of
            // one client's filter to another's output topic.
            m_log->error(
                "subscription_registry: hash collision on key '{}' - '{}' vs existing '{}'",
                key, expression, existing_expression);
            co_return std::pair<uint64_t, nats_asio::status>{
                0, nats_asio::status(nats_asio::error_code::operation_failed)};
        }
    } catch (const std::exception& e) {
        m_log->error("subscription_registry: invalid existing entry for '{}': {}",
                     expression, e.what());
        co_return std::pair<uint64_t, nats_asio::status>{
            0, nats_asio::status(nats_asio::error_code::operation_failed)};
    }

    m_log->debug("subscription_registry: adopted existing id {} for expression '{}'",
                 entry.revision, expression);
    co_return std::pair<uint64_t, nats_asio::status>{entry.revision, nats_asio::status{}};
}

} // namespace sidecar
