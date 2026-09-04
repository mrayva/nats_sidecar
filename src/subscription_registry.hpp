#pragma once

#include <nats_asio/nats_asio.hpp>
#include <asio/awaitable.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <nlohmann/json_fwd.hpp>
#include <spdlog/spdlog.h>
#include <string>
#include <utility>

namespace sidecar {

// Maps boolean-expression subscriptions to a globally-agreed subscription id
// via one NATS JetStream KV bucket shared by every sidecar instance in a
// fleet, replacing the old client-supplied-id broadcast mechanism.
//
// resolve_id() uses kv_create's atomic create-fails-if-exists semantics: the
// first instance (anywhere, at any time) to register a given expression wins
// the create, and the KV revision NATS itself assigns on that create IS the
// subscription id - no separate counter or CAS-retry-loop needed. Every
// other instance/request for the same expression reads back that same
// revision via kv_get. Entries are never deleted (see the "Why registry
// entries are permanent" section of the design this implements): nothing
// else ever writes to a key after its one creation, which is exactly what
// makes a `kv_get`'s revision permanently trustworthy as the id.
class subscription_registry {
public:
    subscription_registry(nats_asio::iconnection_sptr conn, std::string bucket,
                          std::shared_ptr<spdlog::logger> log);

    // Validate/provision the KV bucket. Must be called after the NATS
    // connection is established, before resolve_id() is used.
    asio::awaitable<bool> ensure_bucket();

    // Resolve the globally-agreed id for `expression`, creating a new
    // registry entry (and thus a new id) iff no instance has ever registered
    // it before, in the lifetime of this bucket. Returns {0, failed-status}
    // on a real error, or on a hash collision (same key, different stored
    // expression - astronomically unlikely, but checked explicitly so it can
    // never silently misroute one client's filter to another's output
    // topic).
    asio::awaitable<std::pair<uint64_t, nats_asio::status>>
    resolve_id(const std::string& expression);

    // Stable 64-bit FNV-1a hash of `s`, hex-encoded (16 chars, alphanumeric only) - originally
    // built for expression->registry-key hashing (NATS subject/KV-key syntax forbids whitespace,
    // '.', '*', '>', and other characters that appear routinely in boolean expressions), now also
    // reused by sidecar.cpp's on_subscribe_request() to turn lease_key ("<id>.<client_id>", safe
    // for a KV key but NOT safe as a JetStream stream/consumer name - those must be a single
    // subject token, no '.') into a name a client can safely pass as durable_name when creating
    // its own durable JetStream consumer for the output_stream_enabled feature. Public because of
    // that second, unrelated-to-the-registry call site - not otherwise part of this class's own
    // public contract.
    static std::string registry_key(const std::string& s);

private:
    bool validate_existing_bucket(const nlohmann::json& info) const;
    asio::awaitable<bool> create_bucket(std::chrono::milliseconds timeout);

    nats_asio::iconnection_sptr m_conn;
    std::string m_bucket;
    std::shared_ptr<spdlog::logger> m_log;
};

} // namespace sidecar
