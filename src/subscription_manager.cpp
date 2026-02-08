#include "subscription_manager.hpp"

namespace sidecar {

static atree::Tree build_tree(const std::vector<attribute_def>& attributes) {
    auto builder = atree::Tree::builder();
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:      builder.with_boolean(attr.name); break;
            case attribute_type::integer:       builder.with_integer(attr.name); break;
            case attribute_type::float_val:     builder.with_float(attr.name); break;
            case attribute_type::string:        builder.with_string(attr.name); break;
            case attribute_type::string_list:   builder.with_string_list(attr.name); break;
            case attribute_type::integer_list:  builder.with_integer_list(attr.name); break;
        }
    }
    return std::move(builder).build();
}

subscription_manager::subscription_manager(
    const std::vector<attribute_def>& attributes,
    const std::string& output_prefix,
    std::shared_ptr<spdlog::logger> log)
    : m_log(std::move(log)),
      m_attributes(attributes),
      m_output_prefix(output_prefix)
{
    // Publish an initial empty snapshot
    publish_snapshot();
}

void subscription_manager::publish_snapshot() {
    // Build a fresh tree and insert all current expressions
    auto tree = std::make_shared<atree::Tree>(build_tree(m_attributes));
    for (const auto& [id, sub] : m_subscriptions) {
        tree->insert(id, sub.expression);
    }

    auto snap = std::make_shared<tree_snapshot>();
    snap->tree = std::move(tree);
    snap->active_count = m_subscriptions.size();

    for (const auto& [id, sub] : m_subscriptions) {
        snap->output_subjects[id] = m_output_prefix + "." + std::to_string(id);
    }

    std::atomic_store(&m_snapshot,
                      std::shared_ptr<const tree_snapshot>(std::move(snap)));
}

uint64_t subscription_manager::subscribe(const std::string& expression,
                                         const std::string& client_id) {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    // Check if expression already exists — lease-only change, no snapshot publish
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) {
        auto& sub = m_subscriptions[it->second];
        sub.lease_holders.insert(client_id);
        m_log->info("Reused subscription {} for expression '{}', client '{}'",
                   it->second, expression, client_id);
        return it->second;
    }

    // New expression — build tree first to validate (throws before maps are modified)
    uint64_t id = m_next_id++;

    // Temporarily add to maps to include in rebuild
    subscription_info info;
    info.id = id;
    info.expression = expression;
    info.lease_holders.insert(client_id);

    m_subscriptions[id] = std::move(info);
    m_expr_to_id[expression] = id;

    try {
        publish_snapshot();
    } catch (...) {
        // Rollback maps if tree rebuild fails (invalid expression)
        m_subscriptions.erase(id);
        m_expr_to_id.erase(expression);
        m_next_id--;
        throw;
    }

    m_log->info("New subscription {} for expression '{}', client '{}'",
               id, expression, client_id);
    return id;
}

bool subscription_manager::remove_lease(uint64_t subscription_id,
                                        const std::string& client_id) {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return false;

    it->second.lease_holders.erase(client_id);

    if (it->second.lease_holders.empty()) {
        // No more clients — remove subscription and publish new snapshot
        m_expr_to_id.erase(it->second.expression);
        m_log->info("Removed subscription {} (expression '{}') - no active leases",
                   subscription_id, it->second.expression);
        m_subscriptions.erase(it);
        publish_snapshot();
        return true;
    }

    m_log->debug("Removed lease for client '{}' on subscription {}, {} leases remain",
                client_id, subscription_id, it->second.lease_holders.size());
    return false;
}

bool subscription_manager::remove_subscription(uint64_t subscription_id) {
    std::lock_guard<std::mutex> lock(m_write_mutex);

    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return false;

    m_expr_to_id.erase(it->second.expression);
    m_log->info("Force-removed subscription {} (expression '{}')",
               subscription_id, it->second.expression);
    m_subscriptions.erase(it);
    publish_snapshot();
    return true;
}

std::optional<subscription_info> subscription_manager::get_subscription(uint64_t id) const {
    auto it = m_subscriptions.find(id);
    if (it != m_subscriptions.end()) return it->second;
    return std::nullopt;
}

std::optional<uint64_t> subscription_manager::find_by_expression(const std::string& expression) const {
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) return it->second;
    return std::nullopt;
}

std::shared_ptr<const tree_snapshot> subscription_manager::snapshot() const {
    return std::atomic_load(&m_snapshot);
}

std::size_t subscription_manager::active_count() const {
    return std::atomic_load(&m_snapshot)->active_count;
}

} // namespace sidecar
