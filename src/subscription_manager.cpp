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
    std::shared_ptr<spdlog::logger> log)
    : m_tree(build_tree(attributes)), m_log(std::move(log))
{}

uint64_t subscription_manager::subscribe(const std::string& expression,
                                         const std::string& client_id) {
    // Check if expression already exists
    auto it = m_expr_to_id.find(expression);
    if (it != m_expr_to_id.end()) {
        auto& sub = m_subscriptions[it->second];
        sub.lease_holders.insert(client_id);
        m_log->info("Reused subscription {} for expression '{}', client '{}'",
                   it->second, expression, client_id);
        return it->second;
    }

    // New expression - assign ID and insert into A-Tree
    uint64_t id = m_next_id++;
    m_tree.insert(id, expression);  // throws atree::Error if invalid

    subscription_info info;
    info.id = id;
    info.expression = expression;
    info.lease_holders.insert(client_id);

    m_subscriptions[id] = std::move(info);
    m_expr_to_id[expression] = id;

    m_log->info("New subscription {} for expression '{}', client '{}'",
               id, expression, client_id);
    return id;
}

bool subscription_manager::remove_lease(uint64_t subscription_id,
                                        const std::string& client_id) {
    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return false;

    it->second.lease_holders.erase(client_id);

    if (it->second.lease_holders.empty()) {
        // No more clients interested — remove from A-Tree
        m_tree.delete_subscription(subscription_id);
        m_expr_to_id.erase(it->second.expression);
        m_log->info("Removed subscription {} (expression '{}') - no active leases",
                   subscription_id, it->second.expression);
        m_subscriptions.erase(it);
        return true;
    }

    m_log->debug("Removed lease for client '{}' on subscription {}, {} leases remain",
                client_id, subscription_id, it->second.lease_holders.size());
    return false;
}

bool subscription_manager::remove_subscription(uint64_t subscription_id) {
    auto it = m_subscriptions.find(subscription_id);
    if (it == m_subscriptions.end()) return false;

    m_tree.delete_subscription(subscription_id);
    m_expr_to_id.erase(it->second.expression);
    m_log->info("Force-removed subscription {} (expression '{}')",
               subscription_id, it->second.expression);
    m_subscriptions.erase(it);
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

std::size_t subscription_manager::active_count() const {
    return m_subscriptions.size();
}

} // namespace sidecar
