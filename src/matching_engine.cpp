#include "matching_engine.hpp"
#include "dialect.hpp"
#include <atree.hpp>
#include <betree_cpp.hpp>
#include <unordered_map>

namespace sidecar {

// Generous, fixed bounds for be-tree's domain-partitioned numeric/string
// schema. a-tree's own schema carries no such bounds, so attribute_def has
// nothing narrower to offer - wide enough to cover any real attribute value,
// at the cost of forgoing partitioning benefit a tighter bound would give.
namespace {
constexpr int64_t kBetreeIntMin = -1'000'000'000LL;
constexpr int64_t kBetreeIntMax = 1'000'000'000LL;
constexpr double kBetreeFloatMin = -1e9;
constexpr double kBetreeFloatMax = 1e9;
constexpr std::size_t kBetreeStringCount = 1024;
}

namespace {

atree::Tree build_atree(const std::vector<attribute_def>& attributes) {
    auto builder = atree::Tree::builder();
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:      builder.with_boolean(attr.name); break;
            case attribute_type::integer:      builder.with_integer(attr.name); break;
            case attribute_type::float_val:    builder.with_float(attr.name); break;
            case attribute_type::string:       builder.with_string(attr.name); break;
            case attribute_type::string_list:  builder.with_string_list(attr.name); break;
            case attribute_type::integer_list: builder.with_integer_list(attr.name); break;
        }
    }
    return std::move(builder).build();
}

class atree_event_sink : public event_sink {
public:
    explicit atree_event_sink(atree::EventBuilder builder) : m_builder(std::move(builder)) {}

    void with_boolean(const std::string& name, bool value) override {
        m_builder.with_boolean(name, value);
    }
    void with_integer(const std::string& name, int64_t value) override {
        m_builder.with_integer(name, value);
    }
    void with_float(const std::string& name, double value) override {
        m_builder.with_float(name, value);
    }
    void with_string(const std::string& name, std::string_view value) override {
        m_builder.with_string(name, value);
    }
    void with_string_list(const std::string& name, const std::vector<std::string>& values) override {
        m_builder.with_string_list(name, values);
    }
    void with_integer_list(const std::string& name, const std::vector<int64_t>& values) override {
        m_builder.with_integer_list(name, values);
    }
    void with_undefined(const std::string& name) override {
        m_builder.with_undefined(name);
    }

    atree::EventBuilder& native() { return m_builder; }

private:
    atree::EventBuilder m_builder;
};

class atree_matching_engine : public matching_engine {
public:
    explicit atree_matching_engine(atree::Tree tree) : m_tree(std::move(tree)) {}

    void insert(uint64_t id, const std::string& expression) override {
        try {
            m_tree.insert(id, translate_to_atree_dialect(expression));
        } catch (const atree::Error& e) {
            throw matching_engine_error(e.what());
        }
    }

    std::unique_ptr<event_sink> make_event() const override {
        return std::make_unique<atree_event_sink>(m_tree.make_event());
    }

    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<atree_event_sink&>(event);
        try {
            return m_tree.search(sink.native());
        } catch (const atree::Error& e) {
            throw matching_engine_error(e.what());
        }
    }

private:
    atree::Tree m_tree;
};

// be::Event's set_* methods are index-based, not name-based (unlike
// atree::EventBuilder) - indices are assigned in schema-declaration order
// and looked up by name here to keep event_sink's interface uniform.
be::Tree build_betree(const std::vector<attribute_def>& attributes,
                       std::unordered_map<std::string, std::size_t>& indices_out)
{
    be::Tree tree;
    std::size_t idx = 0;
    for (const auto& attr : attributes) {
        switch (attr.type) {
            case attribute_type::boolean:
                tree.add_boolean(attr.name, true);
                break;
            case attribute_type::integer:
                tree.add_integer(attr.name, true, kBetreeIntMin, kBetreeIntMax);
                break;
            case attribute_type::float_val:
                tree.add_float(attr.name, true, kBetreeFloatMin, kBetreeFloatMax);
                break;
            case attribute_type::string:
                tree.add_string(attr.name, true, kBetreeStringCount);
                break;
            case attribute_type::string_list:
                tree.add_string_list(attr.name, true, kBetreeStringCount);
                break;
            case attribute_type::integer_list:
                tree.add_integer_list(attr.name, true, kBetreeIntMin, kBetreeIntMax);
                break;
        }
        indices_out[attr.name] = idx++;
    }
    return tree;
}

class betree_event_sink : public event_sink {
public:
    betree_event_sink(be::Event event, const std::unordered_map<std::string, std::size_t>& indices)
        : m_event(std::move(event)), m_indices(indices) {}

    void with_boolean(const std::string& name, bool value) override {
        m_event.set_boolean(index_for(name), value);
    }
    void with_integer(const std::string& name, int64_t value) override {
        m_event.set_integer(index_for(name), value);
    }
    void with_float(const std::string& name, double value) override {
        m_event.set_float(index_for(name), value);
    }
    void with_string(const std::string& name, std::string_view value) override {
        m_event.set_string(index_for(name), value);
    }
    void with_string_list(const std::string& name, const std::vector<std::string>& values) override {
        std::vector<std::string_view> views(values.begin(), values.end());
        m_event.set_string_list(index_for(name), views);
    }
    void with_integer_list(const std::string& name, const std::vector<int64_t>& values) override {
        m_event.set_integer_list(index_for(name), values);
    }
    void with_undefined(const std::string& name) override {
        m_event.clear(index_for(name));
    }

    be::Event& native() { return m_event; }

private:
    std::size_t index_for(const std::string& name) const {
        auto it = m_indices.find(name);
        if (it == m_indices.end()) {
            throw matching_engine_error("unknown attribute: " + name);
        }
        return it->second;
    }

    be::Event m_event;
    const std::unordered_map<std::string, std::size_t>& m_indices;
};

class betree_matching_engine : public matching_engine {
public:
    betree_matching_engine(be::Tree tree, std::unordered_map<std::string, std::size_t> indices)
        : m_tree(std::move(tree)), m_indices(std::move(indices)) {}

    void insert(uint64_t id, const std::string& expression) override {
        std::string rewritten;
        try {
            rewritten = translate_to_betree_dialect(expression);
        } catch (const std::exception& e) {
            throw matching_engine_error(e.what());
        }
        if (!m_tree.insert(id, rewritten)) {
            throw matching_engine_error("invalid expression: " + expression);
        }
    }

    std::unique_ptr<event_sink> make_event() const override {
        return std::make_unique<betree_event_sink>(m_tree.make_event(), m_indices);
    }

    std::vector<uint64_t> search(event_sink& event) const override {
        auto& sink = static_cast<betree_event_sink&>(event);
        try {
            return m_tree.search(sink.native()).matched_subs;
        } catch (const std::exception& e) {
            throw matching_engine_error(e.what());
        }
    }

private:
    be::Tree m_tree;
    std::unordered_map<std::string, std::size_t> m_indices;
};

} // namespace

std::unique_ptr<matching_engine> build_matching_engine(
    engine_type type, const std::vector<attribute_def>& attributes)
{
    switch (type) {
        case engine_type::atree:
            return std::make_unique<atree_matching_engine>(build_atree(attributes));
        case engine_type::betree: {
            std::unordered_map<std::string, std::size_t> indices;
            auto tree = build_betree(attributes, indices);
            return std::make_unique<betree_matching_engine>(std::move(tree), std::move(indices));
        }
    }
    throw matching_engine_error("unknown engine type");
}

} // namespace sidecar
