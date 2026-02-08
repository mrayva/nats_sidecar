#pragma once

#include <atree.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace sidecar {

// Immutable snapshot of the a-tree and associated metadata.
// Shared by worker threads via shared_ptr<const tree_snapshot>.
// Workers only need the tree (for search) and precomputed output subjects.
struct tree_snapshot {
    std::shared_ptr<const atree::Tree> tree;

    // subscription_id -> precomputed output subject (e.g. "sensor.filtered.42")
    std::unordered_map<uint64_t, std::string> output_subjects;

    std::size_t active_count = 0;
};

} // namespace sidecar
