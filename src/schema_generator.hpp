#pragma once

#include "config.hpp"
#include <string>

namespace sidecar {

// Read a binary sample file, deserialize it, infer attribute types,
// and print a YAML `attributes:` block to stdout.
// Throws on file I/O or deserialization errors.
void generate_schema(const std::string& path, binary_format format);

} // namespace sidecar
