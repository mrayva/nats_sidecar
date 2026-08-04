#pragma once

#include "config.hpp"
#include <cxxopts.hpp>
#include <optional>
#include <string>

namespace sidecar {

// Builds the CLI option table (shared by --help output and parsing).
cxxopts::Options build_cli_options();

// Applies CLI flags present in `result` onto `cfg`, overriding any
// YAML-loaded or default values - including parsing --attr name:type pairs
// and --verbose. Returns an error message if any flag value is invalid,
// nullopt on success.
std::optional<std::string> apply_cli_overrides(config& cfg, const cxxopts::ParseResult& result);

// Fills in defaults (output_prefix defaults to input_subject) and validates
// required/nonzero fields. Returns an error message if invalid, nullopt on
// success.
std::optional<std::string> finalize_and_validate_config(config& cfg);

// Resolves the worker thread count that will actually be used:
// cfg.worker_threads if set, otherwise hardware_concurrency() clamped to at
// least 1 (mirrors worker_pool's own resolution, for startup logging).
unsigned int effective_worker_count(const config& cfg);

} // namespace sidecar
