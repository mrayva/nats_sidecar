cmake_minimum_required(VERSION 3.25)

set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_SOURCE_DIR}/triplets" CACHE STRING "")

project(nats_sidecar
    VERSION 0.1.0
    DESCRIPTION "Content-based filtering sidecar for NATS"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SIDECAR_BUILD_TESTS "Build tests" ON)
option(SIDECAR_BUILD_BENCHMARKS "Build matching-engine benchmarks" OFF)
set(SIDECAR_SANITIZER "none" CACHE STRING "Sanitizer: none, address, or thread")
set_property(CACHE SIDECAR_SANITIZER PROPERTY STRINGS none address thread)

add_library(sidecar_sanitizers INTERFACE)
if(SIDECAR_SANITIZER STREQUAL "address")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "AddressSanitizer requires GCC or Clang")
    endif()
    target_compile_options(sidecar_sanitizers INTERFACE
        -fsanitize=address,undefined
        -fno-sanitize=alignment
        -fno-omit-frame-pointer
    )
    target_link_options(sidecar_sanitizers INTERFACE
        -fsanitize=address,undefined
        -fno-sanitize=alignment
    )
elseif(SIDECAR_SANITIZER STREQUAL "thread")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "ThreadSanitizer requires GCC or Clang")
    endif()
    target_compile_options(sidecar_sanitizers INTERFACE
        -fsanitize=thread
        -fno-omit-frame-pointer
        -Wno-tsan
    )
    target_link_options(sidecar_sanitizers INTERFACE -fsanitize=thread)
elseif(NOT SIDECAR_SANITIZER STREQUAL "none")
    message(FATAL_ERROR
        "Invalid SIDECAR_SANITIZER='${SIDECAR_SANITIZER}'; expected none, address, or thread")
endif()

# --- vcpkg dependencies ---
# asio/cxxopts/fmt/nlohmann_json/spdlog/yaml-cpp/Threads are linked directly
# by sidecar_lib below. simdjson/OpenSSL/ZLIB/libzip/zstd are not used by our
# own code - they satisfy nats_asio's own vcpkg.json (it depends on them
# internally, e.g. OpenSSL for TLS); since nats_asio is pulled in via
# FetchContent rather than vcpkg's registry, vcpkg only resolves the
# top-level manifest, so these have to stay declared here for nats_asio's
# find_package() calls to succeed. Same reasoning covers gtl/magic-enum/
# stringzilla/concurrentqueue/gtest in vcpkg.json.
find_package(asio CONFIG REQUIRED)
find_package(cxxopts CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(libzip CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(Threads REQUIRED)

# Apache Arrow (arrow_columnar_rows.hpp) - deliberately NOT added to vcpkg.json/resolved via
# vcpkg like every dependency above. vcpkg's own arrow port is one of its heaviest/most fragile
# (large transitive dependency graph, slow to build, historical breakage) - a real cost on every
# clean CI run and first-time contributor setup for a single new input format. Provisioned as a
# system package instead (apt-get install libarrow-dev in CI - see .github/workflows/*.yml),
# exactly how pg_arrow (the Postgres extension producing this project's Arrow input) already
# gets Arrow on every box this project builds on.
#
# Found via pkg-config, NOT find_package(Arrow CONFIG) - deliberately, not for lack of trying.
# Arrow's CMake CONFIG package re-resolves its own full transitive dependency graph (re2,
# utf8proc, and more depending on which optional Arrow components the system package was built
# with) via find_package() calls of its own - and since this project sets
# CMAKE_TOOLCHAIN_FILE=vcpkg.cmake, vcpkg's toolchain intercepts EVERY find_package() call
# project-wide, not just the ones for packages actually listed in vcpkg.json, and tries to
# resolve Arrow's own dependencies as vcpkg ports too. Some of those happen to succeed by
# coincidence (re2), others fail outright (utf8proc - Arrow's own Findutf8proc.cmake module
# looks for a target named the way vcpkg's own port names it, which no plain system
# libutf8proc-dev package provides). Rather than patching one transitive dependency at a time
# with no guarantee more won't surface, pkg-config sidesteps the whole problem: `arrow.pc` is
# just a flat list of compiler/linker flags, no CMake target graph to re-resolve - exactly how
# pg_arrow.cpp's own Makefile already links against this same library on this same box
# (`pkg-config --cflags/--libs arrow`), confirmed working here too before adopting it.
find_package(PkgConfig REQUIRED)
pkg_check_modules(Arrow REQUIRED IMPORTED_TARGET arrow)

# Warnings for first-party targets only (sidecar_lib/nats_sidecar/sidecar_test).
# FetchContent'd deps are marked SYSTEM above/below so their headers don't
# trigger these when pulled into our translation units.
set(SIDECAR_WARNING_FLAGS -Wall -Wextra -Wpedantic)

# --- FetchContent: nats_asio (header-only, brings zerialize) ---
include(FetchContent)
# GIT_SHALLOW is intentionally omitted: it shallow-clones only the current
# tip of nats_asio's default branch, then tries to `git checkout` the pinned
# GIT_TAG commit out of that shallow history - this only works while the pin
# happens to equal the branch tip, and breaks ("fatal: unable to read tree")
# the moment nats_asio gains even one more commit after the pin was set,
# which happens routinely since nats_asio is actively developed. Reproduced
# locally with a from-scratch build (bypassing this repo's own _deps cache,
# which was masking the break) and confirmed this is the actual root cause
# of nats_sidecar's CI failing at Configure on every commit since this pin
# started lagging - not a CI-environment flake.
FetchContent_Declare(
    nats_asio
    GIT_REPOSITORY https://github.com/mrayva/nats_asio.git
    GIT_TAG        751c8173154c49d2b71af022d0b23a689b1e5906
    SYSTEM
)
# Disable nats_asio's own samples/tests - we only need the library
set(NATS_ASIO_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nats_asio)

# zerialize is used directly by the event bridge and schema generator. Newer
# nats_asio releases intentionally expose it only when their samples are built,
# so keep it as an explicit, independently pinned dependency here.
#
# Using mrayva/zerialize (fork of colinator/zerialize) rather than upstream,
# matching what nats_asio itself now pins: fixes OOB reads in the msgpack and
# CBOR parsers and the Flex string/key writer, null-pointer memcpy/memcmp UB
# in zera, and an exception-safety leak in json::RootSerializer. The msgpack/
# CBOR/Flex/Zera/Ion/BSON/BEVE deserializers are exactly what
# deserialize_and_match() runs on every untrusted inbound NATS payload.
set(JSONCONS_BUILD_TESTS OFF CACHE BOOL "Disable jsoncons tests when vendored" FORCE)
# BEVE is gated behind this flag upstream (glaze, requires C++23) - now that
# this project itself is C++23 (see CMAKE_CXX_STANDARD above), take it on
# deliberately rather than leaving it excluded, same call nats_asio's own
# CMakeLists.txt made for nats_tool's --format beve.
set(ZERIALIZE_ENABLE_BEVE ON CACHE BOOL "Enable BEVE (via glaze) for the sidecar's --format" FORCE)
FetchContent_Declare(
    zerialize
    GIT_REPOSITORY https://github.com/mrayva/zerialize.git
    GIT_TAG        01144c9fc5943506550aae3cfd264ebc3619ed36
    SYSTEM
)
FetchContent_MakeAvailable(zerialize)

# --- a-tree FFI (Rust -> C/C++) ---
include(cmake/BuildAtree.cmake)

# --- be-tree (C++20 core, native CMake) ---
include(cmake/BuildBetree.cmake)

# --- pstree (header-only C++20, PS-Tree/PSTDynamic) ---
include(cmake/BuildPSTree.cmake)

# --- sidecar library (shared between executable and tests) ---
add_library(sidecar_lib STATIC
    src/cli.cpp
    src/config.cpp
    src/dialect.cpp
    src/event_bridge.cpp
    src/matching_engine.cpp
    src/pstree_dialect.cpp
    src/schema_generator.cpp
    src/subscription_manager.cpp
    src/subscription_registry.cpp
    src/lease_manager.cpp
    src/worker_pool.cpp
    src/sidecar.cpp
)

target_include_directories(sidecar_lib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(sidecar_lib
    PUBLIC
        nats_asio::nats_asio
        zerialize
        atree_ffi
        betree
        pstree
        yaml-cpp::yaml-cpp
        spdlog::spdlog
        fmt::fmt
        asio::asio
        cxxopts::cxxopts
        nlohmann_json::nlohmann_json
        Threads::Threads
        PkgConfig::Arrow
        sidecar_sanitizers
)

# libarrow.so's own transitive dependency on the system libprotobuf.so.32 has an undefined
# __cxa_call_terminate@CXXABI_1.3.15 reference that genuinely resolves fine at runtime
# (confirmed via ldd - libprotobuf.so.32 finds libstdc++.so.6 with no missing symbols) but trips
# the static linker's default (Ubuntu-hardening-flag-driven) strict verification of every
# transitively-needed .so's own symbols, not just the ones this project links directly. The
# standard fix for exactly this class of false positive: tell the linker to trust the dynamic
# loader for shared libraries it wasn't asked to fully resolve.
target_link_options(sidecar_lib PUBLIC -Wl,--allow-shlib-undefined)

target_compile_definitions(sidecar_lib PUBLIC FLATBUFFERS_LOCALE_INDEPENDENT=0)
target_compile_options(sidecar_lib PRIVATE ${SIDECAR_WARNING_FLAGS})

# --- main executable ---
add_executable(nats_sidecar src/main.cpp)
target_link_libraries(nats_sidecar PRIVATE sidecar_lib)
target_compile_options(nats_sidecar PRIVATE ${SIDECAR_WARNING_FLAGS})

set_target_properties(nats_sidecar PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# --- tests ---
if(SIDECAR_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)

    add_executable(sidecar_test
        tests/test_arrow_columnar.cpp
        tests/test_cli.cpp
        tests/test_config_loading.cpp
        tests/test_dialect.cpp
        tests/test_event_bridge.cpp
        tests/test_lease_manager.cpp
        tests/test_match_timing.cpp
        tests/test_matching_engine.cpp
        tests/test_matching_engine_differential.cpp
        tests/test_schema_generator.cpp
        tests/test_sidecar_lifecycle.cpp
        tests/test_subscription_manager.cpp
        tests/test_subscription_registry.cpp
        tests/test_worker_pool.cpp
    )
    target_link_libraries(sidecar_test
        PRIVATE
            sidecar_lib
            GTest::gtest_main
    )
    target_compile_options(sidecar_test PRIVATE ${SIDECAR_WARNING_FLAGS})
    set_target_properties(sidecar_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
    )
    add_test(NAME sidecar_test COMMAND sidecar_test)
endif()

# --- benchmarks ---
if(SIDECAR_BUILD_BENCHMARKS)
    add_executable(matching_engine_bench benchmarks/matching_engine_bench.cpp)
    target_link_libraries(matching_engine_bench PRIVATE sidecar_lib)
    target_compile_options(matching_engine_bench PRIVATE ${SIDECAR_WARNING_FLAGS})
    set_target_properties(matching_engine_bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Reusable perf-profiling target (isolates matching_engine::search() cost, zero I/O) and
    # cross-engine differential checker (real expressions + real sample rows, all three engines
    # must agree) - see each .cpp's own comment for what they're for and how to run them. Both
    # have already found and helped verify fixes for two real bugs.
    add_executable(perf_search_loop benchmarks/perf_search_loop.cpp)
    target_link_libraries(perf_search_loop PRIVATE sidecar_lib)
    set_target_properties(perf_search_loop PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    add_executable(diag_engine_diff benchmarks/diag_engine_diff.cpp)
    target_link_libraries(diag_engine_diff PRIVATE sidecar_lib)
    set_target_properties(diag_engine_diff PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Isolated microbenchmark that validated the count-then-emit adaptive dispatch (see the
    # README's own "Backpressure-adaptive count-then-emit matching for pstree" section) before
    # any of it was implemented - fully self-contained (models GroupEntry locally, no sidecar_lib
    # dependency), reusable for re-checking the crossover point if the real kept/dropped ratios
    # this project measures ever shift meaningfully.
    add_executable(countemit_bench benchmarks/countemit_bench.cpp)
    target_compile_options(countemit_bench PRIVATE -O3 -march=native)
    set_target_properties(countemit_bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Demonstrates/cross-checks the list-valued-attribute blind spot (pstree has no
    # representation for list attributes at all) - see the .cpp's own comment.
    add_executable(diag_list_attr benchmarks/diag_list_attr.cpp)
    target_link_libraries(diag_list_attr PRIVATE sidecar_lib)
    set_target_properties(diag_list_attr PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Correctness bridge for the string-vs-integer attribute matching-cost experiment - see the
    # .cpp's own comment and nyse-matrix/translate_to_int_attrs.py.
    add_executable(diag_int_attrs benchmarks/diag_int_attrs.cpp)
    target_link_libraries(diag_int_attrs PRIVATE sidecar_lib)
    set_target_properties(diag_int_attrs PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )

    # Publish-independent sidecar throughput benchmark: drives the real deserialize/populate/
    # match/fan-out pipeline via worker_pool with zero real NATS I/O (tests/fake_connection.hpp),
    # zero Postgres, zero external publisher process - see the .cpp's own comment for why this
    # exists (a real fleet-trial ceiling was found to be publisher-bound, not sidecar-bound, which
    # masked a real sidecar-side fix's own measured effect). tests/ added to this one target's own
    # include path for fake_connection.hpp - a test-only header, not part of sidecar_lib's public
    # include path.
    add_executable(sidecar_pipeline_bench benchmarks/sidecar_pipeline_bench.cpp)
    target_link_libraries(sidecar_pipeline_bench PRIVATE sidecar_lib)
    target_include_directories(sidecar_pipeline_bench PRIVATE ${CMAKE_SOURCE_DIR}/tests)
    set_target_properties(sidecar_pipeline_bench PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()
