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
    GIT_TAG        9e64adce6100218388d05c8bf983389f7a381286
    SYSTEM
)
FetchContent_MakeAvailable(zerialize)

# --- a-tree FFI (Rust -> C/C++) ---
include(cmake/BuildAtree.cmake)

# --- be-tree (C++20 core, native CMake) ---
include(cmake/BuildBetree.cmake)

# --- sidecar library (shared between executable and tests) ---
add_library(sidecar_lib STATIC
    src/cli.cpp
    src/config.cpp
    src/dialect.cpp
    src/event_bridge.cpp
    src/matching_engine.cpp
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
        yaml-cpp::yaml-cpp
        spdlog::spdlog
        fmt::fmt
        asio::asio
        cxxopts::cxxopts
        nlohmann_json::nlohmann_json
        Threads::Threads
        sidecar_sanitizers
)

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
        tests/test_cli.cpp
        tests/test_config_loading.cpp
        tests/test_dialect.cpp
        tests/test_event_bridge.cpp
        tests/test_lease_manager.cpp
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
