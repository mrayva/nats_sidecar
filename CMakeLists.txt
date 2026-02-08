cmake_minimum_required(VERSION 3.25)

set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_SOURCE_DIR}/triplets" CACHE STRING "")

project(nats_sidecar
    VERSION 0.1.0
    DESCRIPTION "Content-based filtering sidecar for NATS"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SIDECAR_BUILD_TESTS "Build tests" ON)

# --- vcpkg dependencies ---
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

# --- FetchContent: nats_asio (header-only, brings zerialize) ---
include(FetchContent)
FetchContent_Declare(
    nats_asio
    GIT_REPOSITORY https://github.com/mrayva/nats_asio.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)
# Disable nats_asio's own samples/tests - we only need the library
set(NATS_ASIO_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nats_asio)

# --- a-tree FFI (Rust -> C/C++) ---
include(cmake/BuildAtree.cmake)

# --- sidecar library (shared between executable and tests) ---
add_library(sidecar_lib STATIC
    src/config.cpp
    src/event_bridge.cpp
    src/schema_generator.cpp
    src/subscription_manager.cpp
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
        yaml-cpp::yaml-cpp
        spdlog::spdlog
        fmt::fmt
        asio::asio
        cxxopts::cxxopts
        Threads::Threads
)

target_compile_definitions(sidecar_lib PUBLIC FLATBUFFERS_LOCALE_INDEPENDENT=0)

# --- main executable ---
add_executable(nats_sidecar src/main.cpp)
target_link_libraries(nats_sidecar PRIVATE sidecar_lib)

set_target_properties(nats_sidecar PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# --- tests ---
if(SIDECAR_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)

    add_executable(sidecar_test
        tests/test_event_bridge.cpp
        tests/test_subscription_manager.cpp
    )
    target_link_libraries(sidecar_test
        PRIVATE
            sidecar_lib
            GTest::gtest_main
    )
    set_target_properties(sidecar_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
    )
    add_test(NAME sidecar_test COMMAND sidecar_test)
endif()
