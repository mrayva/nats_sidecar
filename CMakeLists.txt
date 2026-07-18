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
    GIT_TAG        36095e5c2ad5488b460d60e424f37be6404453ab
    GIT_SHALLOW    TRUE
)
# Disable nats_asio's own samples/tests - we only need the library
set(NATS_ASIO_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(NATS_ASIO_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(nats_asio)

# zerialize is used directly by the event bridge and schema generator. Newer
# nats_asio releases intentionally expose it only when their samples are built,
# so keep it as an explicit, independently pinned dependency here.
set(JSONCONS_BUILD_TESTS OFF CACHE BOOL "Disable jsoncons tests when vendored" FORCE)
FetchContent_Declare(
    zerialize
    GIT_REPOSITORY https://github.com/colinator/zerialize.git
    GIT_TAG        aedaaf2e585dbc0a7f239bca33fd2a6595ed47df
)
FetchContent_MakeAvailable(zerialize)

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
        sidecar_sanitizers
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
        tests/test_sidecar_lifecycle.cpp
        tests/test_subscription_manager.cpp
        tests/test_worker_pool.cpp
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
