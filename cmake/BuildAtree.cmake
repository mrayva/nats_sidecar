# BuildAtree.cmake - Fetch a-tree source and build the Rust FFI via cargo
#
# Provides: atree_ffi (IMPORTED STATIC library) with include dir pointing
# at the FFI headers (atree.h, atree.hpp).

include(FetchContent)

FetchContent_Declare(
    atree
    GIT_REPOSITORY https://github.com/mrayva/a-tree.git
    GIT_TAG        61a85fec7b2fcee4fd7eef0549d2e36450387b74
    GIT_SHALLOW    TRUE
    SYSTEM
)
FetchContent_MakeAvailable(atree)

set(ATREE_SOURCE_DIR ${atree_SOURCE_DIR})
set(ATREE_FFI_DIR    ${ATREE_SOURCE_DIR}/a-tree-ffi)

# Determine cargo build output directory - only a plain "Debug" (or an
# unspecified CMAKE_BUILD_TYPE) should leave a-tree's own Rust code
# unoptimized. This used to check for the exact string "Release", which
# silently left RelWithDebInfo (and MinSizeRel) builds - including every
# `perf`-profiling build in this project - compiling a-tree via `cargo build`
# with no optimizations at all, while the rest of the C++ was fully
# optimized: a real confound when profiling where CPU time actually goes.
if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR NOT CMAKE_BUILD_TYPE)
    set(ATREE_CARGO_PROFILE debug)
    set(ATREE_CARGO_FLAGS "")
else()
    set(ATREE_CARGO_PROFILE release)
    set(ATREE_CARGO_FLAGS --release)
endif()

set(ATREE_LIB_DIR ${ATREE_FFI_DIR}/target/${ATREE_CARGO_PROFILE})
set(ATREE_STATIC_LIB ${ATREE_LIB_DIR}/liba_tree_ffi.a)

# Custom command: build the Rust FFI crate
add_custom_command(
    OUTPUT ${ATREE_STATIC_LIB}
    COMMAND cargo build ${ATREE_CARGO_FLAGS}
    WORKING_DIRECTORY ${ATREE_FFI_DIR}
    COMMENT "Building a-tree FFI via cargo (${ATREE_CARGO_PROFILE})..."
    VERBATIM
)

add_custom_target(atree_ffi_build DEPENDS ${ATREE_STATIC_LIB})

# Imported library target
add_library(atree_ffi STATIC IMPORTED GLOBAL)
set_target_properties(atree_ffi PROPERTIES
    IMPORTED_LOCATION ${ATREE_STATIC_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${ATREE_FFI_DIR}
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES ${ATREE_FFI_DIR}
)
add_dependencies(atree_ffi atree_ffi_build)

# Rust staticlib needs these system libs on Linux
target_link_libraries(atree_ffi INTERFACE dl pthread m)
