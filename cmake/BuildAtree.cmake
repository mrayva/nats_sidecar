# BuildAtree.cmake - Fetch a-tree source and build the Rust FFI via cargo
#
# Provides: atree_ffi (IMPORTED STATIC library) with include dir pointing
# at the FFI headers (atree.h, atree.hpp).

include(FetchContent)

FetchContent_Declare(
    atree
    GIT_REPOSITORY https://github.com/mrayva/a-tree.git
    GIT_TAG        bc434fb8377279b4ac8e4a1b3620ec9d1b0a85e3
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

# Build the Rust FFI crate via a custom TARGET, not an OUTPUT-tracked
# add_custom_command. A custom_command only re-runs its COMMAND when a
# declared DEPENDS file is newer than OUTPUT (${ATREE_STATIC_LIB}) - but
# there's no practical way to enumerate every source file this staticlib
# actually depends on (this crate's own src/, plus its path-dependency on
# the sibling a-tree crate's src/, which FetchContent re-populates in
# place on every pin bump - a checkout doesn't reliably bump every
# touched file's mtime relative to an already-built .a sitting in the
# same reused build directory). Caught 2026-08-27: after bumping the pin,
# `make` reported "Built target atree_ffi_build" without ever invoking
# `cargo build` on the new source, needing a manual `cargo build` in
# ${ATREE_FFI_DIR} as a workaround before the fix below.
#
# A custom TARGET's COMMAND(s) - unlike add_custom_command's OUTPUT-gated
# one - run unconditionally on every build invocation, regardless of file
# timestamps. `cargo build` is already fast when nothing changed
# (~0.3-0.8s) and does its own correct, fine-grained incremental staleness
# detection internally - the fix is to stop trying to replicate that
# inside CMake and just let cargo decide, every time.
add_custom_target(atree_ffi_build
    COMMAND cargo build ${ATREE_CARGO_FLAGS}
    WORKING_DIRECTORY ${ATREE_FFI_DIR}
    BYPRODUCTS ${ATREE_STATIC_LIB}
    COMMENT "Building a-tree FFI via cargo (${ATREE_CARGO_PROFILE})..."
    VERBATIM
)

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
