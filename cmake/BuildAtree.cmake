# BuildAtree.cmake - Fetch a-tree source and build the Rust FFI via cargo
#
# Provides: atree_ffi (IMPORTED STATIC library) with include dir pointing
# at the FFI headers (atree.h, atree.hpp).

include(FetchContent)

FetchContent_Declare(
    atree
    GIT_REPOSITORY https://github.com/mrayva/a-tree.git
    GIT_TAG        master
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(atree)

set(ATREE_SOURCE_DIR ${atree_SOURCE_DIR})
set(ATREE_FFI_DIR    ${ATREE_SOURCE_DIR}/a-tree-ffi)

# Determine cargo build output directory
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(ATREE_CARGO_PROFILE release)
    set(ATREE_CARGO_FLAGS --release)
else()
    set(ATREE_CARGO_PROFILE debug)
    set(ATREE_CARGO_FLAGS "")
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
)
add_dependencies(atree_ffi atree_ffi_build)

# Rust staticlib needs these system libs on Linux
target_link_libraries(atree_ffi INTERFACE dl pthread m)
