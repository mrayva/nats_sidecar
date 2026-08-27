# BuildBetree.cmake - Fetch be-tree source and build it as a library.
#
# Provides: betree (STATIC library target) with both betree.h (C API,
# src/) and betree_cpp.hpp (C++ wrapper, include/) on its include path.
#
# be-tree's own top-level CMakeLists.txt unconditionally registers its full
# unit test / benchmark suite (~24 executables) as top-level targets - there
# is no BUILD_TESTS-style option to gate them. add_subdirectory(...
# EXCLUDE_FROM_ALL) keeps them out of nats_sidecar's default build; only the
# `betree` library target is actually built, since that's the only one
# anything here depends on.

include(FetchContent)

FetchContent_Declare(
    betree
    GIT_REPOSITORY https://github.com/mrayva/be-tree.git
    GIT_TAG        bebd8b62245cef4d2d06309eff54758350a8b2c1
    GIT_SHALLOW    TRUE
    SYSTEM
)

FetchContent_GetProperties(betree)
if(NOT betree_POPULATED)
    FetchContent_Populate(betree)
    add_subdirectory(${betree_SOURCE_DIR} ${betree_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# betree's own target only exports src/ (for betree.h) as a public include
# dir - betree_cpp.hpp lives in include/ and isn't added anywhere upstream.
# Generator-expression form matches how betree's own src/CMakeLists.txt
# already declares its src/ include dir - a bare source-tree path here is
# rejected by its install() export set (invalid once installed).
target_include_directories(betree INTERFACE
    $<BUILD_INTERFACE:${betree_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
