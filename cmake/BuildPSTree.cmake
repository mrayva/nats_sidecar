# BuildPSTree.cmake - Fetch pstree source and expose it as a header-only library.
#
# Provides: pstree (INTERFACE library target) with include/ (order_key.hpp,
# ps_tree.hpp, predicate.hpp, dim_sig.hpp, pst_dynamic.hpp) on its include
# path.
#
# pstree's own top-level CMakeLists.txt unconditionally registers its full
# unit test suite (8 executables) as top-level targets, same situation as
# be-tree's own CMakeLists.txt (see BuildBetree.cmake's identical comment) -
# add_subdirectory(... EXCLUDE_FROM_ALL) keeps them out of nats_sidecar's
# default build.

include(FetchContent)

FetchContent_Declare(
    pstree
    GIT_REPOSITORY https://github.com/mrayva/pstree.git
    GIT_TAG        0a85a7799f24a94e91ff2f7b791f664b6de76d5d
    GIT_SHALLOW    TRUE
    SYSTEM
)

FetchContent_GetProperties(pstree)
if(NOT pstree_POPULATED)
    FetchContent_Populate(pstree)
    add_subdirectory(${pstree_SOURCE_DIR} ${pstree_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
