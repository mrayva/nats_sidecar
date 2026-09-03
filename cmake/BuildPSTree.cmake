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

# GIT_SHALLOW deliberately OFF (unlike BuildBetree.cmake's identical-looking Declare): a shallow
# clone fetches an arbitrary commit SHA (not necessarily the tip of an advertised ref) via git's
# smart-HTTP "fetch by want SHA1" capability, which GitHub supports but not always reliably for a
# commit that has since been superseded on the branch it once tipped - reproduced 4/4 times in CI
# ("fatal: unable to read tree <sha>", cmake/BuildPSTree.cmake's own populate step) against a pin
# that fetched (and built) fine locally every time. A full (non-shallow) clone has no such
# dependency - it walks full history instead, slightly slower but reliable - and pstree is small
# enough that the extra transfer is not worth trading back for the flake.
FetchContent_Declare(
    pstree
    GIT_REPOSITORY https://github.com/mrayva/pstree.git
    GIT_TAG        3093b3fff644d2130ee4ce046ef31c1967d0aaa6
    SYSTEM
)

FetchContent_GetProperties(pstree)
if(NOT pstree_POPULATED)
    FetchContent_Populate(pstree)
    add_subdirectory(${pstree_SOURCE_DIR} ${pstree_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
