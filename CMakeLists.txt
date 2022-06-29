cmake_minimum_required(VERSION 3.14)
project(utree)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

include_directories(${LIBPMEM_INCLUDE_DIRS} ${LIBPMEMOBJ_INCLUDE_DIRS} multiThread)
link_libraries(${LIBPMEM_LIBRARIES} ${LIBPMEMOBJ_LIBRARIES})

option(USE_PMDK "use pmdk library" ON)
option(BUILD_SHARED_LIBS "build shared libs" ON)
if (USE_PMDK)
    add_definitions(-DUSE_PMDK)
endif ()

OPTION(UTREE_BASED "use utree as index" ON)
OPTION(FASTFAIR_BASED "use fast fait as index" OFF)

add_library(utree "")
target_sources(utree PUBLIC
        "multiThread/utree/utree.h"
        "multiThread/utree/utree.cc"
        )
target_include_directories(utree PUBLIC multiThread/utree)

add_library(fastfair "")
target_sources(fastfair PUBLIC
        "multiThread/fast_fair/btree.h"
        "multiThread/fast_fair/btree.cc"
        )
target_include_directories(fastfair INTERFACE multiThread/fast_fair)

add_library(treedb "")
target_sources(treedb PUBLIC
        "multiThread/global_log.cc"
        "multiThread/tree_db.cc"
        )

if (UTREE_BASED)
    message("Link utree")
    target_compile_definitions(treedb PUBLIC -DUTREE)
    target_link_libraries(treedb utree)
endif ()
if (FASTFAIR_BASED)
    message("Link fast-fair")
    add_definitions(-DFASTFAIR)
    target_link_libraries(treedb fastfair)
endif ()


add_executable(utree_test
        "multiThread/utree_test.cc"
        )
target_include_directories(utree_test PUBLIC multiThread/utree)
if (UTREE_BASED)
    target_link_libraries(utree_test treedb utree)
endif ()
if (FASTFAIR_BASED)
    target_link_libraries(utree_test treedb fastfair)
endif ()
