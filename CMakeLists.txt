cmake_minimum_required(VERSION 3.14)
project(utree)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

option(USE_PMDK "use pmdk library" ON)
option(BUILD_SHARED_LIBS "build shared libs" ON)
if (USE_PMDK)
    add_definitions(-DUSE_PMDK)
endif ()

add_library(utree "")
target_sources(utree PUBLIC
        "multiThread/utree/utree.h"
        "multiThread/utree/utree.cc"
        "multiThread/utree/raw_key.h"
        "multiThread/utree/log.h"
        "multiThread/utree/global_log.cc"
        "multiThread/utree/global_log.h"
        "multiThread/utree/udb.cc"
        "multiThread/utree/udb.h"
        )
target_include_directories(utree PUBLIC multiThread/utree ${LIBPMEM_INCLUDE_DIRS} ${LIBPMEMOBJ_INCLUDE_DIRS})
target_link_libraries(utree ${LIBPMEM_LIBRARIES} ${LIBPMEMOBJ_LIBRARIES})

add_executable(utree_test
        "multiThread/utree/utree_test.cc"
        )
target_include_directories(utree_test PUBLIC multiThread/utree ${LIBPMEM_INCLUDE_DIRS} ${LIBPMEMOBJ_INCLUDE_DIRS})
target_link_libraries(utree_test utree ${LIBPMEM_LIBRARIES} ${LIBPMEMOBJ_LIBRARIES})