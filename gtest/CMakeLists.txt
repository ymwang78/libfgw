cmake_minimum_required(VERSION 3.15)
project(fgw_gtest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(${CMAKE_CURRENT_SOURCE_DIR}/../../../cmake/PlatformArch.cmake)

find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    find_package(GTest REQUIRED)
endif()
find_package(Threads REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${CMAKE_CURRENT_SOURCE_DIR}/../../libzce
    ${PLATFORM_SOLUTION_DIR}/include
)
if(UNIX)
    include_directories(${PLATFORM_SOLUTION_DIR}/include/linux)
endif()

# ─── Sources the tests pull in directly from ../src ─────────────────────────
# Only the purely pure-C++ pieces need to be compiled: avoid sources that
# depend on the full libzce runtime (zce::Reactor, zce::zvm::Machine, …).
set(FGW_PURE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/zfgw_segment.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/zfgw_pack.cpp
)

set(ZCE_LIB /zdata/cxxproj/libsrc/libzce/build/libzce.a)
set(ZCE_DEPS
    ${ZCE_LIB}
    Threads::Threads
)

# ─── test_zfgw_segment ──────────────────────────────────────────────────────
add_executable(test_zfgw_segment
    test_zfgw_segment.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/zfgw_segment.cpp
)
target_compile_definitions(test_zfgw_segment PRIVATE
    USE_GTEST_MAIN __LINUX__ ZDP OBJECT_MONITOR
)
target_link_libraries(test_zfgw_segment PRIVATE
    GTest::gtest GTest::gtest_main Threads::Threads
)

# ─── test_zfgw_pack ─────────────────────────────────────────────────────────
if(EXISTS ${ZCE_LIB})
    add_executable(test_zfgw_pack
        test_zfgw_pack.cpp
        ${FGW_PURE_SOURCES}
    )
    target_compile_definitions(test_zfgw_pack PRIVATE
        USE_GTEST_MAIN __LINUX__ ZDP OBJECT_MONITOR
    )
    target_link_libraries(test_zfgw_pack PRIVATE
        ${ZCE_DEPS}
        GTest::gtest GTest::gtest_main
    )
    add_test(NAME ZfgwPackTest COMMAND test_zfgw_pack)
endif()

enable_testing()
add_test(NAME ZfgwSegmentTest COMMAND test_zfgw_segment)
