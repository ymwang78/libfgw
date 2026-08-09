cmake_minimum_required(VERSION 3.15)
project(fgw VERSION 0.1 LANGUAGES C CXX)

include(${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/PlatformArch.cmake)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ─── Build options ──────────────────────────────────────────────────────────
option(FGW_ENABLE_UTP "Enable libutp-based FgwUtpChannel" OFF)
option(FGW_BUILD_TESTS "Build libfgw unit tests" ON)

if(WIN32)
    if(DEFINED ENV{VCPKG_ROOT} AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            CACHE STRING "Vcpkg toolchain file")
    endif()
    set(VCPKG_TARGET_TRIPLET "x64-windows-static-md" CACHE STRING "Vcpkg target triplet")
    set(VCPKG_CRT_LINKAGE "dynamic" CACHE STRING "Vcpkg CRT linkage")
endif()

# ─── Include directories ────────────────────────────────────────────────────
include_directories(
    .
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../libzce
    ${CMAKE_CURRENT_SOURCE_DIR}/../libutp
    ${PLATFORM_SOLUTION_DIR}/include
)

if(WIN32)
    include_directories(
        ${PLATFORM_SOLUTION_DIR}/include/win64
    )
elseif(UNIX)
    include_directories(
        ${PLATFORM_SOLUTION_DIR}/include/linux
        /usr/include
        /usr/local/include
    )
endif()

# ─── Source list ────────────────────────────────────────────────────────────
file(GLOB LIB_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if(FGW_ENABLE_UTP)
    target_compile_definitions(${PROJECT_NAME} PRIVATE FGW_USE_LIBUTP=1)
endif()

if(WIN32)
    target_compile_definitions(${PROJECT_NAME} PRIVATE
        WIN32 NDEBUG _LIB ZDP _CRT_SECURE_NO_WARNINGS
    )
elseif(UNIX)
    target_compile_definitions(${PROJECT_NAME} PRIVATE
        __LINUX__ NDEBUG NODEBUG ZDP OBJECT_MONITOR
    )
endif()

if(MSVC)
    target_compile_options(${PROJECT_NAME} PRIVATE /W3 /wd4996)
    target_compile_options(${PROJECT_NAME} PRIVATE $<$<CONFIG:Release>:/O2 /Oi /GL>)
    set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS_RELEASE "/LTCG")
else()
    target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra -g)
endif()

set_target_properties(${PROJECT_NAME} PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    OUTPUT_NAME "zfgw"
)

# ─── Install rules ──────────────────────────────────────────────────────────
if(PLATFORM_ARCH MATCHES "win_(.*)")
    set(ARCH_NAME ${CMAKE_MATCH_1})
elseif(PLATFORM_ARCH MATCHES "linux_(.*)")
    set(ARCH_NAME ${CMAKE_MATCH_1})
else()
    set(ARCH_NAME ${PLATFORM_ARCH})
endif()

set(CMAKE_INSTALL_PREFIX "${PLATFORM_SOLUTION_DIR}/" CACHE PATH "Installation Directory" FORCE)

install(TARGETS ${PROJECT_NAME}
    LIBRARY DESTINATION ${PLATFORM_LIB_DIR}
    ARCHIVE DESTINATION ${PLATFORM_LIB_DIR}
    RUNTIME DESTINATION ${PLATFORM_BIN_DIR})

install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/zfgw.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/zfgw_proto.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/zfgw_pack.h
    DESTINATION include/zfgw)

# ─── Unit tests ─────────────────────────────────────────────────────────────
if(FGW_BUILD_TESTS AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/gtest/CMakeLists.txt)
    enable_testing()
    add_subdirectory(gtest)
endif()
