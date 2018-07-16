# Copyright 2018 The HyperPebblesDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
project(PebblesDB VERSION 1.21.0 LANGUAGES C CXX)

# This project requires C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Setup the options that CMake can take in
option(PEBBLESDB_INSTALL "Install PebblesDB's header and library" ON)
option(PEBBLESDB_BUILD_TESTS "Build PeblesDB's unit tests" ON)

# Setup the basic C++ Compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wno-variadic-macros")

message(STATUS "CMAKE_SOURCE_DIR:    " ${CMAKE_SOURCE_DIR})

set(PROJECT_SOURCE_DIR "${PROJECT_SOURCE_DIR}/src")
message(STATUS "PROJECT_SOURCE_DIR:  " ${PROJECT_SOURCE_DIR})

# need to let "port_posix.h" to include "config.h"
add_definitions(-DHAVE_CONFIG_H)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG(-fno-builtin-memcmp COMPILER_SUPPORTS_MEMCMP)
CHECK_CXX_COMPILER_FLAG(-fno-builtin-memmove COMPILER_SUPPORTS_MEMMOVE)
CHECK_CXX_COMPILER_FLAG(-Qunused-arguments COMPILER_SUPPORTS_UNUSED_ARGS)
if (COMPILER_SUPPORTS_MEMCMP)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif (COMPILER_SUPPORTS_MEMCMP)
if (COMPILER_SUPPORTS_MEMMOVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memmove")
endif (COMPILER_SUPPORTS_MEMMOVE)
if (COMPILER_SUPPORTS_UNUSED_ARGS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")
endif (COMPILER_SUPPORTS_UNUSED_ARGS)

# variable name (e.g. HAVE_ENDIAN_H) matches "port/port_posix.h"
include(CheckIncludeFile)
check_include_file("endian.h" HAVE_ENDIAN_H)
check_include_file("machine/endian.h" HAVE_MACHINE_ENDIAN_H)
check_include_file("sys/endian.h" HAVE_SYS_ENDIAN_H)
check_include_file("sys/isa_defs.h" HAVE_SYS_ISA_DEFS_H)
check_include_file("sys/types.h" HAVE_SYS_TYPES_H)
check_include_file("dlfcn.h" HAVE_DLFCN_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
check_include_file("memory.h" HAVE_MEMORY_H)
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("stdlib.h" HAVE_STDLIB_H)
check_include_file("strings.h" HAVE_STRINGS_H)
check_include_file("string.h" HAVE_STRING_H)
check_include_file("sys/param.h" HAVE_SYS_PARAM_H)
check_include_file("sys/stat.h" HAVE_SYS_STAT_H)
check_include_file("unistd.h" HAVE_UNISTD_H)


include(CheckFunctionExists)

macro(check_function_exists_in_batch name_list)
    foreach (arg ${ARGV})
        LIST(APPEND ListNames ${arg})
    endforeach ()
    foreach (name ${ListNames})
        string(TOUPPER ${name} allcap)
        string(CONCAT var_name HAVE_ ${allcap})
        check_function_exists(${name} ${var_name})
    endforeach ()
endmacro()

check_function_exists_in_batch(
        error_at_line mmap alarm clock_gettime mach_absolute_time ftruncate fsync
        fread fread_unlocked fwrite fwrite_unlocked fflush
        fflush_unlocked memmove mkdir munmap rmdir socket getpagesize)

include(CheckSymbolExists)
check_symbol_exists(fdatasync "unistd.h" HAVE_DECL_FDATASYNC)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

set(PEBBLESDB_PUBLIC_INCLUDE_DIR "include/pebblesdb")

configure_file(
        "${PROJECT_SOURCE_DIR}/config.h.in.forcmake"
        "${PROJECT_SOURCE_DIR}/config.h"
)

include_directories(
        "${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}"
)

add_library(pebblesdb "")
target_sources(pebblesdb
        PRIVATE
        "${PROJECT_SOURCE_DIR}/db/builder.cc"
        "${PROJECT_SOURCE_DIR}/db/c.cc"
        "${PROJECT_SOURCE_DIR}/db/dbformat.cc"
        "${PROJECT_SOURCE_DIR}/db/db_impl.cc"
        "${PROJECT_SOURCE_DIR}/db/db_iter.cc"
        "${PROJECT_SOURCE_DIR}/db/filename.cc"
        "${PROJECT_SOURCE_DIR}/db/log_reader.cc"
        "${PROJECT_SOURCE_DIR}/db/log_writer.cc"
        "${PROJECT_SOURCE_DIR}/db/memtable.cc"
        "${PROJECT_SOURCE_DIR}/db/murmurhash3.cc"
        "${PROJECT_SOURCE_DIR}/db/repair.cc"
        "${PROJECT_SOURCE_DIR}/db/replay_iterator.cc"
        "${PROJECT_SOURCE_DIR}/db/table_cache.cc"
        "${PROJECT_SOURCE_DIR}/db/version_edit.cc"
        "${PROJECT_SOURCE_DIR}/db/version_set.cc"
        "${PROJECT_SOURCE_DIR}/db/write_batch.cc"
        "${PROJECT_SOURCE_DIR}/table/block_builder.cc"
        "${PROJECT_SOURCE_DIR}/table/block.cc"
        "${PROJECT_SOURCE_DIR}/table/filter_block.cc"
        "${PROJECT_SOURCE_DIR}/table/format.cc"
        "${PROJECT_SOURCE_DIR}/table/iterator.cc"
        "${PROJECT_SOURCE_DIR}/table/merger.cc"
        "${PROJECT_SOURCE_DIR}/table/table_builder.cc"
        "${PROJECT_SOURCE_DIR}/table/table.cc"
        "${PROJECT_SOURCE_DIR}/table/two_level_iterator.cc"
        "${PROJECT_SOURCE_DIR}/util/arena.cc"
        "${PROJECT_SOURCE_DIR}/util/atomic.cc"
        "${PROJECT_SOURCE_DIR}/util/bloom.cc"
        "${PROJECT_SOURCE_DIR}/util/cache.cc"
        "${PROJECT_SOURCE_DIR}/util/coding.cc"
        "${PROJECT_SOURCE_DIR}/util/comparator.cc"
        "${PROJECT_SOURCE_DIR}/util/crc32c.cc"
        "${PROJECT_SOURCE_DIR}/util/env.cc"
        "${PROJECT_SOURCE_DIR}/util/env_posix.cc"
        "${PROJECT_SOURCE_DIR}/util/filter_policy.cc"
        "${PROJECT_SOURCE_DIR}/util/hash.cc"
        "${PROJECT_SOURCE_DIR}/util/histogram.cc"
        "${PROJECT_SOURCE_DIR}/util/logging.cc"
        "${PROJECT_SOURCE_DIR}/util/options.cc"
        "${PROJECT_SOURCE_DIR}/util/status.cc"
        "${PROJECT_SOURCE_DIR}/port/port_posix.cc"
        )

target_compile_definitions(pebblesdb
        PRIVATE
        # Used by port/port.h.
        LEVELDB_PLATFORM_POSIX=1
        )

if (HAVE_SNAPPY)
    target_link_libraries(pebblesdb snappy)
endif (HAVE_SNAPPY)

# Locate the pthread library
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(pebblesdb Threads::Threads)

message(STATUS "C++ flags:    " ${CMAKE_CXX_FLAGS})

if (PEBBLESDB_BUILD_TESTS)
    enable_testing()

    function(pebblesdb_test test_file)
        get_filename_component(test_target_name "${test_file}" NAME_WE)

        add_executable("${test_target_name}" "")
        target_sources("${test_target_name}"
                PRIVATE
                "${PROJECT_SOURCE_DIR}/util/testharness.cc"
                "${PROJECT_SOURCE_DIR}/util/testharness.h"
                "${PROJECT_SOURCE_DIR}/util/testutil.cc"
                "${PROJECT_SOURCE_DIR}/util/testutil.h"

                "${test_file}"
                )
        target_link_libraries("${test_target_name}" pebblesdb)
        target_compile_definitions("${test_target_name}"
                PRIVATE
                LEVELDB_PLATFORM_POSIX=1
                )

        add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
    endfunction(pebblesdb_test)

    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/autocompact_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/arena_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/bloom_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/c_test.c")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/cache_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/coding_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/corruption_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/crc32c_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/db_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/dbformat_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/util/env_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/filename_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/table/filter_block_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/log_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/table/table_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/skiplist_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/version_edit_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/version_set_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/db/write_batch_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/issues/issue178_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/issues/issue200_test.cc")
    pebblesdb_test("${PROJECT_SOURCE_DIR}/demo/installation_test.cc")
endif (PEBBLESDB_BUILD_TESTS)

if(PEBBLESDB_INSTALL)
    include(GNUInstallDirs)
    install(TARGETS pebblesdb
            EXPORT pebblesdbTargets
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            )
    install(
            FILES
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/c.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/cache.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/comparator.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/db.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/env.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/iterator.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/options.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/slice.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/replay_iterator.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/status.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/table.h"
            "${PROJECT_SOURCE_DIR}/${PEBBLESDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/pebblesdb
    )
endif(PEBBLESDB_INSTALL)

# Print out all the CMake variables
get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()