# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name rocksdb)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(ROCKSDB_CXX_FLAGS "-Wno-pessimizing-move -Wno-redundant-move -Wno-deprecated-copy -Wno-error=shadow -Wno-error=sign-compare")
ExternalProject_Add(
    ${name}
    URL https://github.com/facebook/rocksdb/archive/v5.15.10.tar.gz
    URL_HASH MD5=5b1c1fa7ff4756218514205238d8900d
    DOWNLOAD_NAME rocksdb-5.15.10.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    UPDATE_COMMAND ""
    CMAKE_ARGS
        ${common_cmake_args}
        -DPORTABLE=ON
        -DWITH_SNAPPY=ON
        -DWITH_ZSTD=ON
        -DWITH_ZLIB=ON
        -DWITH_JEMALLOC=OFF
        -DWITH_GFLAGS=OFF
        -DWITH_TESTS=OFF
        -DWITH_TOOLS=OFF
        -DUSE_RTTI=ON
        -DFAIL_ON_WARNINGS=OFF
        -DCMAKE_BUILD_TYPE=Release
#-DCMAKE_CXX_FLAGS:STRING=${ROCKSDB_CXX_FLAGS}
    PATCH_COMMAND patch CMakeLists.txt ${CMAKE_SOURCE_DIR}/patches/rocksdb-5.15.10.patch
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM} VERBOSE=1
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES configure
    COMMAND make clean -j
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} clean)
