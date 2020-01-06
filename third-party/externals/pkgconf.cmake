# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

ExternalProject_Add(
    pkgconf
    URL https://github.com/pkgconf/pkgconf/archive/pkgconf-1.6.1.tar.gz
    URL_HASH MD5=ba6bda0fca2010a05de3ccda3fcd5b50
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/pkgconf
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/pkgconf/source
    CMAKE_ARGS ${common_cmake_args}
    BUILD_IN_SOURCE 1
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    INSTALL_COMMAND make -s -j${BUILDING_JOBS_NUM} install
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

