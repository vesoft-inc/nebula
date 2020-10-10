# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name lz4)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/lz4/lz4/archive/v1.9.2.tar.gz
    URL_HASH MD5=3898c56c82fb3d9455aefd48db48eaad
    DOWNLOAD_NAME lz4-1.9.2.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND
        make install -s
             MOREFLAGS=-fPIC
             "LN_S=ln -sf"
             BUILD_SHARED=no
             -j${BUILDING_JOBS_NUM}
             PREFIX=${CMAKE_INSTALL_PREFIX}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES build
    COMMAND make clean -j
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} clean)
