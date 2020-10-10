# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name flex)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz
    URL_HASH MD5=2882e3179748cc9f9c23ec593d6adc8d
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ${common_configure_envs}
        ./configure ${common_configure_args}
                    --enable-static --disable-shared
    BUILD_COMMAND make -s
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s install
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
