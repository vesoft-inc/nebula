# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name keyutils)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://people.redhat.com/dhowells/keyutils/keyutils-1.6.tar.bz2
    URL_HASH MD5=191987b0ab46bb5b50efd70a6e6ce808
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make BINDIR=/bin SBINDIR=/sbin
                         SHAREDIR=/share MANDIR=/man
                         INCLUDEDIR=/include LIBDIR=/lib
                         CFLAGS=-fPIC NO_SOLIB=1 DESTDIR=${CMAKE_INSTALL_PREFIX}
                         install
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
