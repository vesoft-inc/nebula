# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name krb5)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://kerberos.org/dist/krb5/1.16/krb5-1.16.1.tar.gz
    URL_HASH MD5=848e9b80d6aaaa798e3f3df24b83c407
#URL https://kerberos.org/dist/krb5/1.16/krb5-1.16.3.tar.gz
#URL_HASH MD5=65f5f695bd78ba6a64ac786f571047f4
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        env PATH=${BUILDING_PATH}
        make -s -j${BUILDING_JOBS_NUM} -C src
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND
        env PATH=${BUILDING_PATH}
        make -s install -j${BUILDING_JOBS_NUM} -C src
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
)

ExternalProject_Add_Step(krb5 mannual-configure
    DEPENDEES download update patch configure
    DEPENDERS build install
    COMMAND
        ${common_configure_envs}
        ./configure
            ${common_configure_args}
            --enable-static
            --disable-shared
            --disable-rpath
            --disable-aesni
            --disable-thread-support
            --without-system-verto
    WORKING_DIRECTORY ${source_dir}/src
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES mannual-configure
    COMMAND
        env PATH=${BUILDING_PATH}
        make clean -j -C src
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} clean)
