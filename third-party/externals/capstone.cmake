# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set(name capstone)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
ExternalProject_Add(
    ${name}
    URL https://github.com/aquynh/capstone/archive/4.0.1.tar.gz
    URL_HASH MD5=1b0a9a0d50d9515dcf7684ce0a2270a4
    DOWNLOAD_NAME capstone-4.0.1.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CMAKE_ARGS
        ${common_cmake_args}
        -DCAPSTONE_X86_SUPPORT=ON
        -DCAPSTONE_ARM_SUPPORT=OFF
        -DCAPSTONE_ARM64_SUPPORT=OFF
        -DCAPSTONE_M680X_SUPPORT=OFF
        -DCAPSTONE_M68K_SUPPORT=OFF
        -DCAPSTONE_MIPS_SUPPORT=OFF
        -DCAPSTONE_MOS65XX_SUPPORT=OFF
        -DCAPSTONE_PPC_SUPPORT=OFF
        -DCAPSTONE_SPARC_SUPPORT=OFF
        -DCAPSTONE_SYSZ_SUPPORT=OFF
        -DCAPSTONE_XCORE_SUPPORT=OFF
        -DCAPSTONE_TMS320C64X_SUPPORT=OFF
        -DCAPSTONE_M680X_SUPPORT=OFF
        -DCAPSTONE_EVM_SUPPORT=OFF
        -DCAPSTONE_BUILD_DIET=OFF
        -DCAPSTONE_X86_REDUCE=OFF
        -DCAPSTONE_BUILD_TESTS=OFF
        -DCAPSTONE_BUILD_STATIC=ON
        -DCAPSTONE_BUILD_SHARED=OFF
        -DCMAKE_BUILD_TYPE=Release
    BUILD_COMMAND make -s -j${BUILDING_JOBS_NUM}
    BUILD_IN_SOURCE 1
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
