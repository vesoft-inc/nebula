# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# The build can be controlled by defining following variables on the
# <cmake> command line
#
#   CMAKE_C_COMPILER               -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER             -- Specify the compiler for C++ language
#
#   NEBULA_THIRDPARTY_ROOT         -- Specify the third-party root dir.
#   ENABLE_TESTING                 -- Build unit test
#
cmake_minimum_required(VERSION 3.0.0)

project("Nebula Graph codec" C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (${CMAKE_SYSTEM_NAME} MATCHES "(FreeBSD|Windows)")
    MESSAGE(FATAL_ERROR "Only Linux is supported and MacOS")
endif()

option(ENABLE_TESTING "Whether to compile unit test ON or OFF" OFF)

message(STATUS "CMAKE_CURRENT_BINARY_DIR:" ${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "CMAKE_CURRENT_SOURCE_DIR:" ${CMAKE_CURRENT_SOURCE_DIR})
message(STATUS "NEBULA_THIRDPARTY_ROOT:" ${NEBULA_THIRDPARTY_ROOT})

# locate jni header
include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)
include_directories(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../)
include_directories(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../common)
include_directories(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(SYSTEM ${NEBULA_THIRDPARTY_ROOT}/include)

if (NOT DEFINED ENV{JAVA_HOME})
    message(FATAL_ERROR "Couldn't find JAVA_HOME")
endif()

if (APPLE)
    set(JNI_INCLUDE "$ENV{JAVA_HOME}/include")
    # include jni.h path
    include_directories(AFTER ${JNI_INCLUDE})
    # include the install path of glog and gflags
    include_directories(AFTER /usr/local/include)
    # include jni_md.h path
    find_path(JNI_MD_FILE_DIR NAMES jni_md.h)
    if (NOT JNI_MD_FILE_DIR)
        message(FATAL_ERROR "Couldn't find jni_md.h")
    endif()
    include_directories(AFTER ${JNI_MD_FILE_DIR})
    link_directories(/usr/local/lib)
endif()

if (NEBULA_THIRDPARTY_ROOT)
    link_directories(
        ${NEBULA_THIRDPARTY_ROOT}/lib
        ${NEBULA_THIRDPARTY_ROOT}/lib64
    )
endif()

if (ENABLE_TESTING)
    enable_testing()
endif()

if (!CMAKE_CXX_COMPILER)
    message(FATAL_ERROR "No C++ compiler found")
endif()

add_compile_options(-fPIC)
add_compile_options(-Wall)
add_compile_options(-Werror)
add_compile_options(-Wunused-parameter)


add_subdirectory(src)

add_custom_target(
    java_lib
    COMMAND mvn clean package
    COMMAND mv target ${CMAKE_CURRENT_BINARY_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/java
    DEPENDS nebula_codec
)

