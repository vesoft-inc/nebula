#!/bin/bash

# The method takes up to three parameters
#   1st: Project name
#   2nd: source directory. It must be a sub-directory of
#        the default source path "_build/${project_name}"
#        If specified, it must start with "/", so that it can
#        be appended to the default source path
function prepareBuild() {
    # Make sure the script is executed from the source directory
    CURR_DIR=`dirname $0`
    if [[ $CURR_DIR != '.' ]]; then
        echo "build.sh must be executed under <source> directroy"
        exit 1
    fi

    if [[ $# < 1 ]]; then
        echo "Must specify the project name"
        exit 1
    fi
    local prj_name=$1

    if [[ $# > 1 ]]; then
        local src_dir=$2
    else
        local src_dir=""
    fi

    CURR_DIR=`pwd`
    THIRD_PARTY_DIR=`cd .. && pwd`
    SOURCE_TAR_BALL_NAME=`ls ${prj_name}*.tar.gz`
    PROJECT_NAME=`echo $SOURCE_TAR_BALL_NAME | sed s/\.tar\.gz//`
    BUILD_PATH=$CURR_DIR/_build

    # Extract source code
    mkdir -p $BUILD_PATH
    cd $BUILD_PATH
    tar -zxf ../$SOURCE_TAR_BALL_NAME --keep-newer-files 2> /dev/null

    SOURCE_DIR=${BUILD_PATH}/${PROJECT_NAME}${src_dir}
    INSTALL_PATH=$CURR_DIR/_install

    NEBULA_INCLUDE_DIRS=""
    NEBULA_LIB_DIRS=""
    NEBULA_PREFIX_DIRS=""
    NEBULA_PROGRAM_DIRS=""
    if [[ -n $NEBULA_KRB5_ROOT ]]; then
        NEBULA_PREFIX_DIRS="$NEBULA_KRB5_ROOT;$NEBULA_PREFIX_DIRS"
        NEBULA_INCLUDE_DIRS="$NEBULA_KRB5_ROOT/include;$NEBULA_INCLUDE_DIRS"
        NEBULA_LIB_DIRS="$NEBULA_KRB5_ROOT/lib;$NEBULA_LIB_DIRS"
        NEBULA_PROGRAM_DIRS="$NEBULA_KRB5_ROOT/bin;$NEBULA_PROGRAM_DIRS"
    fi
    if [[ -n $NEBULA_LIBUNWIND_ROOT ]]; then
        NEBULA_PREFIX_DIRS="$NEBULA_LIBUNWIND_ROOT;$NEBULA_PREFIX_DIRS"
        NEBULA_INCLUDE_DIRS="$NEBULA_LIBUNWIND_ROOT/include;$NEBULA_INCLUDE_DIRS"
        NEBULA_LIB_DIRS="$NEBULA_LIBUNWIND_ROOT/lib;$NEBULA_LIB_DIRS"
    fi
    if [[ -n $NEBULA_OPENSSL_ROOT ]]; then
        NEBULA_PREFIX_DIRS="$NEBULA_OPENSSL_ROOT;$NEBULA_PREFIX_DIRS"
        NEBULA_INCLUDE_DIRS="$NEBULA_OPENSSL_ROOT/include;$NEBULA_INCLUDE_DIRS"
        NEBULA_LIB_DIRS="$NEBULA_OPENSSL_ROOT/lib;$NEBULA_LIB_DIRS"
    fi
    if [[ -n $NEBULA_BOOST_ROOT ]]; then
        NEBULA_PREFIX_DIRS="$NEBULA_BOOST_ROOT;$NEBULA_PREFIX_DIRS"
        NEBULA_INCLUDE_DIRS="$NEBULA_BOOST_ROOT/include;$NEBULA_INCLUDE_DIRS"
        NEBULA_LIB_DIRS="$NEBULA_BOOST_ROOT/lib;$NEBULA_LIB_DIRS"
    fi
    if [[ -n $NEBULA_GPERF_BIN_DIR ]]; then
        NEBULA_PROGRAM_DIRS="$NEBULA_GPERF_BIN_DIR;$NEBULA_PROGRAM_DIRS"
    fi

    CMAKE_FLAGS="-DCMAKE_CXX_COMPILER:FILEPATH=$NEBULA_CXX_COMPILER -DCMAKE_C_COMPILER:FILEPATH=$NEBULA_C_COMPILER -DCMAKE_SKIP_INSTALL_RPATH:BOOL=YES -DCMAKE_SKIP_RPATH:BOOL=YES -DBUILD_SHARED_LIBS=OFF -DCMAKE_VERBOSE_MAKEFILE:BOOL=TRUE -DTHREADS_PREFER_PTHREAD_FLAG:BOOL=TRUE -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH"

    CC_VER_STR=`$NEBULA_C_COMPILER --version | head -1 | cut -d ' ' -f 3`
    CC_MAJOR_VER=`echo $CC_VER_STR | cut -d . -f 1`
    if [[ -z $CC_MAJOR_VER ]]; then
        CC_MAJOR_VER=0
    fi
    CC_MINOR_VER=`echo $CC_VER_STR | cut -d . -f 2`
    if [[ -z $CC_MINOR_VER ]]; then
        CC_MINOR_VER=0
    fi
    CC_RELEASE_VER=`echo $CC_VER_STR | cut -d . -f 3`
    if [[ -z $CC_RELEASE_VER ]]; then
        CC_RELEASE_VER=0
    fi

    CXX_VER_STR=`$NEBULA_CXX_COMPILER --version | head -1 | cut -d ' ' -f 3`
    CXX_MAJOR_VER=`echo $CXX_VER_STR | cut -d . -f 1`
    if [[ -z $CXX_MAJOR_VER ]]; then
        CXX_MAJOR_VER=0
    fi
    CXX_MINOR_VER=`echo $CXX_VER_STR | cut -d . -f 2`
    if [[ -z $CXX_MINOR_VER ]]; then
        CXX_MINOR_VER=0
    fi
    CXX_RELEASE_VER=`echo $CXX_VER_STR | cut -d . -f 3`
    if [[ -z $CXX_RELEASE_VER ]]; then
        CXX_RELEASE_VER=0
    fi

    EXTRA_CXXFLAGS="-O2"
    EXTRA_LDFLAGS="$NEBULA_GETTIME_LDFLAGS"
}

