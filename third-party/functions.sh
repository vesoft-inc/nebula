#!/bin/sh

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

    VGRAPH_INCLUDE_DIRS=""
    VGRAPH_LIB_DIRS=""
    VGRAPH_PREFIX_DIRS=""
    if [[ -n $VGRAPH_KRB5_ROOT ]]; then
        VGRAPH_PREFIX_DIRS="$VGRAPH_KRB5_ROOT;$VGRAPH_PREFIX_DIRS"
        VGRAPH_INCLUDE_DIRS="$VGRAPH_KRB5_ROOT/include;$VGRAPH_INCLUDE_DIRS"
        VGRAPH_LIB_DIRS="$VGRAPH_KRB5_ROOT/lib;$VGRAPH_LIB_DIRS"
    fi
    if [[ -n $VGRAPH_LIBUNWIND_ROOT ]]; then
        VGRAPH_PREFIX_DIRS="$VGRAPH_LIBUNWIND_ROOT;$VGRAPH_PREFIX_DIRS"
        VGRAPH_INCLUDE_DIRS="$VGRAPH_LIBUNWIND_ROOT/include;$VGRAPH_INCLUDE_DIRS"
        VGRAPH_LIB_DIRS="$VGRAPH_LIBUNWIND_ROOT/lib;$VGRAPH_LIB_DIRS"
    fi
    if [[ -n $VGRAPH_OPENSSL_ROOT ]]; then
        VGRAPH_PREFIX_DIRS="$VGRAPH_OPENSSL_ROOT;$VGRAPH_PREFIX_DIRS"
        VGRAPH_INCLUDE_DIRS="$VGRAPH_OPENSSL_ROOT/include;$VGRAPH_INCLUDE_DIRS"
        VGRAPH_LIB_DIRS="$VGRAPH_OPENSSL_ROOT/lib;$VGRAPH_LIB_DIRS"
    fi
    if [[ -n $VGRAPH_BOOST_ROOT ]]; then
        VGRAPH_PREFIX_DIRS="$VGRAPH_BOOST_ROOT;$VGRAPH_PREFIX_DIRS"
        VGRAPH_INCLUDE_DIRS="$VGRAPH_BOOST_ROOT/include;$VGRAPH_INCLUDE_DIRS"
        VGRAPH_LIB_DIRS="$VGRAPH_BOOST_ROOT/lib;$VGRAPH_LIB_DIRS"
    fi
    if [[ -n $VGRAPH_GPERF_BIN_DIR ]]; then
        VGRAPH_LIB_DIRS="$VGRAPH_GPERF_BIN_DIR;$VGRAPH_LIB_DIRS"
    fi

    CMAKE_FLAGS="-DCMAKE_CXX_COMPILER:FILEPATH=$VGRAPH_CXX_COMPILER -DCMAKE_C_COMPILER:FILEPATH=$VGRAPH_C_COMPILER -DCMAKE_SKIP_INSTALL_RPATH:BOOL=YES -DCMAKE_SKIP_RPATH:BOOL=YES -DBUILD_SHARED_LIBS=OFF -DCMAKE_VERBOSE_MAKEFILE:BOOL=TRUE -DTHREADS_PREFER_PTHREAD_FLAG:BOOL=TRUE -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH"

    GCC_VER=`$VGRAPH_C_COMPILER --version | head -1 | cut -d ' ' -f 3`
    EXTRA_CXXFLAGS="-O2"
    EXTRA_LDFLAGS=
}

