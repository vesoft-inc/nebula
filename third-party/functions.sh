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
    TOOLS_ROOT=/home/engshare
    SOURCE_TAR_BALL_NAME=`ls ${prj_name}*.tar.gz`
    PROJECT_NAME=`echo $SOURCE_TAR_BALL_NAME | sed s/\.tar\.gz//`
    BUILD_PATH=$CURR_DIR/_build
    
    # Extract source code
    mkdir -p $BUILD_PATH
    cd $BUILD_PATH
    tar -zxf ../$SOURCE_TAR_BALL_NAME --keep-newer-files 2> /dev/null

    SOURCE_DIR=${BUILD_PATH}/${PROJECT_NAME}${src_dir}
    INSTALL_PATH=$CURR_DIR/_install

    GCC_ROOT=$TOOLS_ROOT/gcc
    CMAKE_ROOT=$TOOLS_ROOT/cmake
    FLEX_ROOT=$TOOLS_ROOT/flex

    CMAKE_FLAGS="-DCMAKE_CXX_COMPILER:FILEPATH=$GCC_ROOT/bin/g++ -DCMAKE_C_COMPILER:FILEPATH=$GCC_ROOT/bin/gcc -DCMAKE_SKIP_INSTALL_RPATH:BOOL=YES -DCMAKE_SKIP_RPATH:BOOL=YES -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON -DCMAKE_VERBOSE_MAKEFILE:BOOL=TRUE -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH"

    GCC_VER=`$GCC_ROOT/bin/gcc --version | head -1 | cut -d ' ' -f 3`
    EXTRA_CXXFLAGS="-O2"
    EXTRA_LDFLAGS=
}

