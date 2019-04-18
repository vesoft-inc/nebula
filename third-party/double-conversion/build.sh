#!/bin/bash

source ../functions.sh

prepareBuild "double-conversion"

echo
echo "Start building $PROJECT_NAME with $NEBULA_CXX_COMPILER ($CXX_VER_STR)"
echo

#if !(cd $SOURCE_DIR && autoreconf -ivf); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

COMPILE_FLAGS="-fPIC -DPIC -std=c++11    $EXTRA_CXXFLAGS "

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($NEBULA_CMAKE $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$COMPILE_FLAGS" -DCMAKE_CC_FLAGS:STRING="$COMPILE_FLAGS" -DCMAKE_CPP_FLAGS:STRING="$COMPILE_FLAGS"    $SOURCE_DIR); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ && make install); then
    cd $CURR_DIR
    mkdir -p $INSTALL_PATH/include
    cp $SOURCE_DIR/src/*.h $INSTALL_PATH/include/.
    echo
    echo ">>> $PROJECT_NAME is built and installed successfully <<<"
    echo
    exit 0
else
    cd $CURR_DIR
    echo
    echo "### $PROJECT_NAME failed to build ###"
    echo
    exit 1
fi

