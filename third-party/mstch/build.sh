#!/bin/bash

source ../functions.sh

prepareBuild "mstch"

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

CXXFLAGS="-fPIC -DPIC    $EXTRA_CXXFLAGS"
LDFLAGS="$EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -levent -lunwind -ldl -lrt"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($NEBULA_CMAKE $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" -DCMAKE_INCLUDE_PATH="$NEBULA_INCLUDE_DIRS" -DCMAKE_LIBRARY_DIRS="$NEBULA_LIB_DIRS" -DBoost_NO_SYSTEM_PATHS=OFF     $SOURCE_DIR/.); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ all && make install); then
    cd $CURR_DIR
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

