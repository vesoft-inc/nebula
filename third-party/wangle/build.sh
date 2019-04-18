#!/bin/bash

source ../functions.sh

prepareBuild "wangle" "/wangle"

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
folly_release=$THIRD_PARTY_DIR/folly/_install
libevent_release=$THIRD_PARTY_DIR/libevent/_install

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

CXXFLAGS="-fPIC -DPIC  $EXTRA_CXXFLAGS"
LDFLAGS="$EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -lssl -lstdc++ -levent -lunwind -ldl -lrt"
NEBULA_INCLUDE_DIRS="$double_conversion_release/include;$gflags_release/include;$glog_release/include;$folly_release/include;$libevent_release/include;$NEBULA_INCLUDE_DIRS"
NEBULA_LIB_DIRS="$double_conversion_release/lib;$gflags_release/lib;$glog_release/lib;$folly_release/lib;$libevent_release/lib;$NEBULA_LIB_DIRS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($NEBULA_CMAKE $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" -DCMAKE_INCLUDE_PATH="$NEBULA_INCLUDE_DIRS" -DCMAKE_LIBRARY_PATH="$NEBULA_LIB_DIRS" -DBoost_NO_SYSTEM_PATHS=OFF -DBUILD_TESTS=OFF     $SOURCE_DIR/.); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ install); then
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

