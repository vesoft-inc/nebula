#!/bin/sh

source ../functions.sh

prepareBuild "wangle" "/wangle"

boost_release=$TOOLS_ROOT/boost
openssl_release=$TOOLS_ROOT/openssl
libunwind_release=$TOOLS_ROOT/libunwind

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
folly_release=$THIRD_PARTY_DIR/folly/_install
libevent_release=$THIRD_PARTY_DIR/libevent/_install

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
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
LDFLAGS="-static-libgcc -static-libstdc++ -L$libunwind_release/lib  $EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -lssl -lstdc++ -levent -lunwind -ldl -lrt"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" -DGLOG_INCLUDE_DIR=$glog_release/include -DGLOG_LIBRARY=$glog_release/lib/libglog.a -DGFLAGS_INCLUDE_DIR=$gflags_release/include -DGFLAGS_LIBRARY=$gflags_release/lib/libgflags.a -DDOUBLE_CONVERSION_INCLUDE_DIR=$double_conversion_release/include -DDOUBLE_CONVERSION_LIBRARY=$double_conversion_release/lib/libdouble-conversion.a -DLIBEVENT_INCLUDE_DIR=$libevent_release/include -DLIBEVENT_LIBRARY=$libevent_release/lib/libevent.a -DFOLLY_INCLUDEDIR=$folly_release/include -DFOLLY_LIBRARYDIR=$folly_release/lib -DBOOST_INCLUDEDIR=$boost_release/include -DBOOST_LIBRARYDIR=$boost_release/lib -DBoost_NO_SYSTEM_PATHS=ON -DOPENSSL_ROOT_DIR=$openssl_release -DBUILD_TESTS=OFF     $SOURCE_DIR/.); then
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

