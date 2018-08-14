#!/bin/sh

source ../functions.sh

prepareBuild "wangle" "wangle"

BOOST_RELEASE=$TOOLS_ROOT/boost
OPENSSL_RELEASE=$TOOLS_ROOT/openssl
LIBUNWIND_RELEASE=$TOOLS_ROOT/libunwind

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
LDFLAGS="-static-libgcc -static-libstdc++ -L$LIBUNWIND_RELEASE/lib  $EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -lssl -lstdc++ -levent -lunwind -ldl -lrt"
INCLUDE_DIR=$INSTALL_PATH/include
LIB_DIR=$INSTALL_PATH/lib

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS"   -DGLOG_INCLUDE_DIR=$INCLUDE_DIR -DGLOG_LIBRARY=$LIB_DIR/libglog.a   -DGFLAGS_INCLUDE_DIR=$INCLUDE_DIR -DGFLAGS_LIBRARY=$LIB_DIR/libgflags.a     -DDOUBLE_CONVERSION_INCLUDE_DIR=$INCLUDE_DIR -DDOUBLE_CONVERSION_LIBRARY=$LIB_DIR/libdouble-conversion.a     -DLIBEVENT_INCLUDE_DIR=$INCLUDE_DIR  -DLIBEVENT_LIBRARY=$LIB_DIR/libevent.a    -DFOLLY_INCLUDEDIR=$INCLUDE_DIR -DFOLLY_LIBRARYDIR=$LIB_DIR   -DBOOST_INCLUDEDIR=$BOOST_RELEASE/include -DBOOST_LIBRARYDIR=$BOOST_RELEASE/lib    -DBoost_NO_SYSTEM_PATHS=ON   -DOPENSSL_ROOT_DIR=$OPENSSL_RELEASE     -DBUILD_TESTS=OFF     $SOURCE_DIR/.); then
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

