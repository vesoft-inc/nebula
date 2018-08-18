#!/bin/sh

source ../functions.sh

prepareBuild "mstch"

boost_release=$TOOLS_ROOT/boost
openssl_release=$TOOLS_ROOT/openssl
libunwind_release=$TOOLS_ROOT/libunwind

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

CXXFLAGS="-fPIC -DPIC -I$boost_release/include  $EXTRA_CXXFLAGS"
LDFLAGS="-static-libgcc -static-libstdc++ -L$openssl_release/lib -L$libunwind_release/lib -L$boost_release/lib  $EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -levent -lunwind -ldl -lrt"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS"  -DBOOST_INCLUDEDIR=$boost_release/include -DBOOST_LIBRARYDIR=$boost_release/lib    -DBoost_NO_SYSTEM_PATHS=ON      $SOURCE_DIR/.); then
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

