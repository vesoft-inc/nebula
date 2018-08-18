#!/bin/sh

source ../functions.sh

prepareBuild "mstch"

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

CXXFLAGS="-fPIC -DPIC -I$INSTALL_PATH/include -I$BOOST_RELEASE/include  $EXTRA_CXXFLAGS"
LDFLAGS="-static-libgcc -static-libstdc++ -L$INSTALL_PATH/lib -L$OPENSSL_RELEASE/lib -L$LIBUNWIND_RELEASE/lib -L$BOOST_RELEASE/lib  $EXTRA_LDFLAGS -lboost_context -lboost_chrono -lboost_thread -lboost_system -lboost_regex -levent -lunwind -ldl -lrt"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CC_FLAGS:STRING="$CXXFLAGS" -DCMAKE_CPP_FLAGS:STRING="$CXXFLAGS" -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS"  -DBOOST_INCLUDEDIR=$BOOST_RELEASE/include -DBOOST_LIBRARYDIR=$BOOST_RELEASE/lib    -DBoost_NO_SYSTEM_PATHS=ON      $SOURCE_DIR/.); then
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

