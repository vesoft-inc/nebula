#!/bin/sh

source ../functions.sh

prepareBuild "snappy"

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
echo

#if !(cd $SOURCE_DIR && NOCONFIGURE=1 ./autogen.sh); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

COMPILER_FLAGS=" -fPIC -DPIC   $EXTRA_CXXFLAGS"
LINKER_FLAGS=" -static-libgcc -static-libstdc++    $EXTRA_LDFLAGS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_INSTALL_LIBDIR=lib -DSNAPPY_BUILD_TESTS=NO -DCMAKE_SKIP_RPATH:BOOL=YES -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_STATIC_LINKER_FLAGS:STRING="" -DCMAKE_EXE_LINKER_FLAGS:STRING="$LINKER_FLAGS"    $SOURCE_DIR); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ all && make install); then
    cd $CURR_DIR
    rm -fr $INSTALL_PATH/lib/CMake
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

