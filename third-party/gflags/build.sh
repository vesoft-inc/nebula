#!/bin/sh

source ../functions.sh

prepareBuild "gflags"

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER and glibc-$GLIBC_VER
echo

#if !(cd $SOURCE_DIR && NOCONFIGURE=1 ./autogen.sh); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_CXX_FLAGS:STRING="-fPIC -DPIC $EXTRA_CXXFLAGS " -DCMAKE_CC_FLAGS:STRING="-fPIC -DPIC $EXTRA_CXXFLAGS " -DCMAKE_CPP_FLAGS:STRING="-fPIC -DPIC $EXTRA_CXXFLAGS " -DCMAKE_STATIC_LINKER_FLAGS:STRING="" -DCMAKE_LINK_LIBRARY_FLAG="-static-libgcc -static-libstdc++ -lrt"    $SOURCE_DIR); then
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

