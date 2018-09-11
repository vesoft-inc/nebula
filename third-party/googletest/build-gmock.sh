#!/bin/sh

PROJECT_NAME=$1
CURR_DIR=$2
THIRD_PARTY_DIR=$3
TOOLS_ROOT=$4
SOURCE_DIR=$5
INSTALL_PATH=$6
shift 6
GCC_ROOT=$1
CMAKE_ROOT=$2
FLEX_ROOT=$3
GCC_VER=$4
EXTRA_CXXFLAGS=$5
EXTRA_LDFLAGS=$6
shift 6

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
gtest_release=$INSTALL_PATH
libevent_release=$THIRD_PARTY_DIR/libevent/_install

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
echo

cd $SOURCE_DIR

if [[ $SOURCE_DIR/configure.ac -nt $SOURCE_DIR/configure ]]; then
    if !(autoreconf -ivf); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to auto-reconfigure ###"
        echo
        exit 1
    fi
fi

if [[ $SOURCE_DIR/configure -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build-gmock.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$GCC_ROOT/bin/gcc CPP=$GCC_ROOT/bin/cpp CXX=$GCC_ROOT/bin/g++    CXXFLAGS="-fPIC -DPIC -DHAVE_LIB_GFLAGS -std=c++11  -I$double_conversion_release/include -I$gflags_release/include -I$glog_release/include -I$gtest_release/include -I$libevent_release/include   $EXTRA_CXXFLAGS"      CFLAGS=$CXXFLAGS CPPFLAGS=$CXXFLAGS      LDFLAGS="-static-libgcc -static-libstdc++  -L$double_conversion_release/lib -L$gflags_release/lib -L$glog_release/lib -L$gtest_release/lib -L$libevent_release/lib    $EXTRA_LDFLAGS"          $SOURCE_DIR/configure --prefix=$INSTALL_PATH --with-gnu-ld); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/lib/.libs/libgmock.a -nt $INSTALL_PATH/lib/libgmock.a ||
          $SOURCE_DIR/lib/.libs/libgmock_main.a -nt $INSTALL_PATH/lib/libgmock_main.a ]]; then
        mkdir -p $INSTALL_PATH/lib
        cp -rf lib/.libs/*.a $INSTALL_PATH/lib/.
        cp -rf include $INSTALL_PATH/.
        cd $CURR_DIR
        echo
        echo ">>> $PROJECT_NAME is built and installed successfully <<<"
        echo
        exit 0
    else
        echo
        echo ">>> $PROJECT_NAME is up-to-date <<<"
        echo
        exit 0
    fi
else
    cd $CURR_DIR
    echo
    echo "### $PROJECT_NAME failed to build ###"
    echo
    exit 1
fi

