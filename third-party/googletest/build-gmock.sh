#!/bin/bash

PROJECT_NAME=$1
SOURCE_DIR=$2
shift 2

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
gtest_release=$INSTALL_PATH
libevent_release=$THIRD_PARTY_DIR/libevent/_install

echo
echo "Start building $PROJECT_NAME with $NEBULA_CXX_COMPILER ($CXX_VER_STR)"
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
    if !(CC=$NEBULA_C_COMPILER CPP="$NEBULA_C_COMPILER -E" CXX=$NEBULA_CXX_COMPILER    CXXFLAGS="-fPIC -DPIC -DHAVE_LIB_GFLAGS -std=c++11  -I$double_conversion_release/include -I$gflags_release/include -I$glog_release/include -I$gtest_release/include -I$libevent_release/include   $EXTRA_CXXFLAGS"      CFLAGS=$CXXFLAGS CPPFLAGS=$CXXFLAGS      LDFLAGS="-L$double_conversion_release/lib -L$gflags_release/lib -L$glog_release/lib -L$gtest_release/lib -L$libevent_release/lib    $EXTRA_LDFLAGS"          $SOURCE_DIR/configure --prefix=$INSTALL_PATH --with-gnu-ld); then
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

