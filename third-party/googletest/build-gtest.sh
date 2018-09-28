#!/bin/sh

PROJECT_NAME=$1
SOURCE_DIR=$2
shift 2

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
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
      $CURR_DIR/build-gtest.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$VGRAPH_C_COMPILER CPP="$VGRAPH_C_COMPILER -E" CXX=$VGRAPH_CXX_COMPILER    CXXFLAGS="-fPIC -DPIC -DHAVE_LIB_GFLAGS -std=c++11 -I$double_conversion/include -I$libevent_release/include -I$gflags_release/include -I$glog_release/include    $EXTRA_CXXFLAGS"      CFLAGS=$CXXFLAGS CPPFLAGS=$CXXFLAGS      LDFLAGS="-L$double_conversion/lib -L$libevent_release/lib -L$gflags_release/lib -L$glog_release/lib     $EXTRA_LDFLAGS"            $SOURCE_DIR/configure --prefix=$INSTALL_PATH --with-gnu-ld); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/lib/.libs/libgtest.a -nt $INSTALL_PATH/lib/libgtest.a ||
          $SOURCE_DIR/lib/.libs/libgtest_main.a -nt $INSTALL_PATH/lib/libgtest_main.a ]]; then
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

