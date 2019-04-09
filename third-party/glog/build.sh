#!/bin/bash

source ../functions.sh

prepareBuild "glog"

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
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
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$NEBULA_C_COMPILER CPP="$NEBULA_C_COMPILER -E" CXX=$NEBULA_CXX_COMPILER   CXXFLAGS="-fPIC -DPIC -DHAVE_LIB_GFLAGS -I$double_conversion_release/include -I$gflags_release/include -I$libevent_release/include     $EXTRA_CXXFLAGS"  CFLAGS=$CXXFLAGS  CPPFLAGS=$CXXFLAGS       LDFLAGS="-L$double_conversion_release/lib -L$gflags_release/lib -L$libevent_release/lib   $EXTRA_LDFLAGS"     LIBS="-lgflags"          $SOURCE_DIR/configure --prefix=$INSTALL_PATH --enable-shared=no); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/.libs/libglog.a -nt $INSTALL_PATH/lib/libglog.a ]]; then
        if (make install); then
            cd $CURR_DIR
            rm $INSTALL_PATH/lib/libglog*.la
            echo
            echo ">>> $PROJECT_NAME is built and installed successfully <<<"
            echo
            exit 0
        else
            cd $CURR_DIR
            echo
            echo "### $PROJECT_NAME failed to install ###"
            echo
            exit 1
        fi
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

