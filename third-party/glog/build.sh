#!/bin/sh

source ../functions.sh

prepareBuild "glog"

#GFLAGS_RELEASE=$TOOLS_ROOT/gflags
#DOUBLE_CONVERSION_RELEASE=$TOOLS_ROOT/double-conversion
#LIBEVENT_RELEASE=$TOOLS_ROOT/libevent

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

if [[ $SOURCE_DIR/configure -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$GCC_ROOT/bin/gcc CPP=$GCC_ROOT/bin/cpp CXX=$GCC_ROOT/bin/g++    CXXFLAGS="-fPIC -DPIC -DHAVE_LIB_GFLAGS  -I$INSTALL_PATH/include   $EXTRA_CXXFLAGS"  CFLAGS=$CXXFLAGS  CPPFLAGS=$CXXFLAGS       LDFLAGS="-static-libgcc -static-libstdc++ -L$INSTALL_PATH/lib   $EXTRA_LDFLAGS"     LIBS="-lgflags"          $SOURCE_DIR/configure --prefix=$INSTALL_PATH --enable-shared=no); then
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

