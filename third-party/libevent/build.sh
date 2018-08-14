#!/bin/sh

source ../functions.sh

prepareBuild "libevent"

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

if [[ $SOURCE_DIR/configure -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$GCC_ROOT/bin/gcc CPP=$GCC_ROOT/bin/cpp CXX=$GCC_ROOT/bin/g++ CXXFLAGS="-fPIC -DPIC  $EXTRA_CXXFLAGS" CFLAGS=$CXXFLAGS CPPFLAGS=$CXXFLAGS      LDFLAGS="-static-libgcc -static-libstdc++  $EXTRA_LDFLAGS"        $SOURCE_DIR/configure --prefix=$INSTALL_PATH --enable-shared=no); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/.libs/libevent.a -nt $INSTALL_PATH/lib/libevent.a ||
          $SOURCE_DIR/.libs/libevent_core.a -nt $INSTALL_PATH/lib/libevent_core.a ||
          $SOURCE_DIR/.libs/libevent_extra.a -nt $INSTALL_PATH/lib/libevent_extra.a ||
          $SOURCE_DIR/.libs/libevent_pthreads.a -nt $INSTALL_PATH/lib/libevent_pthreads.a ]]; then
        if (make install); then
            cd $CURR_DIR
            rm -f $INSTALL_PATH/lib/libevent*.la
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

