#!/bin/sh

source ../functions.sh

prepareBuild "proxygen" "/proxygen"

# Link the googletest
cd $SOURCE_DIR/lib/test
ln -s $CURR_DIR/googletest-release-1.8.0.zip release-1.8.0.zip

BOOST_RELEASE=$TOOLS_ROOT/boost
GPERF_RELEASE=$TOOLS_ROOT/gperf
OPENSSL_RELEASE=$TOOLS_ROOT/openssl
LIBUNWIND_RELEASE=$TOOLS_ROOT/libunwind

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
echo

#
# Need to modify the original configure.ac to remove the reference of -lfolly
#
# LIBS="$LIBS $BOOST_LDFLAGS -lpthread -pthread -lfolly -lglog"
# LIBS="$LIBS -ldouble-conversion -lboost_system -lboost_thread"
# ==>
# LIBS="$LIBS $BOOST_LDFLAGS -lpthread -pthread -lglog"
# LIBS="$LIBS -ldouble-conversion -lboost_context -lboost_system -lboost_thread"
#

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
    if !(PATH=$GPERF_RELEASE/bin:$PATH CC=$GCC_ROOT/bin/gcc CPP=$GCC_ROOT/bin/cpp CXX=$GCC_ROOT/bin/g++       CXXFLAGS="-fPIC -DPIC -I$INSTALL_PATH/include -I$INSTALL_PATH/compression/include -I$BOOST_RELEASE/include -I$OPENSSL_RELEASE/include   $EXTRA_CXXFLAGS"  CPPFLAGS=$CXXFLAGS  CFLAGS=$CXXFLAGS        LDFLAGS="-static-libgcc -static-libstdc++ -L$INSTALL_PATH/lib -L$INSTALL_PATH/compression/lib -L$BOOST_RELEASE/lib -L$OPENSSL_RELEASE/lib -L$LIBUNWIND_RELEASE/lib   $EXTRA_LDFLAGS"           LIBS="-ldl"         $SOURCE_DIR/configure --prefix=$INSTALL_PATH --enable-shared=no); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (PATH=$GPERF_RELEASE/bin:$PATH    make $@); then
    if [[ $SOURCE_DIR/lib/.libs/libproxygenlib.a -nt $INSTALL_PATH/lib/libproxygenlib.a ||
          $SOURCE_DIR/httpserver/.libs/libproxygenhttpserver.a -nt $INSTALL_PATH/lib/libproxygenhttpserver.a ||
          $SOURCE_DIR/httpclient/samples/curl/.libs/libproxygencurl.a -nt $INSTALL_PATH/lib/libproxygencurl.a ]]; then
        if (make install); then
            cd $CURR_DIR
            rm $INSTALL_PATH/lib/*.la
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

