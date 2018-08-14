#!/bin/sh

source ../functions.sh

prepareBuild "folly"

BOOST_RELEASE=$TOOLS_ROOT/boost
OPENSSL_RELEASE=$TOOLS_ROOT/openssl
LIBUNWIND_RELEASE=$TOOLS_ROOT/libunwind

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
echo

#if !(cd $SOURCE_DIR && autoreconf -ivf ); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

COMPILER_FLAGS="-fPIC -DPIC -DFOLLY_HAVE_LIBDWARF_DWARF_H -DFOLLY_HAVE_LIBZSTD -I$INSTALL_PATH/include   $EXTRA_CXXFLAGS"
EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++ -L$LIBUNWIND_RELEASE/lib -L$INSTALL_PATH/lib   $EXTRA_LDFLAGS"
INCLUDE_DIR=$INSTALL_PATH/include
LIB_DIR=$INSTALL_PATH/lib

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_EXE_LINKER_FLAGS:STRING="$EXE_LINKER_FLAGS" -DOPENSSL_INCLUDE_DIR=$OPENSSL_RELEASE/include -DOPENSSL_SSL_LIBRARY=$OPENSSL_RELEASE/lib/libssl.a -DOPENSSL_CRYPTO_LIBRARY=$OPENSSL_RELEASE/lib/libcrypto.a -DBOOST_ROOT=$BOOST_RELEASE -DBoost_USE_STATIC_LIBS:BOOL=YES -DDOUBLE_CONVERSION_INCLUDE_DIR=$INCLUDE_DIR -DDOUBLE_CONVERSION_LIBRARY=$LIB_DIR/libdouble-conversion.a -DLIBEVENT_INCLUDE_DIR=$INCLUDE_DIR -DLIBEVENT_LIB=$LIB_DIR/libevent.a -DGFLAGS_INCLUDE_DIR=$INCLUDE_DIR -DGFLAGS_LIBRARY=$LIB_DIR/libgflags.a -DLIBGLOG_INCLUDE_DIR=$INCLUDE_DIR -DLIBGLOG_LIBRARY=$LIB_DIR/libglog.a -DZLIB_INCLUDE_DIRS=$INCLUDE_DIR -DZLIB_LIBRARIES=$LIB_DIR/libz.a -DZSTD_INCLUDE_DIRS=$INCLUDE_DIR -DZSTD_LIBRARIES=$LIB_DIR/libzstd.a       $SOURCE_DIR); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ install); then
    cd $CURR_DIR
    rm -f $INSTALL_PATH/lib/*.la
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

