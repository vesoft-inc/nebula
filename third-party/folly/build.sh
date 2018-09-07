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

INCLUDE_DIR=$INSTALL_PATH/include
COMPRESS_INCLUDE_DIR=$INSTALL_PATH/compression/include
LIB_DIR=$INSTALL_PATH/lib
COMPRESS_LIB_DIR=$INSTALL_PATH/compression/lib

COMPILER_FLAGS="-fPIC -DPIC -DFOLLY_HAVE_LIBDWARF_DWARF_H -DFOLLY_HAVE_LIBZSTD -I$INCLUDE_DIR   $EXTRA_CXXFLAGS"
EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++ -L$LIBUNWIND_RELEASE/lib -L$LIB_DIR   $EXTRA_LDFLAGS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_EXE_LINKER_FLAGS:STRING="$EXE_LINKER_FLAGS" -DOPENSSL_INCLUDE_DIR=$OPENSSL_RELEASE/include -DOPENSSL_SSL_LIBRARY=$OPENSSL_RELEASE/lib/libssl.a -DOPENSSL_CRYPTO_LIBRARY=$OPENSSL_RELEASE/lib/libcrypto.a -DBOOST_ROOT=$BOOST_RELEASE -DBoost_USE_STATIC_LIBS:BOOL=YES -DDOUBLE_CONVERSION_INCLUDE_DIR=$INCLUDE_DIR -DDOUBLE_CONVERSION_LIBRARY=$LIB_DIR/libdouble-conversion.a -DLIBEVENT_INCLUDE_DIR=$INCLUDE_DIR -DLIBEVENT_LIB=$LIB_DIR/libevent.a -DGFLAGS_INCLUDE_DIR=$INCLUDE_DIR -DGFLAGS_LIBRARY=$LIB_DIR/libgflags.a -DLIBGLOG_INCLUDE_DIR=$INCLUDE_DIR -DLIBGLOG_LIBRARY=$LIB_DIR/libglog.a -DZLIB_INCLUDE_DIR=$COMPRESS_INCLUDE_DIR -DZLIB_LIBRARY=$COMPRESS_LIB_DIR/libz.a -DZSTD_INCLUDE_DIR=$COMPRESS_INCLUDE_DIR -DZSTD_LIBRARY=$COMPRESS_LIB_DIR/libzstd.a -DSNAPPY_INCLUDE_DIR=$COMPRESS_INCLUDE_DIR -DSNAPPY_LIBRARY=$COMPRESS_LIB_DIR/libsnappy.a       $SOURCE_DIR); then
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

