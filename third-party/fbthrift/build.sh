#!/bin/sh

source ../functions.sh

prepareBuild "fbthrift"

BOOST_RELEASE=$TOOLS_ROOT/boost
OPENSSL_RELEASE=$TOOLS_ROOT/openssl
LIBUNWIND_RELEASE=$TOOLS_ROOT/libunwind
KRB_RELEASE=$TOOLS_ROOT/krb5

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
echo

#if !(cd $SOURCE_DIR && autoreconf -ivf -I/usr/local/share/aclocal-1.15 -I/usr/share/aclocal); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

COMPILER_FLAGS="-fPIC -DPIC -I$OPENSSL_RELEASE/include -I$KRB_RELEASE/include -I$INSTALL_PATH/include   $EXTRA_CXXFLAGS"
LINKER_FLAGS="-static-libgcc -static-libstdc++ -L$KRB_RELEASE/lib -L$LIBUNWIND_RELEASE/lib -L$INSTALL_PATH/lib   $EXTRA_LDFLAGS"
INCLUDE_DIR=$INSTALL_PATH/include
COMPRESS_INCLUDE_DIR=$INSTALL_PATH/compression/include
LIB_DIR=$INSTALL_PATH/lib
COMPRESS_LIB_DIR=$INSTALL_PATH/compression/lib

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_STATIC_LINKER_FLAGS:STRING="" -DCMAKE_EXE_LINKER_FLAGS:STRING="$LINKER_FLAGS"  -DOPENSSL_INCLUDE_DIR=$OPENSSL_RELEASE/include -DOPENSSL_SSL_LIBRARY=$OPENSSL_RELEASE/lib/libssl.a -DOPENSSL_CRYPTO_LIBRARY=$OPENSSL_RELEASE/lib/libcrypto.a -DBOOST_ROOT=$BOOST_RELEASE -DBoost_USE_STATIC_LIBS:BOOL=YES -DMSTCH_INCLUDE_DIRS=$INCLUDE_DIR -DMSTCH_LIBRARIES=$LIB_DIR/libmstch.a -DDOUBLE_CONVERSION_INCLUDE_DIR=$INCLUDE_DIR -DDOUBLE_CONVERSION_LIBRARY=$LIB_DIR -DFOLLY_ROOT=$INSTALL_PATH -DGFLAGS_INCLUDE_DIR=$INCLUDE_DIR -DGFLAGS_LIBRARY=$LIB_DIR -DGLOG_INCLUDE_DIRS=$INCLUDE_DIR -DGLOG_LIBRARIES=$LIB_DIR/libglog.a -DWANGLE_INCLUDE_DIRS=$INCLUDE_DIR -DWANGLE_LIBRARIES=$LIB_DIR/libwangle.a -DZLIB_INCLUDE_DIRS=$COMPRESS_INCLUDE_DIR -DZLIB_LIBRARIES=$COMPRESS_LIB_DIR/libz.a -DZSTD_INCLUDE_DIRS=$COMPRESS_INCLUDE_DIR -DZSTD_LIBRARIES=$COMPRESS_LIB_DIR/libzstd.a -DFLEX_EXECUTABLE=$FLEX_ROOT/bin/flex    -DTHRIFT_HOME=$INSTALL_PATH    $SOURCE_DIR); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ all && make install); then
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

