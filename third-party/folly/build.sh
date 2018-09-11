#!/bin/sh

source ../functions.sh

prepareBuild "folly"

boost_release=$TOOLS_ROOT/boost
openssl_release=$TOOLS_ROOT/openssl
libunwind_release=$TOOLS_ROOT/libunwind

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
libevent_release=$THIRD_PARTY_DIR/libevent/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
zlib_release=$THIRD_PARTY_DIR/zlib/_install
zstd_release=$THIRD_PARTY_DIR/zstd/_install
snappy_release=$THIRD_PARTY_DIR/snappy/_install

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

COMPILER_FLAGS="-fPIC -DPIC -DFOLLY_HAVE_LIBDWARF_DWARF_H -DFOLLY_HAVE_LIBZSTD -DFOLLY_HAVE_MEMRCHR -Wno-noexcept-type   $EXTRA_CXXFLAGS"
EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++ -L$libunwind_release/lib    $EXTRA_LDFLAGS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_EXE_LINKER_FLAGS:STRING="$EXE_LINKER_FLAGS" -DOPENSSL_INCLUDE_DIR=$openssl_release/include -DOPENSSL_SSL_LIBRARY=$openssl_release/lib/libssl.a -DOPENSSL_CRYPTO_LIBRARY=$openssl_release/lib/libcrypto.a -DBOOST_ROOT=$boost_release -DBoost_USE_STATIC_LIBS:BOOL=YES -DDOUBLE_CONVERSION_INCLUDE_DIR=$double_conversion_release/include -DDOUBLE_CONVERSION_LIBRARY=$double_conversion_release/lib/libdouble-conversion.a -DLIBEVENT_INCLUDE_DIR=$libevent_release/include -DLIBEVENT_LIB=$libevent_release/lib/libevent.a -DLIBGFLAGS_INCLUDE_DIR=$gflags_release/include -DLIBGFLAGS_LIBRARY_RELEASE=$gflags_release/lib/libgflags.a -DLIBGLOG_INCLUDE_DIR=$glog_release/include -DLIBGLOG_LIBRARY=$glog_release/lib/libglog.a -DZLIB_INCLUDE_DIR=$zlib_release/include -DZLIB_LIBRARY=$zlib_release/lib/libz.a -DZSTD_INCLUDE_DIR=$zstd_release/include -DZSTD_LIBRARY=$zstd_release/lib/libzstd.a -DSNAPPY_INCLUDE_DIR=$snappy_release/include -DSNAPPY_LIBRARY_RELEASE=$snappy_release/lib/libsnappy.a      $SOURCE_DIR); then
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

