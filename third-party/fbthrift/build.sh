#!/bin/sh

source ../functions.sh

prepareBuild "fbthrift"

boost_release=$TOOLS_ROOT/boost
openssl_release=$TOOLS_ROOT/openssl
libunwind_release=$TOOLS_ROOT/libunwind
krb_release=$TOOLS_ROOT/krb5

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
folly_release=$THIRD_PARTY_DIR/folly/_install
wangle_release=$THIRD_PARTY_DIR/wangle/_install
mstch_release=$THIRD_PARTY_DIR/mstch/_install
zlib_release=$THIRD_PARTY_DIR/zlib/_install
zstd_release=$THIRD_PARTY_DIR/zstd/_install

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

COMPILER_FLAGS="-fPIC -DPIC -I$openssl_release/include -I$krb_release/include   $EXTRA_CXXFLAGS"
LINKER_FLAGS="-static-libgcc -static-libstdc++ -L$krb_release/lib -L$libunwind_release/lib   $EXTRA_LDFLAGS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_CXX_FLAGS:STRING="$COMPILER_FLAGS" -DCMAKE_STATIC_LINKER_FLAGS:STRING="" -DCMAKE_EXE_LINKER_FLAGS:STRING="$LINKER_FLAGS"  -DOPENSSL_INCLUDE_DIR=$openssl_release/include -DOPENSSL_SSL_LIBRARY=$openssl_release/lib/libssl.a -DOPENSSL_CRYPTO_LIBRARY=$openssl_release/lib/libcrypto.a -DBOOST_ROOT=$boost_release -DBoost_USE_STATIC_LIBS:BOOL=YES -DMSTCH_INCLUDE_DIRS=$mstch_release/include -DMSTCH_LIBRARIES=$mstch_release/lib/libmstch.a -DDOUBLE_CONVERSION_INCLUDE_DIR=$double_conversion_release/include -DDOUBLE_CONVERSION_LIBRARY=$double_conversion_release/lib -Dfolly_DIR=$folly_release/lib/cmake/folly -DGFLAGS_INCLUDE_DIR=$gflags_release/include -DGFLAGS_LIBRARY=$gflags_release/lib -DGLOG_INCLUDE_DIRS=$glog_release/include -DGLOG_LIBRARIES=$glog_release/lib/libglog.a -Dwangle_DIR=$wangle_release/lib/cmake/wangle -DZLIB_INCLUDE_DIRS=$zlib_release/include -DZLIB_LIBRARIES=$zlib_release/lib/libz.a -DZSTD_INCLUDE_DIRS=$zstd_release/include -DZSTD_LIBRARIES=$zstd_release/lib/libzstd.a -DTHRIFT_HOME=$INSTALL_PATH -DFLEX_EXECUTABLE=$FLEX_ROOT/bin/flex      $SOURCE_DIR); then
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

