#!/bin/sh

source ../functions.sh

prepareBuild "rocksdb"

boost_release=$TOOLS_ROOT/boost
openssl_release=$TOOLS_ROOT/openssl
zlib_release=$THIRD_PARTY_DIR/zlib/_install
zstd_release=$THIRD_PARTY_DIR/zstd/_install
snappy_release=$THIRD_PARTY_DIR/snappy/_install
bzip2_release=$THIRD_PARTY_DIR/bzip2/_install

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

compiler_flags="-fPIC -DPIC -DOPTDBG=1 -DROCKSDB_PLATFORM_POSIX -DROCKSDB_SUPPORT_THREAD_LOCAL -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_MALLOC_USABLE_SIZE -march=native -Wno-error=shadow  $EXTRA_CXXFLAGS"
exe_linker_flags="$EXTRA_LDFLAGS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="${compiler_flags}" -DCMAKE_CXX_FLAGS:STRING="${compiler_flags}" -DCMAKE_EXE_LINKER_FLAGS:STRING="${exe_linker_flags}" -DBOOST_ROOT=$boost_release -DOPENSSL_ROOT_DIR=$openssl_release -DWITH_ZLIB=1 -DWITH_BZIP2=1 -DWITH_SNAPPY=1 -DWITH_ZSTD=1 -DZLIB_ROOT_DIR=$zlib_release -DBZIP2_ROOT_DIR=$bzip2_release -DSNAPPY_ROOT_DIR=$snappy_release -DZSTD_ROOT_DIR=$zstd_release -DWITH_GFLAGS=0 -DWITH_JEMALLOC=0 -DWITH_TESTS=off .); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to install ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/librocksdb.a -nt $INSTALL_PATH/lib/librocksdb.a ]]; then
        echo "===> Ready to install"
        if (make install); then
            cd $CURR_DIR
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

