#!/bin/bash

source ../functions.sh

prepareBuild "rocksdb"

zlib_release=$THIRD_PARTY_DIR/zlib/_install
zstd_release=$THIRD_PARTY_DIR/zstd/_install
snappy_release=$THIRD_PARTY_DIR/snappy/_install
jemalloc_release=$THIRD_PARTY_DIR/jemalloc/_install

echo
echo "Start building $PROJECT_NAME with $NEBULA_C_COMPILER ($CC_VER_STR)"
echo

#if !(cd $SOURCE_DIR && autoreconf -ivf); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

compiler_flags="-Wno-pessimizing-move -Wno-redundant-move -Wno-deprecated-copy -fPIC -DPIC -DROCKSDB_PLATFORM_POSIX -DROCKSDB_SUPPORT_THREAD_LOCAL -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_MALLOC_USABLE_SIZE -Wno-error=shadow  $EXTRA_CXXFLAGS"
exe_linker_flags="-ldl $EXTRA_LDFLAGS"
NEBULA_INCLUDE_DIRS="$zlib_release/include;$zstd_release/include;$snappy_release/include;$jemalloc_release/include;$NEBULA_INCLUDE_DIRS"
NEBULA_LIB_DIRS="$zlib_release/lib;$zstd_release/lib;$snappy_release/lib;$jemalloc_release/lib;$NEBULA_LIB_DIRS"

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($NEBULA_CMAKE $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="${compiler_flags}" -DCMAKE_CXX_FLAGS:STRING="${compiler_flags}" -DCMAKE_EXE_LINKER_FLAGS:STRING="${exe_linker_flags}" -DCMAKE_INCLUDE_PATH="$NEBULA_INCLUDE_DIRS" -DCMAKE_LIBRARY_PATH="$NEBULA_LIB_DIRS" -DCMAKE_PROGRAM_PATH="$NEBULA_PROGRAM_DIRS" -DCMAKE_INSTALL_LIBDIR="lib" -DWITH_ZLIB=1 -DWITH_SNAPPY=1 -DWITH_ZSTD=1 -DWITH_GFLAGS=0 -DWITH_JEMALLOC=0 -DWITH_TESTS=off -DWITH_TOOLS=off .); then
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

