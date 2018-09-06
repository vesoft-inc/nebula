#!/bin/sh

source ../functions.sh

prepareBuild "rocksdb"

BOOST_RELEASE=$TOOLS_ROOT/boost
OPENSSL_RELEASE=$TOOLS_ROOT/openssl

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

if !($CMAKE_ROOT/bin/cmake $CMAKE_FLAGS -DBOOST_ROOT=$TOOLS_ROOT/boost -DOPENSSL_ROOT_DIR=$TOOLS_ROOT/openssl .); then
    cd $CURR_DIR
    echo
    echo "### $PROJECT_NAME failed to install ###"
    echo
    exit 1
fi

if (PLATFORM=OS_LINUX      CCFLAGS="-DROCKSDB_PLATFORM_POSIX -DROCKSDB_SUPPORT_THREAD_LOCAL -DOS_LINUX -fno-builtin-memcmp -DZLIB -DBZIP2 -DSNAPPY -DZSTD -DROCKSDB_MALLOC_USABLE_SIZE -march=native -fPIC -DPIC -Wno-error=shadow  -I$INSTALL_PATH/include   $EXTRA_CXXFLAGS"             CXXFLAGS=$CCFLAGS                  PLATFORM_LDFLAGS="-static-libstdc++ -static-libgcc -L$INSTALL_PATH/lib   $EXTRA_LDFLAGS -lz -lbz2 -lsnappy -lzstd"                     JAVA_LDFLAGS=$PLATFORM_LDFLAGS      DEBUG_LEVEL=0         make $@); then
    if [[ $SOURCE_DIR/librocksdb.a -nt $INSTALL_PATH/lib/librocksdb.a ]]; then
        echo "===> Ready to install"
        if (INSTALL_PATH=$INSTALL_PATH   make install); then
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

