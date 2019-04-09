#!/bin/bash

source ../functions.sh

prepareBuild "zstd"

echo
echo "Start building $PROJECT_NAME with $NEBULA_C_COMPILER ($CC_VER_STR)"
echo

#if !(cd $SOURCE_DIR && NOCONFIGURE=1 ./autogen.sh); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

if (CC=$NEBULA_C_COMPILER   CFLAGS="-fPIC -DPIC  $EXTRA_CXXFLAGS"  LDFLAGS="-static-libgcc -static-libstdc++"    PREFIX=$INSTALL_PATH      make -e $@); then
    if [[ $SOURCE_DIR/lib/libzstd.a -nt $INSTALL_PATH/lib/libzstd.a ]]; then
        if (PREFIX=$INSTALL_PATH   make -e install); then
            cd $CURR_DIR
            rm -f $INSTALL_PATH/lib/libzstd.so*
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

