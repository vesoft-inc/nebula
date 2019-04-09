#!/bin/bash

source ../functions.sh

prepareBuild "bzip2"

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

#cd $CURR_DIR
#rm -fr .build && mkdir .build && cd .build

cd $SOURCE_DIR

if (CC=$NEBULA_C_COMPILER   make -e $@); then
    if [[ $SOURCE_DIR/libbz2.a -nt $INSTALL_PATH/lib/libbz2.a ]]; then
        if (make install PREFIX=$INSTALL_PATH); then
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

