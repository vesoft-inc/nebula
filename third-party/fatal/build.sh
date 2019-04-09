#!/bin/bash

# fatal is a haeder library, no need tp build, we just need to copy to the destination, so that we could build the rpm package

source ../functions.sh

prepareBuild "fatal"

echo
echo Start installing $PROJECT_NAME
echo

mkdir -p $INSTALL_PATH/include
#cp -r $SOURCE_DIR/demo $INSTALL_PATH
#cp -r $SOURCE_DIR/docs $INSTALL_PATH
cp -r $SOURCE_DIR/fatal $INSTALL_PATH/include/.
#cp -r $SOURCE_DIR/lesson $INSTALL_PATH

echo
echo ">>> $PROJECT_NAME is installed successfully <<<"
echo

exit 0

