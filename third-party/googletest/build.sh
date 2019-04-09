#!/bin/bash

source ../functions.sh

prepareBuild "googletest"

export THIRD_PARTY_DIR=$THIRD_PARTY_DIR
export GCC_VER=$GCC_VER
export EXTRA_CXXFLAGS=$EXTRA_CXXFLAGS
export EXTRA_LDFLAGS=$EXTRA_LDFLAGS
export INSTALL_PATH=$INSTALL_PATH
export CURR_DIR=$CURR_DIR

# Build gtest first
if !($CURR_DIR/build-gtest.sh "gtest" "$SOURCE_DIR/googletest" $@); then
    exit 1
fi

# Then build gmock
if !($CURR_DIR/build-gmock.sh "gmock" "$SOURCE_DIR/googlemock" $@); then
    exit 1
fi

exit 0
