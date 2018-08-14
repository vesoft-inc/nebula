#!/bin/sh

source ../functions.sh

prepareBuild "googletest"

# Build gtest first
if !($CURR_DIR/build-gtest.sh "gtest" "$CURR_DIR" "$THIRD_PARTY_DIR" "$TOOLS_ROOT" "$SOURCE_DIR/googletest" "$INSTALL_PATH" "$GCC_ROOT" "$CMAKE_ROOT" "$FLEX_ROOT" "$GCC_VER" "$EXTRA_CXXFLAGS" "$EXTRA_LDFLAGS" $@); then
    exit 1
fi

# Then build gmock
if !($CURR_DIR/build-gmock.sh "gmock" "$CURR_DIR" "$THIRD_PARTY_DIR" "$TOOLS_ROOT" "$SOURCE_DIR/googlemock" "$INSTALL_PATH" "$GCC_ROOT" "$CMAKE_ROOT" "$FLEX_ROOT" "$GCC_VER" "$EXTRA_CXXFLAGS" "$EXTRA_LDFLAGS" $@); then
    exit 1
fi

exit 0
