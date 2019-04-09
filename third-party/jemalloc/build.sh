#!/bin/bash

source ../functions.sh

prepareBuild "jemalloc"

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

#cd $CURR_DIR
#rm -fr .build && mkdir .build && cd .build

cd $SOURCE_DIR

if [[ -n $NEBULA_LIBUNWIND_ROOT ]]; then
    EXTRA_LDFLAGS="-L$NEBULA_LIBUNWIND_ROOT/lib    $EXTRA_LDFLAGS"
fi

if [[ $SOURCE_DIR/configure.ac -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$NEBULA_C_COMPILER CPP="$NEBULA_C_COMPILER -E" CXX=$NEBULA_CXX_COMPILER    CXXFLAGS="-fPIC -DPIC    $EXTRA_CXXFLAGS" CFLAGS=$CXXFLAGS    LDFLAGS="-static-libgcc -static-libstdc++     $EXTRA_LDFLAGS"        $SOURCE_DIR/autogen.sh --enable-autogen --disable-valgrind --enable-stats --enable-prof --enable-prof-libunwind --enable-prof-gcc --enable-prof-libgcc --prefix=$INSTALL_PATH); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@); then
    if [[ $SOURCE_DIR/lib/libjemalloc.a -nt $INSTALL_PATH/lib/libjemalloc.a ||
          $SOURCE_DIR/lib/libjemalloc_pic.a -nt $INSTALL_PATH/lib/libjemalloc_pic.a ]]; then
        if (make install_bin install_include install_lib); then
            cd $CURR_DIR
            rm -f $INSTALL_PATH/lib/libjemalloc.so*
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

