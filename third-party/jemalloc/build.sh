#!/bin/sh

source ../functions.sh

prepareBuild "jemalloc"

libunwind_release=$TOOLS_ROOT/libunwind

echo
echo Start building $PROJECT_NAME with gcc-$GCC_VER
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

if [[ $SOURCE_DIR/configure.ac -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !(CC=$GCC_ROOT/bin/gcc CPP=$GCC_ROOT/bin/cpp CXX=$GCC_ROOT/bin/g++    CXXFLAGS="-fPIC -DPIC -I$libunwind_release/include   $EXTRA_CXXFLAGS" CFLAGS=$CXXFLAGS    LDFLAGS="-static-libgcc -static-libstdc++ -L$libunwind_release/lib  $EXTRA_LDFLAGS"        $SOURCE_DIR/autogen.sh --enable-autogen --disable-valgrind --enable-stats --enable-prof --enable-prof-libunwind --with-static-libunwind=$libunwind_release/lib/libunwind.a --enable-prof-gcc --enable-prof-libgcc --prefix=$INSTALL_PATH); then
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

