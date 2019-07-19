#!/bin/bash

source ../functions.sh

prepareBuild "folly"

double_conversion_release=$THIRD_PARTY_DIR/double-conversion/_install
libevent_release=$THIRD_PARTY_DIR/libevent/_install
gflags_release=$THIRD_PARTY_DIR/gflags/_install
glog_release=$THIRD_PARTY_DIR/glog/_install
zlib_release=$THIRD_PARTY_DIR/zlib/_install
zstd_release=$THIRD_PARTY_DIR/zstd/_install
snappy_release=$THIRD_PARTY_DIR/snappy/_install

echo
echo "Start building $PROJECT_NAME with $NEBULA_CXX_COMPILER ($CXX_VER_STR)"
echo

#if !(cd $SOURCE_DIR && autoreconf -ivf ); then
#    cd $CURR_DIR
#    echo
#    echo "### $PROJECT_NAME failed to auto-reconfigure ###"
#    echo
#    exit 1
#fi

cd $SOURCE_DIR

if [[ -n $NEBULA_GETTIME_NEEDS_POSIX_MACRO ]]; then
    EXTRA_CXXFLAGS="$EXTRA_CXXFLAGS -DFOLLY_HAVE_CLOCK_GETTIME -D__USE_POSIX199309"
fi

compiler_flags="-Wno-array-bounds -Wno-class-memaccess -fPIC -DPIC -DFOLLY_HAVE_LIBDWARF_DWARF_H -DFOLLY_HAVE_MEMRCHR -Wno-noexcept-type -Wno-error=parentheses -Wno-error=shadow=compatible-local  $EXTRA_CXXFLAGS"
exe_linker_flags="-static-libgcc -static-libstdc++ $EXTRA_LDFLAGS"
NEBULA_INCLUDE_DIRS="$double_conversion_release/include;$libevent_release/include;$gflags_release/include;$glog_release/include;$zstd_release/include;$zlib_release/include;$snappy_release/include;$NEBULA_INCLUDE_DIRS"
NEBULA_LIB_DIRS="$double_conversion_release/lib;$libevent_release/lib;$gflags_release/lib;$glog_release/lib;$zstd_release/lib;$zlib_release/lib;$snappy_release/lib;$NEBULA_LIB_DIRS"

if (( $CXX_MAJOR_VER > 8 || $CXX_MAJOR_VER == 8 && $CXX_MINOR_VER > 2 || $CXX_MAJOR_VER == 8 && $CXX_MINOR_VER == 2 && $CXX_RELEASE_VER > 0 )); then
    compiler_flags="$compiler_flags -Wno-error=stringop-truncation"
fi

if [[ $SOURCE_DIR/CMakeLists.txt -nt $SOURCE_DIR/Makefile ||
      $CURR_DIR/build.sh -nt $SOURCE_DIR/Makefile ]]; then
    if !($NEBULA_CMAKE $CMAKE_FLAGS -DCMAKE_C_FLAGS:STRING="$compiler_flags" -DCMAKE_CXX_FLAGS:STRING="$compiler_flags" -DCMAKE_INCLUDE_PATH="$NEBULA_INCLUDE_DIRS" -DCMAKE_LIBRARY_PATH="$NEBULA_LIB_DIRS" -DCMAKE_EXE_LINKER_FLAGS:STRING="$exe_linker_flags" -DBoost_USE_STATIC_LIBS:BOOL=YES     $SOURCE_DIR); then
        cd $CURR_DIR
        echo
        echo "### $PROJECT_NAME failed to configure the build ###"
        echo
        exit 1
    fi
fi

if (make $@ install); then
    cd $CURR_DIR
    rm -f $INSTALL_PATH/lib/*.la
    echo
    echo ">>> $PROJECT_NAME is built and installed successfully <<<"
    echo
    exit 0
else
    cd $CURR_DIR
    echo
    echo "### $PROJECT_NAME failed to build ###"
    echo
    exit 1
fi

