#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# Usage: build-gcc.sh [prefix]

# Always use bash
shell=$(basename $(readlink /proc/$$/exe))
if [ ! x$shell = x"bash" ]
then
    bash $0 $@
    exit $?
fi

this_dir=$(dirname $(readlink -f $0))

# GCC, binutils and support libraries
#url_base=http://ftpmirror.gnu.org
url_base=http://mirrors.ustc.edu.cn/gnu

gcc_version=7.5.0
gcc_tarball=gcc-$gcc_version.tar.xz
gcc_url=$url_base/gcc/gcc-$gcc_version/$gcc_tarball

gmp_version=5.1.3
gmp_tarball=gmp-$gmp_version.tar.xz
gmp_url=$url_base/gmp/$gmp_tarball

mpfr_version=3.1.4
mpfr_tarball=mpfr-$mpfr_version.tar.xz
mpfr_url=$url_base/mpfr/$mpfr_tarball

mpc_version=1.0.3
mpc_tarball=mpc-$mpc_version.tar.gz
mpc_url=$url_base/mpc/$mpc_tarball

bu_version=2.28.1
bu_tarball=binutils-$bu_version.tar.xz
bu_url=$url_base/binutils/$bu_tarball

gcc_checksum=79cb8a65d44dfc8a2402b46395535c9a
gmp_checksum=e5fe367801ff067b923d1e6a126448aa
mpfr_checksum=064b2c18185038e404a401b830d59be8
mpc_checksum=d6a1d5f8ddea3abd2cc3e98f58352d26
bu_checksum=a3bf359889e4b299fce1f4cb919dc7b6

# Building directories setup
cur_dir=$PWD
root_dir=$PWD/toolset-build
tarballs_dir=$root_dir/downloads
source_dir=$root_dir/source
gcc_object_dir=$root_dir/gcc-build
bu_object_dir=$root_dir/binutils-build
prefix=$1
install_dir=${prefix:-$root_dir/install}/vesoft/toolset/gcc/$gcc_version
logfile=$root_dir/gcc-build.log
triplet=$(uname -m)-vesoft-linux
distro=$(lsb_release -si)

# Guess number of building jobs
source $this_dir/guess-building-jobs-num.sh
echo "Building GCC-$gcc_version with $building_jobs_num jobs"

# Download source tarballs
function get_checksum {
    md5sum $1 | cut -d ' ' -f 1
}

# args: <download command> <tarball> <url> <checksum>
function fetch_tarball {
    local checksum
    [[ -f $2 ]] && checksum=$(get_checksum $2)
    [[ -n $checksum ]] && [[ $checksum = $4 ]] && return 0
    echo "Downloading $2..."
    if ! bash -c "$1 $3"
    then
        echo "Download $2 Failed"
        exit 1
    fi
}

function fetch_tarballs {
    hash wget &> /dev/null && download_cmd="wget -c --progress=bar:force:noscroll"
    if [[ -z $download_cmd ]]
    then
        echo "'wget' not found for downloading" 1>&2;
        exit 1;
    fi
    set +e
    set +o pipefail
    wget --help | grep -q '\--show-progress' && \
            download_cmd="$download_cmd -q --show-progress"
    set -e
    set -o pipefail
    echo "Download command: '$download_cmd'"

    mkdir -p $tarballs_dir && cd $tarballs_dir

    fetch_tarball "$download_cmd" $gcc_tarball $gcc_url $gcc_checksum
    fetch_tarball "$download_cmd" $gmp_tarball $gmp_url $gmp_checksum
    fetch_tarball "$download_cmd" $mpfr_tarball $mpfr_url $mpfr_checksum
    fetch_tarball "$download_cmd" $mpc_tarball $mpc_url $mpc_checksum
    fetch_tarball "$download_cmd" $bu_tarball $bu_url $bu_checksum

    cd $OLDPWD
}

# Unpack source tarballs
function unpack_tarballs {
    mkdir -p $source_dir
    cd $tarballs_dir

    if [[ ! -d $source_dir/gcc-$gcc_version ]]
    then
        echo "Unpacking $gcc_tarball..."
        tar -xf $gcc_tarball -C $source_dir || exit 1
    fi

    if [[ ! -d $source_dir/gmp-$gmp_version ]]
    then
        echo "Unpacking $gmp_tarball..."
        tar -xf $gmp_tarball -C $source_dir || exit 1
    fi

    if [[ ! -d $source_dir/mpfr-$mpfr_version ]]
    then
        echo "Unpacking $mpfr_tarball..."
        tar -xf $mpfr_tarball -C $source_dir || exit 1
    fi

    if [[ ! -d $source_dir/mpc-$mpc_version ]]
    then
        echo "Unpacking $mpc_tarball..."
        tar -xf $mpc_tarball -C $source_dir || exit 1
    fi

    if [[ ! -d $source_dir/binutils-$bu_version ]]
    then
        echo "Unpacking $bu_tarball..."
        tar -xf $bu_tarball -C $source_dir || exit 1
    fi

    cd $OLDPWD
}

# Necessary dependency setup
function setup_deps {
    cd $source_dir/gcc-$gcc_version
    ln -sf ../gmp-$gmp_version gmp
    ln -sf ../mpfr-$mpfr_version mpfr
    ln -sf ../mpc-$mpc_version mpc

    #[[ ! -e config.guess.orig ]] && cp -vp config.guess config.guess.orig
    #cat > config.guess <<EOF
##! /usr/bin/env bash
#this_dir=\$(dirname \$0)
#\$this_dir/config.guess.orig | sed -r 's/-unknown-|-pc-/-vesoft-/'
#EOF
    #chmod +x config.guess

    cd $OLDPWD

    # Tweak GMP to not build with native code
    cd $source_dir/gmp-$gmp_version
    cp -f configfsf.guess config.guess
    cp -f configfsf.sub config.sub

    cd $OLDPWD
}

# Configure GCC
function configure_gcc {
    mkdir -p $gcc_object_dir
    cd $gcc_object_dir
    $source_dir/gcc-$gcc_version/configure      \
        --prefix=$install_dir                   \
        --with-pkgversion="Nebula Graph Build"  \
        --enable-shared                         \
        --enable-threads=posix                  \
        --enable-__cxa_atexit                   \
        --enable-clocale=gnu                    \
        --enable-languages=c,c++                \
        --enable-lto                            \
        --enable-bootstrap                      \
        --disable-nls                           \
        --disable-multilib                      \
        --disable-install-libiberty             \
        --with-system-zlib                      \
        --build=$triplet                        \
        --host=$triplet                         \
        --target=$triplet                       \
        --disable-werror
    [[ $? -ne 0 ]] && { echo "Failed to configure GCC" 1>&2;  exit 1; }
    cd $OLDPWD
}

# Start building GCC
function build_gcc {
    cd $gcc_object_dir
    make -s -j $building_jobs_num bootstrap-lean
    [[ $? -ne 0 ]] && { echo "Failed to build GCC" 1>&2; exit 1; }
    cd $OLDPWD
}

# Install GCC
function install_gcc {
    cd $gcc_object_dir
    make -s -j $building_jobs_num install-strip
    [[ $? -ne 0 ]] && { echo "Failed to install GCC" 1>&2; exit 1; }
    cd $OLDPWD
}

# Clean GCC
function clean_gcc {
    cd $gcc_object_dir
    make -s -j $building_jobs_num clean || true
    cd $OLDPWD
}

# Configure binutils
function configure_binutils {
    mkdir -p $bu_object_dir
    cd $bu_object_dir
    $source_dir/binutils-$bu_version/configure  \
        --prefix=$install_dir                   \
        --with-pkgversion="Nebula Graph Build"  \
        --disable-shared                        \
        --disable-nls                           \
        --enable-gold                           \
        --enable-plugins                        \
        --enable-ld=default                     \
        --with-system-zlib                      \
        --build=$triplet                        \
        --host=$triplet                         \
        --target=$triplet                       \
        --disable-werror
    [[ $? -ne 0 ]] && { echo "Failed to configure binutils" 1>&2; exit 1; }
    cd $OLDPWD
}

# Build binutils
function build_binutils {
    cd $bu_object_dir
    make -s -j $building_jobs_num || { echo "Failed to build binutils" 1>&2; exit 1; }
    cd $OLDPWD
}

# Install binutils
function install_binutils {
    cd $bu_object_dir
    make -s install-strip || { echo "Failed to install binutils" 1>&2; exit 1; }
    cd $OLDPWD
    cd $install_dir
    # Place a copy of assembler and linker to libexec
    cp -vp bin/as libexec/gcc/$triplet/$gcc_version
    cp -vp bin/ld* libexec/gcc/$triplet/$gcc_version
    cd $OLDPWD
}

# Clean binutils
function clean_binutils {
    cd $bu_object_dir
    make -s clean || true
    cd $OLDPWD
}

# Finalize the building
function finalize {
    # Remove all of the annoying libtool files,
    # so that the installation could be copied around
    find $install_dir -name '*.la' | xargs rm -f
}

# Test usability of GCC
function usability_test {
    CXX=$install_dir/bin/g++ $this_dir/cxx-compiler-usability-test.sh
    [[ $? -eq 0 ]] || exit 1
    cp -p $this_dir/cxx-compiler-usability-test.sh $install_dir
}

# Build a self-extractable package
function make_package {
    set +e
    glibc_version=$(ldd --version | head -1 | cut -d ' ' -f4 | cut -d '-' -f1)
    set -e
    exec_file=$root_dir/vesoft-gcc-$gcc_version-$distro-$(uname -m)-glibc-$glibc_version.sh
    echo "Creating self-extractable package $exec_file"
    cat > $exec_file <<EOF
#! /usr/bin/env bash
set -e

[[ \$# -ne 0 ]] && prefix=\$(echo "\$@" | sed 's;.*--prefix=(\S*).*;\1;' -r)
prefix=\${prefix:-/opt}/vesoft/toolset

hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }

mkdir -p \$prefix
[[ -w \$prefix ]] || { echo "\$prefix: No permission to write"; exit 1; }

archive_offset=\$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' \$0)
tail -n+\$archive_offset \$0 | tar --numeric-owner -xJf - -C \$prefix

echo "GCC-$gcc_version has been installed to \$prefix/gcc/$gcc_version"
echo "Performing usability tests"
CXX=\$prefix/gcc/$gcc_version/bin/g++ \$prefix/gcc/$gcc_version/cxx-compiler-usability-test.sh
echo "Run 'source \$prefix/gcc/$gcc_version/enable' to start using."
echo "Run 'source \$prefix/gcc/$gcc_version/disable' to stop using."

exit 0

__start_of_archive__
EOF
    cd $install_dir/../..
    tar -cJf - gcc/$gcc_version >> $exec_file
    chmod 0755 $exec_file
    cd $OLDPWD
}

start_time=$(date +%s)
set -e
set -o pipefail
echo "Starting build"
trap '[[ $? -ne 0 ]] && echo "Building failed, see $logfile for more details." 1>&2' EXIT
mkdir -p $root_dir

fetch_tarballs
{
    unpack_tarballs
    setup_deps
}   |& tee $logfile

{
    configure_gcc
    build_gcc
    install_gcc
    clean_gcc

    configure_binutils
    build_binutils
    install_binutils
    clean_binutils

    finalize
}   |& tee -a $logfile \
    | grep --line-buffered '^Making\|^Configuring\|^Comparing\|^Comparison\|^Failed to'

usability_test

cat > $install_dir/enable <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
[[ ":\$PATH:" =~ ":\$this_path/bin:" ]] || export PATH=\$this_path/bin:\$PATH
export OLD_CC=\$CC
export OLD_CXX=\$CXX
export CC=\$this_path/bin/gcc
export CXX=\$this_path/bin/g++
hash -r
echo "Only PATH was setup so as not to pollute your library path"
echo "You could run 'export LD_LIBRARY_PATH=\$this_path/lib64:\\\$LD_LIBRARY_PATH' if needed"
EOF

cat > $install_dir/disable <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
export PATH=\$(echo \$PATH | sed "s#\$this_path/bin:##")
export CC=\$OLD_CC
export CXX=\$OLD_CXX
[ -z \$CC ] && unset CC
[ -z \$CXX ] && unset CXX
hash -r
EOF

make_package
end_time=$(date +%s)

echo "GCC-$gcc_version has been installed to $install_dir"
echo "$((end_time - start_time)) seconds been taken."
echo "Run 'source $install_dir/enable' to start using."
echo "Run 'source $install_dir/disable' to disable it."
