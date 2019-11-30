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

url_base=http://ftpmirror.gnu.org

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

gcc_checksum=79cb8a65d44dfc8a2402b46395535c9a
gmp_checksum=e5fe367801ff067b923d1e6a126448aa
mpfr_checksum=064b2c18185038e404a401b830d59be8
mpc_checksum=d6a1d5f8ddea3abd2cc3e98f58352d26

cur_dir=$PWD
build_dir=$PWD/gcc-build
tarballs_dir=$build_dir/downloads
source_dir=$build_dir/source
prefix=$1
install_dir=${prefix:-$PWD/gcc-install}/gcc/$gcc_version

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
    hash wget &> /dev/null && download_cmd="wget -c"
    hash axel &> /dev/null && download_cmd="axel -a -n 8"
    if [[ -z $download_cmd ]]
    then
        echo "Neither 'wget' nor 'axel' available for downloading" 1>&2;
        exit 1;
    fi

    mkdir -p $tarballs_dir && cd $tarballs_dir

    fetch_tarball "$download_cmd" $gcc_tarball $gcc_url $gcc_checksum
    fetch_tarball "$download_cmd" $gmp_tarball $gmp_url $gmp_checksum
    fetch_tarball "$download_cmd" $mpfr_tarball $mpfr_url $mpfr_checksum
    fetch_tarball "$download_cmd" $mpc_tarball $mpc_url $mpc_checksum
    cd $OLDPWD
}

function unpack_tarballs {
    mkdir -p $source_dir
    cd $tarballs_dir
    set -e

    echo "Unpacking $gcc_tarball..."
    tar --skip-old-files -xf $gcc_tarball -C $source_dir

    echo "Unpacking $gmp_tarball..."
    tar --skip-old-files -xf $gmp_tarball -C $source_dir

    echo "Unpacking $mpfr_tarball..."
    tar --skip-old-files -xf $mpfr_tarball -C $source_dir

    echo "Unpacking $mpc_tarball..."
    tar --skip-old-files -xf $mpc_tarball -C $source_dir

    set +e
    cd $OLDPWD
}

function setup_deps {
    cd $source_dir/gcc-$gcc_version
    ln -sf ../gmp-$gmp_version gmp
    ln -sf ../mpfr-$mpfr_version mpfr
    ln -sf ../mpc-$mpc_version mpc

    cd $source_dir/gmp-$gmp_version
    cp -f configfsf.guess config.guess
    cp -f configfsf.sub config.sub

    cd $OLDPWD
}

function configure_gcc {
	cd $source_dir/gcc-$gcc_version
	./configure --prefix=$install_dir               \
				--enable-shared                     \
				--with-libc-version=2.18            \
				--enable-threads=posix              \
				--enable-__cxa_atexit               \
				--enable-clocale=gnu                \
				--enable-languages=c,c++            \
				--enable-lto                        \
				--enable-bootstrap                  \
				--disable-nls                       \
				--disable-multilib                  \
				--disable-install-libiberty         \
				--disable-werror                    \
				--with-system-zlib
    [[ $? -eq 0 ]] || exit 1
    cd $OLDPWD
}

function build_gcc {
    cd $source_dir/gcc-$gcc_version
    make -j 20 || exit 1
    cd $OLDPWD
}

function install_gcc {
    cd $source_dir/gcc-$gcc_version
    make -j install-strip || exit 1
    cd $OLDPWD
}

start_time=$(date +%s)
fetch_tarballs
unpack_tarballs
setup_deps
configure_gcc
build_gcc
install_gcc
end_time=$(date +%s)

cat > $install_dir/bin/setup-env.sh <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
[[ ":\$PATH:" =~ ":\$this_path:" ]] || export PATH=\$this_path:\$PATH
export OLD_CC=\$CC
export OLD_CXX=\$CXX
export CC=\$this_path/gcc
export CXX=\$this_path/g++
hash -r
EOF

cat > $install_dir/bin/restore-env.sh <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
export PATH=\$(echo \$PATH | sed "s#\$this_path:##")
export CC=\$OLD_CC
export CXX=\$OLD_CXX
hash -r
EOF

echo "GCC-$gcc_version has been installed to prefix=$install_dir"
echo "$((end_time - start_time)) seconds been taken."
echo "Run 'source $install_dir/bin/setup-env.sh' to make it ready to use."
echo "Run 'source $install_dir/bin/restore-env.sh' to disable it."
