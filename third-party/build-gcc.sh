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

gcc_checksum=6069ae3737cf02bf2cb44a391ef0e937
gmp_checksum=f58fa8001d60c4c77595fbbb62b63c1d
mpfr_checksum=320fbc4463d4c8cb1e566929d8adc4f8
mpc_checksum=4125404e41e482ec68282a2e687f6c73
bu_checksum=0d174cdaf85721c5723bf52355be41e6

cur_dir=$PWD
build_dir=$PWD/gcc-build
tarballs_dir=$build_dir/downloads
source_dir=$build_dir/source
object_dir=$build_dir/build
prefix=$1
install_dir=${prefix:-$build_dir/install}/gcc/$gcc_version

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
    fetch_tarball "$download_cmd" $bu_tarball $bu_url $bu_checksum

    cd $OLDPWD
}

function unpack_tarballs {
    mkdir -p $source_dir
    cd $tarballs_dir
    set -e

    if [[ ! -d $source_dir/gcc-$gcc_version ]]
    then
        echo "Unpacking $gcc_tarball..."
        tar --skip-old-files -xf $gcc_tarball -C $source_dir
    fi

    if [[ ! -d $source_dir/gmp-$gmp_version ]]
    then
        echo "Unpacking $gmp_tarball..."
        tar --skip-old-files -xf $gmp_tarball -C $source_dir
    fi

    if [[ ! -d $source_dir/mpfr-$mpfr_version ]]
    then
        echo "Unpacking $mpfr_tarball..."
        tar --skip-old-files -xf $mpfr_tarball -C $source_dir
    fi

    if [[ ! -d $source_dir/mpc-$mpc_version ]]
    then
        echo "Unpacking $mpc_tarball..."
        tar --skip-old-files -xf $mpc_tarball -C $source_dir
    fi

    if [[ ! -d $source_dir/binutils-$bu_version ]]
    then
        echo "Unpacking $bu_tarball..."
        tar --skip-old-files -xf $bu_tarball -C $source_dir
    fi

    set +e
    cd $OLDPWD
}

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

    ln -sf ../binutils-$bu_version/binutils binutils
    ln -sf ../binutils-$bu_version/gas gas
    ln -sf ../binutils-$bu_version/gold gold
    ln -sf ../binutils-$bu_version/gprof gprof
    ln -sf ../binutils-$bu_version/ld ld
    ln -sf ../binutils-$bu_version/bfd bfd
    ln -sf ../binutils-$bu_version/opcodes opcodes

    cd $OLDPWD

    cd $source_dir/gmp-$gmp_version
    cp -f configfsf.guess config.guess
    cp -f configfsf.sub config.sub

    cd $OLDPWD
}

function configure_gcc {
    mkdir -p $object_dir
    cd $object_dir
    $source_dir/gcc-$gcc_version/configure --prefix=$install_dir                           \
                --with-pkgversion="Nebula Graph Build"          \
                --enable-shared                                 \
                --enable-threads=posix                          \
                --enable-__cxa_atexit                           \
                --enable-clocale=gnu                            \
                --enable-languages=c,c++                        \
                --enable-lto                                    \
                --disable-bootstrap                              \
                --disable-nls                                   \
                --disable-multilib                              \
                --disable-install-libiberty                     \
                --disable-werror                                \
                --with-system-readline                          \
                --with-system-zlib
                #--host=x86_64-vesoft-linux                                \
                #--build=x86_64-vesoft-linux                                \
    [[ $? -eq 0 ]] || exit 1
    cd $OLDPWD
}

function build_gcc {
    cd $object_dir
    make MAKEINFO=true -j 20  |& tee build.log
    [[ $? -ne 0 ]] && exit 1
    cd $OLDPWD
}

function install_gcc {
    cd $object_dir
    make MAKEINFO=true install-strip
    cd $OLDPWD
}

function finalize {
    find $install_dir -name '*.la' | xargs rm -f
}

function make_package {
    glibc_version=$(ldd --version | head -1 | cut -d ' ' -f4)
    exec_file=$build_dir/vesoft-gcc-$gcc_version-linux-x86_64-glibc-$glibc_version.sh
    echo "Creating self-extracting package $exec_file"
    cat > $exec_file <<EOF
#! /usr/bin/env bash
set -e

[[ \$# -ne 0 ]] && prefix=\$(echo "\$@" | sed 's;.*--prefix=(\S*).*;\1;' -r)
prefix=\${prefix:-/usr/local}

hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }

mkdir -p \$prefix
[[ -w \$prefix ]] || { echo "\$prefix: No permission to write"; exit 1; }

archive_offset=\$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' \$0)
tail -n+\$archive_offset \$0 | tar --numeric-owner -xJf - -C \$prefix

exit 0

__start_of_archive__
EOF
    cd $install_dir/../..
    tar -cJf - * >> $exec_file
    chmod 0755 $exec_file
    cd $OLDPWD
}

start_time=$(date +%s)
fetch_tarballs
unpack_tarballs
setup_deps
configure_gcc
build_gcc
install_gcc
finalize

cat > $install_dir/bin/enable-gcc.sh <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
[[ ":\$PATH:" =~ ":\$this_path:" ]] || export PATH=\$this_path:\$PATH
export OLD_CC=\$CC
export OLD_CXX=\$CXX
export CC=\$this_path/gcc
export CXX=\$this_path/g++
hash -r
EOF

cat > $install_dir/bin/disable-gcc.sh <<EOF
this_path=\$(dirname \$(readlink -f \$BASH_SOURCE))
export PATH=\$(echo \$PATH | sed "s#\$this_path:##")
export CC=\$OLD_CC
export CXX=\$OLD_CXX
hash -r
EOF

make_package
end_time=$(date +%s)

echo "GCC-$gcc_version has been installed to prefix=$install_dir"
echo "$((end_time - start_time)) seconds been taken."
echo "Run 'source $install_dir/bin/enable-gcc.sh' to make it ready to use."
echo "Run 'source $install_dir/bin/disable-gcc.sh' to disable it."
