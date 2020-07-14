#! /usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


# Use CC and CXX environment variables to use custom compilers.

start_time=$(date +%s)

# Always use bash
shell=$(basename $(readlink /proc/$$/exe))
if [ ! x$shell = x"bash" ]
then
    bash $0 $@
    exit $?
fi

this_dir=$(dirname $(readlink -f $0))

# CMake and GCC version checking
function version_cmp {
    mapfile -t left < <( echo $1 | tr . '\n' )
    mapfile -t right < <( echo $2 | tr . '\n')
    local i
    for i in ${!left[@]}
    do
        local lv=${left[$i]}
        local rv=${right[$i]}
        [[ -z $rv ]] && { echo $lv; return; }
        [[ $lv -ne $rv ]] && { echo $((lv - rv)); return; }
    done
    ((i++))
    rv=${right[$i]}
    [[ ${#right[@]} -gt ${#left[@]} ]] && { echo $((0-rv)); return; }
}

function check_cmake {
    hash cmake &> /dev/null || { echo "No cmake found." 1>&2; exit 1; }
    local cmake_version=$(cmake --version | head -1 | cut -d ' ' -f 3)
    local least_cmake_version=3.5.0
    if [[ $(version_cmp $cmake_version $least_cmake_version) -lt 0 ]]
    then
        echo "cmake $least_cmake_version or higher required" 1>&2
        exit 1
    fi
}

function check_cxx {
    # TODO To consider clang++
    local cxx_cmd
    hash g++ &> /dev/null && cxx_cmd=g++
    [[ -n $CXX ]] && cxx_cmd=$CXX
    [[ -z $cxx_cmd ]] && { echo "No C++ compiler found" 1>&2; exit 1; }
    cxx_version=$($cxx_cmd -dumpfullversion -dumpversion 2>/dev/null)
    local least_cxx_version=7.0.0
    if [[ $(version_cmp $cxx_version $least_cxx_version) -lt 0 ]]
    then
        echo "g++ $least_cxx_version or higher required, but you have $cxx_version" 1>&2
        exit 1
    fi
}

check_cmake
check_cxx

# Directories setup
cur_dir=`pwd`
source_dir=$(readlink -f $(dirname $0)/..)/third-party
build_root=$cur_dir/third-party
build_dir=$build_root/build
prefix=$1
install_dir=${prefix:-$build_root/install}
download_dir=$build_root/downloads
source_tar_name=nebula-third-party-src-1.0.tgz
source_url=https://oss-cdn.nebula-graph.com.cn/third-party/${source_tar_name}
logfile=$build_root/build.log
cxx_cmd=${CXX:-g++}
gcc_version=$(${CXX:-g++} -dumpfullversion -dumpversion)
abi_version=$($this_dir/cxx-compiler-abi-version.sh)
libc_version=$(ldd --version | head -1 | cut -d ' ' -f4 | cut -d '-' -f1)

# Exit on any failure here after
set -e
set -o pipefail


trap '[[ $? -ne 0 ]] && echo "Building failed, see $logfile for more details." 1>&2' EXIT

# Allow to customize compilers
[[ -n ${CC} ]] && C_COMPILER_ARG="-DCMAKE_C_COMPILER=${CC}"
[[ -n ${CXX} ]] && CXX_COMPILER_ARG="-DCMAKE_CXX_COMPILER=${CXX}"
[[ ${disable_cxx11_abi} -ne 0 ]] && DISABLE_CXX11_ABI="-DDISABLE_CXX11_ABI=1"
export disable_cxx11_abi

# Download source archives if necessary
mkdir -p $build_root
cd $build_root

if [[ -f $source_tar_name ]]
then
    checksum=$(md5sum $source_tar_name | cut -d ' ' -f 1)
fi

if [[ ! $checksum = 375f349b7b5ae1212bd4195bfc30f43a ]]
then
    hash wget &> /dev/null && download_cmd="wget -c"
    if [[ -z $download_cmd ]]
    then
        echo "'wget' not found for downloading" 1>&2;
    elif ! bash -c "$download_cmd $source_url"
    then
        # Resort to the builtin download method of cmake on failure
        echo "Download from $source_url failed." 1>&2
    else
        echo "Source of third party was downdloaded to $build_root"
        echo -n "Extracting into $download_dir..."
        tar -xzf $source_tar_name
        echo "done"
    fi
else
    tar -xzf $source_tar_name
fi

# Build and install
mkdir -p $build_dir $install_dir
cd $build_dir

echo "Starting build"

cmake -DDOWNLOAD_DIR=$download_dir              \
      -DCMAKE_INSTALL_PREFIX=$install_dir       \
      ${C_COMPILER_ARG} ${CXX_COMPILER_ARG}     \
      ${DISABLE_CXX11_ABI}                      \
      $source_dir |& tee $logfile

make |& \
         tee -a $logfile | \
        { grep --line-buffered 'Creating\|^Scanning\|Performing\|Completed\|CMakeFiles.*Error' || true; }
end_time=$(date +%s)

cd $OLDPWD && rm -rf $build_dir

# Remove all libtool files
find $install_dir -name '*.la' | xargs rm -f

# Make krb5 relocatable
sed -i 's/^prefix=.*$/prefix=$(dirname $(dirname $(readlink -f $0)))/' $install_dir/bin/krb5-config
sed -i 's#^LDFLAGS=.*$#LDFLAGS="-L$prefix/lib -L$prefix/lib64"#' $install_dir/bin/krb5-config
sed -i -r 's#^DEFCKTNAME=.*(/var.*keytab).*#DEFCKTNAME="FILE:$prefix\1"#' $install_dir/bin/krb5-config

cat > $install_dir/version-info <<EOF
Package         : Nebula Third Party
glibc           : $libc_version
Arch            : x86_64
Compiler        : GCC $gcc_version
C++ ABI         : $abi_version
Vendor          : VEsoft Inc.
EOF

function make_package {
    exec_file=$build_root/vesoft-third-party-x86_64-libc-$libc_version-gcc-$gcc_version-abi-$abi_version.sh

    echo "Creating self-extractable package $exec_file"
    cat > $exec_file <<EOF
#! /usr/bin/env bash
set -e

hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }

[[ \$# -ne 0 ]] && prefix=\$(echo "\$@" | sed 's;.*--prefix=(\S*).*;\1;p' -rn)
prefix=\${prefix:-/opt/vesoft/third-party}
mkdir -p \$prefix

[[ -w \$prefix ]] || { echo "\$prefix: No permission to write"; exit 1; }

archive_offset=\$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' \$0)
tail -n+\$archive_offset \$0 | tar --no-same-owner --numeric-owner -xJf - -C \$prefix

echo "Nebula Third Party has been installed to \$prefix"

exit 0

__start_of_archive__
EOF
    cd $install_dir
    tar -cJf - * >> $exec_file
    chmod 0755 $exec_file
    cd $OLDPWD
}

[[ $build_package -ne 0 ]] && make_package

echo
echo "Third parties have been successfully installed to $install_dir"
echo "$((end_time - start_time)) seconds been taken."
