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

# Exit on any failure here after
set -e
set -o pipefail

# Directories setup
cur_dir=`pwd`
source_dir=$(readlink -f $(dirname $0)/..)/third-party
build_root=$cur_dir/third-party
build_dir=$build_root/build
install_dir=$build_root/install
download_dir=$build_root/downloads
source_tar_name=nebula-third-party-src-1.0.tgz
source_url=https://nebula-graph.oss-accelerate.aliyuncs.com/third-party/${source_tar_name}
logfile=$build_root/build.log

# Allow to customize compilers
[[ -n ${CC} ]] && C_COMPILER_ARG="-DCMAKE_C_COMPILER=${CC}"
[[ -n ${CXX} ]] && CXX_COMPILER_ARG="-DCMAKE_CXX_COMPILER=${CXX}"

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
        echo  "done"
    fi
else
    tar -xzf $source_tar_name
fi

# Build and install
mkdir -p $build_dir $install_dir
cd $build_dir

echo "Starting build, on any failure, please see $logfile"

cmake -DDOWNLOAD_DIR=$download_dir \
      -DCMAKE_INSTALL_PREFIX=$install_dir \
      ${C_COMPILER_ARG} ${CXX_COMPILER_ARG} \
      $source_dir |& tee $logfile

make |& \
         tee -a $logfile | \
         grep --line-buffered 'Creating\|^Scanning\|Performing\|Completed\|CMakeFiles.*Error'
end_time=$(date +%s)

cd $OLDPWD && rm -rf $build_dir

echo
echo "Third parties have been successfully installed to $install_dir"
echo "$((end_time - start_time)) seconds been taken."
