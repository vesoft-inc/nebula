#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# Usage: build-cmake.sh [prefix]

# Always use bash
shell=$(basename $(readlink /proc/$$/exe))
if [ ! x$shell = x"bash" ]
then
    bash $0 $@
    exit $?
fi

archive=cmake-3.15.5-Linux-x86_64.sh
url=https://cmake.org/files/v3.15/$archive
prefix=`pwd`/cmake-3.15.5

if [[ -n $1 ]]
then
    prefix=$1
fi

if [[ -f $archive ]]
then
    checksum=$(md5sum $archive | cut -d ' ' -f 1)
fi

if [[ ! $checksum = 35d56e9c27b4fd2819a11c29320c655a ]]
then
    hash wget &> /dev/null && download_cmd="wget -c"
    hash axel &> /dev/null && download_cmd="axel -a -n 16"
    if [[ -z $download_cmd ]]
    then
        echo "Neither 'wget' nor 'axel' available for downloading" 1>&2;
        exit 1;
    fi

    echo "Downloading with $download_cmd..."
    if ! bash -c "$download_cmd $url"
    then
        echo "Download failed."
        exit 1
    fi
fi

mkdir -p $prefix
bash $archive --prefix=$prefix &> /dev/null <<EOF
yes
no
EOF

echo "CMake has been installed to prefix=$prefix"
