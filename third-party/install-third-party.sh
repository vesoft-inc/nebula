#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# Usage: install-third-party.sh --prefix=/opt/vesoft/third-party

# Always use bash
shell=$(basename $(readlink /proc/$$/exe))
if [ ! x$shell = x"bash" ]
then
    bash $0 $@
    exit $?
fi

[[ $(uname) = Linux ]] || {
    echo "Only Linux is supported"
    exit 1
}

url_base=https://nebula-graph.oss-accelerate.aliyuncs.com/third-party
this_dir=$(dirname $(readlink -f $0))
cxx_cmd=${CXX:-g++}

# We consider two derivatives: Red Hat and Debian
# Place preset libc versions of each from newer to older
libc_preset=( 2.27 2.23 2.17 2.12 )
gcc_preset=( 9.2.0 9.1.0 8.3.0 7.5.0 7.1.0 )

selected_libc=
selected_gcc=
selected_archive=
this_libc=$(ldd --version | head -1 | cut -d ')' -f 2 | cut -d ' ' -f 2)
this_gcc=$($cxx_cmd -dumpfullversion -dumpversion)
this_abi=$($this_dir/cxx-compiler-abi-version.sh)

hash wget &>/dev/null || {
    echo "'wget' not fould, please install it first" 1>&2
    exit 1
}

download_cmd="wget -c"
if [[ -t 1 ]]
then
    wget --help | grep -q '\--show-progress' && \
            download_cmd="$download_cmd -q --show-progress" || \
            download_cmd="$download_cmd --progress=bar:force:noscroll"
else
    download_cmd="$download_cmd -q"
fi

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

# Find the maximum version not greater than the system one
function select_by_version {
    local this_version=$1
    shift 1
    local candidates="$@"
    for v in $candidates
    do
        if [[ $(version_cmp $v $this_version) -le 0 ]]
        then
            echo $v
            break
        fi
    done
}

selected_libc=$(select_by_version $this_libc "${libc_preset[@]}")
selected_gcc=$(select_by_version $this_gcc "${gcc_preset[@]}")

[[ -z $selected_libc ]] && {
    echo "No prebuilt third-party found to download for your environment: libc-$this_libc, GCC-$this_gcc, ABI $this_abi_version" 1>&2
    echo "Please invoke $this_dir/build-third-party.sh to build manually" 1>&2
    exit 1
}

selected_archive=vesoft-third-party-x86_64-libc-$selected_libc-gcc-$selected_gcc-abi-$this_abi.sh

url=$url_base/$selected_archive
echo "Downloading $selected_archive..."
$download_cmd $url
[[ $? -ne 0 ]] && {
    echo "Downloading $selected_archive failed" 1>&2
    exit 1
}

bash $selected_archive $@

rm -rf $selected_archive
