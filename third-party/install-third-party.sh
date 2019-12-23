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

version=7.5.0
url_base=https://nebula-graph.oss-accelerate.aliyuncs.com/third-party
this_dir=$(dirname $(readlink -f $0))

# We consider two derivatives: Red Hat and Debian
# Place preset libc versions of each from newer to older
libc_preset=( 2.17 2.12 )
libcxx_preset=( 3.4.26 3.4.25 3.4.23 )

selected_libc=
selected_libcxx=
selected_archive=
this_libc=$(ldd --version | head -1 | cut -d ')' -f 2 | cut -d ' ' -f 2)
this_libcxx=$($this_dir/cxx-compiler-libcxx-version.sh)
this_abi=$($this_dir/cxx-compiler-abi-version.sh)

# There is no reliable way to detect version of libcxx of RedHat devtoolset.
# So we back off to use the oldest usable version
[[ -z $this_libcxx ]] && this_libcxx=3.4.23

hash wget &>/dev/null || {
    echo "'wget' not fould, please install it first" 1>&2
    exit 1
}

download_cmd="wget -c"
wget --help | grep -q '\--show-progress' && \
         download_cmd="$download_cmd -q --show-progress" || \
         download_cmd="$download_cmd --progress=bar:force:noscroll"

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
selected_libcxx=$(select_by_version $this_libcxx "${libcxx_preset[@]}")

[[ -z $selected_libc ]] && {
    echo "No prebuilt third-party found to download for your environment: libc-$this_libc, libcxx-$this_libcxx, ABI $this_abi_version" 1>&2
    echo "Please invoke $this_dir/build-third-party.sh to build manually" 1>&2
    exit 1
}

selected_archive=vesoft-third-party-x86_64-libc-$selected_libc-libcxx-$selected_libcxx-abi-$this_abi.sh

echo $selected_archive

url=$url_base/$selected_archive
$download_cmd $url
[[ $? -ne 0 ]] && {
    echo "Downloading $selected_archive failed" 1>&2
    exit 1
}

bash $selected_archive $@

rm -rf $selected_archive
