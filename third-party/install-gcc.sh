#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# Usage: install-gcc.sh --prefix=/opt/nebula/toolset

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
url_base=https://oss-cdn.nebula-graph.com.cn/toolset
this_dir=$(dirname $(readlink -f $0))

# We consider two derivatives: Red Hat and Debian
# Place preset libc versions of each from higher to lower
CentOS_libc_preset_version=( 2.17 2.12 )
Debian_libc_preset_version=( 2.19 2.13 )

selected_distro=
selected_libc_version=
selected_archive=
this_distro=$(lsb_release -si)
this_libc_version=$(ldd --version | head -1 | cut -d ')' -f 2 | cut -d ' ' -f 2)

hash wget &>/dev/null || {
    echo "'wget' not fould, please install it first" 1>&2
    exit 1
}

download_cmd="wget -c"
wget --help | grep -q '\--show-progress' && \
         download_cmd="$download_cmd -q --show-progress" || \
         download_cmd="$download_cmd --progress=bar:force:noscroll"

# Guess the root distro
[[ "$this_distro" = CentOS ]] && selected_distro=CentOS
[[ "$this_distro" = RedHat ]] && selected_distro=CentOS
[[ "$this_distro" = Fedora ]] && selected_distro=CentOS
[[ "$this_distro" = Debian ]] && selected_distro=Debian
[[ "$this_distro" = Ubuntu ]] && selected_distro=Debian
[[ "$this_distro" = LinuxMint ]] && selected_distro=Debian

# backoff distro
[[ -n $this_distro ]] || selected_distro=Debian

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
function select_libc {
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

case $selected_distro in
    CentOS)
        selected_libc_version=$(select_libc $this_libc_version "${CentOS_libc_preset_version[@]}")
    ;;
    Debian)
        selected_libc_version=$(select_libc $this_libc_version "${Debian_libc_preset_version[@]}")
    ;;
esac

[[ -z $selected_libc_version ]] && {
    echo "No suitable GCC found to download for your environment: $this_distro, glibc-$this_libc_version" 1>&2
    echo "Please invoke $this_dir/build-gcc.sh to build one manually" 1>&2
    exit 1
}

selected_archive=vesoft-gcc-$version-$selected_distro-$(uname -m)-glibc-$selected_libc_version.sh

url=$url_base/$selected_archive
$download_cmd $url
[[ $? -ne 0 ]] && {
    echo "Downloading $selected_archive failed" 1>&2
    exit 1
}

bash $selected_archive $@

rm -rf $selected_archive
