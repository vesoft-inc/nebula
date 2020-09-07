#!/usr/bin/env bash
#
#  package nebula as one deb/rpm
# ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> the version should be match tag name
#

set -ex

version=""
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE>"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"/../
enablesanitizer="OFF"
static_sanitizer="OFF"
buildtype="Release"

while getopts v:n:s:d: opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        n)
            package_one=$OPTARG
            ;;
        s)
            strip_enable=$OPTARG
            ;;
        d)
            enablesanitizer="ON"
            if [ "$OPTARG" == "static" ]; then
                static_sanitizer="ON"
            fi
            buildtype="RelWithDebInfo"
            ;;
        ?)
            echo "Invalid option, use default arguments"
            ;;
    esac
done

# version is null, get from tag name
[[ -z $version ]] && version=`git describe --exact-match --abbrev=0 --tags | sed 's/^v//'`
# version is null, use UTC date as version
[[ -z $version ]] && version=$(date -u +%Y.%m.%d)-nightly

if [[ -z $version ]]; then
    echo "version is null, exit"
    echo ${usage}
    exit -1
fi


if [[ $strip_enable != TRUE ]] && [[ $strip_enable != FALSE ]]; then
    echo "strip enable is wrong, exit"
    echo ${usage}
    exit -1
fi

echo "current version is [ $version ], strip enable is [$strip_enable], enablesanitizer is [$enablesanitizer], static_sanitizer is [$static_sanitizer]"

# args: <version>
function build {
    version=$1
    san=$2
    ssan=$3
    build_type=$4
    build_dir=$PROJECT_DIR/build
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    pushd ${build_dir}

    cmake -DCMAKE_BUILD_TYPE=${build_type} -DNEBULA_BUILD_VERSION=${version} -DENABLE_ASAN=${san} --DENABLE_UBSAN=${san} \
        -DENABLE_STATIC_ASAN=${ssan} -DENABLE_STATIC_UBSAN=${ssan} -DCMAKE_INSTALL_PREFIX=/usr/local/nebula -DENABLE_TESTING=OFF \
        -DENABLE_PACK_ONE=${package_one} $PROJECT_DIR

    if !( make -j$(nproc) ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}

# args: <strip_enable>
function package {
    strip_enable=$1
    pushd $PROJECT_DIR/build/
    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    sys_ver=""
    pType="RPM"
    if [[ -f "/etc/redhat-release" ]]; then
        sys_name=`cat /etc/redhat-release | cut -d ' ' -f1`
        if [[ ${sys_name} == "CentOS" ]]; then
            sys_ver=`cat /etc/redhat-release | tr -dc '0-9.' | cut -d \. -f1`
            sys_ver=.el${sys_ver}.x86_64
        elif [[ ${sys_name} == "Fedora" ]]; then
            sys_ver=`cat /etc/redhat-release | cut -d ' ' -f3`
            sys_ver=.fc${sys_ver}.x86_64
        fi
        pType="RPM"
    elif [[ -f "/etc/lsb-release" ]]; then
        sys_ver=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        sys_ver=.ubuntu${sys_ver}.amd64
        pType="DEB"
    fi

    if !( cpack -G ${pType} --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit -1
    else
        # rename package file
        pkg_names=`ls | grep nebula | grep ${version}`
        outputDir=$build_dir/cpack_output
        mkdir -p ${outputDir}
        for pkg_name in ${pkg_names[@]};
        do
            new_pkg_name=${pkg_name/\-Linux/${sys_ver}}
            mv ${pkg_name} ${outputDir}/${new_pkg_name}
            echo "####### taget package file is ${outputDir}/${new_pkg_name}"
        done
    fi

    popd
}


# The main
build $version $enablesanitizer $static_sanitizer $buildtype
package $strip_enable
