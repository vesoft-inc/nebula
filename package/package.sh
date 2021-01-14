#!/usr/bin/env bash
#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is the `commitId`
#   -n: Package to one or multi packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH>
#

set -e

version=""
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/build
enablesanitizer="OFF"
static_sanitizer="OFF"
build_type="Release"
branch="master"

while getopts v:n:s:b:d:t: opt;
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
        b)
            branch=$OPTARG
            ;;
        d)
            enablesanitizer="ON"
            if [ "$OPTARG" == "static" ]; then
                static_sanitizer="ON"
            fi
            build_type="RelWithDebInfo"
            ;;
        t)
            build_type=$OPTARG
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
    branch=$5
    modules_dir=${project_dir}/modules
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    if [[ -d $modules_dir ]]; then
        rm -rf ${modules_dir}/*
    else
        mkdir ${modules_dir}
    fi

    pushd ${build_dir}

    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${san} \
          -DENABLE_UBSAN=${san} \
          -DENABLE_STATIC_ASAN=${ssan} \
          -DENABLE_STATIC_UBSAN=${ssan} \
          -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
          -DNEBULA_COMMON_REPO_TAG=${branch} \
          -DNEBULA_STORAGE_REPO_TAG=${branch} \
          -DENABLE_TESTING=OFF \
          -DENABLE_BUILD_STORAGE=ON \
          -DENABLE_PACK_ONE=${package_one} \
          $project_dir

    if !( make -j$(nproc) ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}

# args: <strip_enable>
function package {
    # The package CMakeLists.txt in ${project_dir}/package/build
    package_dir=${build_dir}/package/
    if [[ -d $package_dir ]]; then
        rm -rf ${package_dir}/*
    else
        mkdir ${package_dir}
    fi
    pushd ${package_dir}
    cmake \
        -DNEBULA_BUILD_VERSION=${version} \
        -DENABLE_PACK_ONE=${package_one} \
        -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
        ${project_dir}/package/

    strip_enable=$1

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
build $version $enablesanitizer $static_sanitizer $build_type $branch
package $strip_enable
