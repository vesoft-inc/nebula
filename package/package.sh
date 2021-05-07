#!/usr/bin/env bash
#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is the `commitId`
#   -n: Package to one or multi packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#   -g: Whether build storage, default is ON
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF>
#

set -e

version=""
build_storage=ON
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/pkg-build
modules_dir=${project_dir}/modules
storage_dir=${modules_dir}/storage
storage_build_dir=${build_dir}/modules/storage
enablesanitizer="OFF"
static_sanitizer="OFF"
build_type="Release"
branch="master"
jobs=$(nproc)

while getopts v:n:s:b:d:t:g: opt;
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
        g)
            build_storage=$OPTARG
            ;;
        ?)
            echo "Invalid option, use default arguments"
            ;;
    esac
done

# version is null, get from tag name
[[ -z $version ]] && version=$(git describe --exact-match --abbrev=0 --tags | sed 's/^v//')
# version is null, use UTC date as version
[[ -z $version ]] && version=$(date -u +%Y.%m.%d)-nightly

if [[ -z $version ]]; then
    echo "version is null, exit"
    echo ${usage}
    exit 1
fi


if [[ $strip_enable != TRUE ]] && [[ $strip_enable != FALSE ]]; then
    echo "strip enable is wrong, exit"
    echo ${usage}
    exit 1
fi

echo "current version is [ $version ], strip enable is [$strip_enable], enablesanitizer is [$enablesanitizer], static_sanitizer is [$static_sanitizer]"

function _build_storage {
    if [[ ! -d ${storage_dir} && ! -L ${storage_dir} ]]; then
        git clone --single-branch --branch ${branch} https://github.com/vesoft-inc/nebula-storage.git ${storage_dir}
    fi

    pushd ${storage_build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${san} \
          -DENABLE_UBSAN=${san} \
          -DENABLE_STATIC_ASAN=${ssan} \
          -DENABLE_STATIC_UBSAN=${ssan} \
          -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
          -DNEBULA_COMMON_REPO_TAG=${branch} \
          -DENABLE_TESTING=OFF \
          -DENABLE_PACK_ONE=${package_one} \
          ${storage_dir}

    if ! ( make -j ${jobs} ); then
        echo ">>> build nebula storage failed <<<"
        exit 1
    fi
    popd
    echo ">>> build nebula storage successfully <<<"
}

function _build_graph {
    pushd ${build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${san} \
          -DENABLE_UBSAN=${san} \
          -DENABLE_STATIC_ASAN=${ssan} \
          -DENABLE_STATIC_UBSAN=${ssan} \
          -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
          -DNEBULA_COMMON_REPO_TAG=${branch} \
          -DENABLE_TESTING=OFF \
          -DENABLE_BUILD_STORAGE=OFF \
          -DENABLE_PACK_ONE=${package_one} \
          ${project_dir}

    if ! ( make -j ${jobs} ); then
        echo ">>> build nebula graph failed <<<"
        exit 1
    fi
    popd
    echo ">>> build nebula graph successfully <<<"
}

# args: <version>
function build {
    version=$1
    san=$2
    ssan=$3
    build_type=$4
    branch=$5

    rm -rf ${build_dir} && mkdir -p ${build_dir}

    if [[ "$build_storage" == "ON" ]]; then
        mkdir -p ${storage_build_dir}
        _build_storage
    fi
    _build_graph
}

# args: <strip_enable>
function package {
    # The package CMakeLists.txt in ${project_dir}/package/build
    package_dir=${build_dir}/package/
    if [[ -d $package_dir ]]; then
        rm -rf ${package_dir:?}/*
    else
        mkdir ${package_dir}
    fi
    pushd ${package_dir}
    cmake \
        -DNEBULA_BUILD_VERSION=${version} \
        -DENABLE_PACK_ONE=${package_one} \
        -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
        -DENABLE_PACKAGE_STORAGE=${build_storage} \
        -DNEBULA_STORAGE_SOURCE_DIR=${storage_dir} \
        -DNEBULA_STORAGE_BINARY_DIR=${storage_build_dir} \
        ${project_dir}/package/

    strip_enable=$1

    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    if ! ( cpack --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit 1
    else
        # rename package file
        outputDir=$build_dir/cpack_output
        mkdir -p ${outputDir}
        for pkg_name in $(ls ./*nebula*-${version}*); do
            mv ${pkg_name} ${outputDir}/
            echo "####### taget package file is ${outputDir}/${pkg_name}"
        done
    fi

    popd
}


# The main
build $version $enablesanitizer $static_sanitizer $build_type $branch
package $strip_enable
