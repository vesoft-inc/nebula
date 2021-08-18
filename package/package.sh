#!/usr/bin/env bash
#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is the `commitId`
#   -n: Package to one or multi-packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#   -b: Branch, default master
#   -d: Whether to enable sanitizer, default OFF
#   -t: Build type, default Release
#   -j: Number of threads, default $(nproc)
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH>
#

set -e

version=""
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -b <BRANCH> -g <ON/OFF> -j <jobs>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/pkg-build
enablesanitizer="OFF"
static_sanitizer="OFF"
build_type="Release"
branch="master"
jobs=$(nproc)

while getopts v:n:s:b:d:t:j:g: opt;
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
        j)
            jobs=$OPTARG
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

function _build_graph {
    pushd ${build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${san} \
          -DENABLE_UBSAN=${san} \
          -DENABLE_STATIC_ASAN=${ssan} \
          -DENABLE_STATIC_UBSAN=${ssan} \
          -DCMAKE_INSTALL_PREFIX=/usr/local/nebula \
          -DENABLE_TESTING=OFF \
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

    _build_graph
}

# args: <strip_enable>
function package {
    pushd ${build_dir}
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
