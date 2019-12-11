#!/usr/bin/env bash
#
#  package nebula as one deb/rpm
# ./package.sh -v <version> -s <strip_enable> the version should be match tag name
#

set -ex

export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:$LIBRARY_PATH

NEBULA_DEP_BIN=/opt/nebula/third-party/bin

version=""
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -s <TRUE/FALSE>"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"/../

while getopts v:t:s: opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        s)
            strip_enable=$OPTARG
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

echo "current version is [ $version ], strip enable is [$strip_enable]"

# args: <version>
function build {
    version=$1
    build_dir=$PROJECT_DIR/build
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    pushd ${build_dir}

    $NEBULA_DEP_BIN/cmake -DCMAKE_C_COMPILER=$NEBULA_DEP_BIN/gcc -DCMAKE_CXX_COMPILER=$NEBULA_DEP_BIN/g++ -DCMAKE_BUILD_TYPE=Release -DNEBULA_BUILD_VERSION=${version} -DCMAKE_INSTALL_PREFIX=/usr/local/nebula -DENABLE_TESTING=OFF $PROJECT_DIR

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

    tagetPackageName=""
    pType="RPM"

    if [[ -f "/etc/redhat-release" ]]; then
        # TODO: update minor version according to OS
        centosMajorVersion=`cat /etc/redhat-release | tr -dc '0-9.' | cut -d \. -f1`
        [[ "$centosMajorVersion" == "7" ]] && tagetPackageName="nebula-${version}.el7-5.x86_64.rpm"
        [[ "$centosMajorVersion" == "6" ]] && tagetPackageName="nebula-${version}.el6-5.x86_64.rpm"
    elif [[ -f "/etc/lsb-release" ]]; then
        ubuntuVersion=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        tagetPackageName="nebula-${version}.ubuntu${ubuntuVersion}.amd64.deb"
        pType="DEB"
    fi

    if [[ "$tagetPackageName" == "" ]]; then
        echo ">>> Unsupported system <<<"
        exit -1
    fi

    if !( $NEBULA_DEP_BIN/cpack -G ${pType} --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit -1
    else
        # rename package file
        pkgName=`ls | grep nebula-graph | grep ${version}`
        outputDir=$PROJECT_DIR/build/cpack_output
        mkdir -p ${outputDir}
        mv ${pkgName} ${outputDir}/${tagetPackageName}
        echo "####### taget package file is ${outputDir}/${tagetPackageName}"
    fi

    popd
}


# The main
build $version
package $strip_enable
