#!/bin/bash
#
#  package nebula as one deb/rpm
# ./package.sh -v <version> -t <packageType> -s <strip_enable> the version should be match tag name
#

version=""
pType=""
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -t <RPM/DEB> -s <TRUE/FALSE>"

while getopts v:t:s: opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        t)
            pType=$OPTARG
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
[[ -z $version ]] && version=`git describe --match 'v*' | sed 's/^v//'`

if [[ -z $version ]]; then
    echo "version is null, exit"
    echo ${usage}
    exit -1
fi


if [[ -z $pType ]] || ([[ $pType != RPM ]] && [[ $pType != DEB ]]); then
    echo "package type is null or type is wrong, exit"
    echo ${usage}
    exit -1
fi

if [[ $strip_enable != TRUE ]] && [[ $strip_enable != FALSE ]]; then
    echo "strip enable is wrong, exit"
    echo ${usage}
    exit -1
fi

echo "current version is [ $version ], package type is [$pType], strip enable is [$strip_enable]"

# args: <version>
function build {
    version=$1
    build_dir=../build
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    pushd ${build_dir}

    cmake -DCMAKE_BUILD_TYPE=Release -DNEBULA_BUILD_VERSION=${version} -DCMAKE_INSTALL_PREFIX=/usr/local/nebula ..

    if !( make -j10 ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}

# args: <pType> <strip_enable>
function package {
    pType=$1
    strip_enable=$2
    pushd ../build/
    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="
    if !( cpack -G ${pType} --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit -1
    fi

    systemVersion=""
    tagetPackageName=""

    if [[ ${pType} == RPM ]]; then
        # rename rpm file
        if [[ `cat /etc/redhat-release | grep 7.5` == "" ]]; then
            systemVersion="el6-5"
        else
            systemVersion="el7-5"
        fi
        rpmName=`ls | grep nebula-graph | grep rpm | grep ${version}`
        tagetPackageName=nebula-${version}.${systemVersion}.x86_64.rpm
        mv ${rpmName} ${tagetPackageName}

    else
        # rename deb file
        systemVersion=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        systemVersion=ubuntu${systemVersion}
        debName=`ls | grep nebula-graph | grep deb | grep ${version}`
        tagetPackageName=nebula-${version}.${systemVersion}.amd64.deb
        mv ${debName} ${tagetPackageName}
    fi

    echo "####### taget package file is `pwd`/${tagetPackageName}"
    popd
}


# The main
build $version
package $pType $strip_enable
