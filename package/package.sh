#!/usr/bin/env bash
#
#  package nebula as one deb/rpm
# ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -t <auto/tar> the version should be match tag name
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
package_type="auto"
file_type=""
install_dir="/usr/local/nebula"
build_dir=$PROJECT_DIR/build

while getopts v:n:s:d:t: opt;
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
        t)
            package_type=$OPTARG
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

if [[ $package_type != "auto" ]] && [[ $package_type != "tar" ]]; then
    echo "package type[$package_type] is wrong, exit"
    echo ${usage}
    exit -1
fi

echo "current version is [ $version ], strip enable is [$strip_enable], enablesanitizer is [$enablesanitizer], static_sanitizer is [$static_sanitizer], package type is [ $package_type ]"

# args: <version>
function build {
    version=$1
    san=$2
    ssan=$3
    build_type=$4
    if [[ -d $build_dir ]]; then
        rm -rf ${build_dir}/*
    else
        mkdir ${build_dir}
    fi

    pushd ${build_dir}

    cmake -DCMAKE_BUILD_TYPE=${build_type} -DNEBULA_BUILD_VERSION=${version} -DENABLE_ASAN=${san} --DENABLE_UBSAN=${san} \
        -DENABLE_STATIC_ASAN=${ssan} -DENABLE_STATIC_UBSAN=${ssan} -DCMAKE_INSTALL_PREFIX=${install_dir} -DENABLE_TESTING=OFF \
        -DENABLE_PACK_ONE=${package_one} $PROJECT_DIR

    if !( make -j$(nproc) ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}


function get_package_name {
    if [[ -f "/etc/redhat-release" ]]; then
        sys_name=`cat /etc/redhat-release | cut -d ' ' -f1`
        if [[ ${sys_name} == "CentOS" ]]; then
            sys_ver=`cat /etc/redhat-release | tr -dc '0-9.' | cut -d \. -f1`
            if [[ ${sys_ver} == 7 ]] || [[ ${sys_ver} == 6 ]]; then
                package_name=.el${sys_ver}-5.$(uname -m)
            else
                package_name=.el${sys_ver}.$(uname -m)
            fi
        elif [[ ${sys_name} == "Fedora" ]]; then
            sys_ver=`cat /etc/redhat-release | cut -d ' ' -f3`
            package_name=.fc${sys_ver}.$(uname -m)
        fi
        file_type="RPM"
    elif [[ -f "/etc/lsb-release" ]]; then
        sys_name=`cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2 | sed 's/\.//'`
        package_name=.ubuntu${sys_name}.$(uname -m)
        file_type="DEB"
    fi
}

# args: <strip_enable>
function package {
    strip_enable=$1
    pushd $PROJECT_DIR/build/
    args=""
    [[ $strip_enable == TRUE ]] && args="-D CPACK_STRIP_FILES=TRUE -D CPACK_RPM_SPEC_MORE_DEFINE="

    if !( cpack -G ${file_type} --verbose $args ); then
        echo ">>> package nebula failed <<<"
        exit -1
    else
        # rename package file
        pkg_names=`ls | grep nebula | grep ${version}`
        outputDir=$build_dir/cpack_output
        mkdir -p ${outputDir}
        for pkg_name in ${pkg_names[@]};
        do
            new_pkg_name=${pkg_name/\-Linux/${package_name}}
            mv ${pkg_name} ${outputDir}/${new_pkg_name}
            echo "####### taget package file is ${outputDir}/${new_pkg_name}"
        done
    fi

    popd
}


function package_tar_sh {
    exec_file=$build_dir/nebula-$version$package_name.sh

    echo "Creating self-extractable package $exec_file"
    cat > $exec_file <<EOF
#! /usr/bin/env bash
set -e

hash xz &> /dev/null || { echo "xz: Command not found"; exit 1; }

[[ \$# -ne 0 ]] && prefix=\$(echo "\$@" | sed 's;.*--prefix=(\S*).*;\1;p' -rn)
prefix=\${prefix:-/usr/local/nebula}
mkdir -p \$prefix

[[ -w \$prefix ]] || { echo "\$prefix: No permission to write"; exit 1; }

archive_offset=\$(awk '/^__start_of_archive__$/{print NR+1; exit 0;}' \$0)
tail -n+\$archive_offset \$0 | tar --no-same-owner --numeric-owner -xJf - -C \$prefix

daemons=(metad graphd storaged)
for daemon in \${daemons[@]}
do
    if [[ ! -f \$prefix/etc/nebula-\$daemon.conf ]] && [[ -f \$prefix/etc/nebula-\$daemon.conf.default ]]; then
        cp \$prefix/etc/nebula-\$daemon.conf.default \$prefix/etc/nebula-\$daemon.conf
        chmod 644 \$prefix/etc/nebula-\$daemon.conf
    fi
done

echo "Nebula Graph has been installed to \$prefix"

exit 0

__start_of_archive__
EOF
    pushd $install_dir
    tar -cJf - * >> $exec_file
    chmod 0755 $exec_file
    echo "####### taget package file is $exec_file"
    popd
}

# The main
get_package_name

if [[ $package_type == "auto" ]]; then
    build $version $enablesanitizer $static_sanitizer $buildtype
    package $strip_enable
else
    install_dir=${build_dir}/install
    build $version $enablesanitizer $static_sanitizer $buildtype
    pushd ${build_dir}
        make install -j$(nproc)
    popd
    package_tar_sh
fi
