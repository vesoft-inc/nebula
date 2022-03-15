#!/usr/bin/env bash
#
#  Package nebula as deb/rpm package
#
# introduce the args
#   -v: The version of package, the version should be match tag name, default value is null
#   -n: Package to one or multi-packages, `ON` means one package, `OFF` means multi packages, default value is `ON`
#   -s: Whether to strip the package, default value is `FALSE`
#   -d: Whether to enable sanitizer, default OFF
#   -t: Build type, default Release
#   -j: Number of threads, default $(nproc)
#   -r: Whether to enable compressed debug info, default ON
#   -p: Whether to dump the symbols from binary by dump_syms
#   -c: Whether to enable console building, default ON
#
# usage: ./package.sh -v <version> -n <ON/OFF> -s <TRUE/FALSE> -c <ON/OFF>
#

set -e

version=""
package_one=ON
strip_enable="FALSE"
usage="Usage: ${0} -v <version> -n <ON/OFF> -s <TRUE/FALSE> -g <ON/OFF> -j <jobs> -t <BUILD TYPE>"
project_dir="$(cd "$(dirname "$0")" && pwd)/.."
build_dir=${project_dir}/pkg-build
enablesanitizer="OFF"
static_sanitizer="OFF"
build_type="Release"
build_console="ON"
branch=$(git rev-parse --abbrev-ref HEAD)
jobs=$(nproc)
enable_compressed_debug_info=ON
dump_symbols="OFF"
dump_syms_tool_dir=
system_name=
install_prefix=/usr/local/nebula

while getopts v:n:s:b:d:t:r:p:j:c: opt;
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
            build_type="RelWithDebInfo"
            ;;
        t)
            build_type=$OPTARG
            ;;
        c)
            build_console=$OPTARG
            ;;
        j)
            jobs=$OPTARG
            ;;
        r)
            enable_compressed_debug_info=$OPTARG
            ;;
        p)
            dump_symbols=$OPTARG
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

cat << EOF
Configuration for this shell:
version: $version
strip_enable: $strip_enable
enablesanitizer: $enablesanitizer
static_sanitizer: $static_sanitizer
build_type: $build_type
build_console: $build_console
branch: $branch
enable_compressed_debug_info: $enable_compressed_debug_info
dump_symbols: $dump_symbols

EOF

function _build_graph {
    pushd ${build_dir}
    cmake -DCMAKE_BUILD_TYPE=${build_type} \
          -DNEBULA_BUILD_VERSION=${version} \
          -DENABLE_ASAN=${san} \
          -DENABLE_UBSAN=${san} \
          -DENABLE_STATIC_ASAN=${ssan} \
          -DENABLE_STATIC_UBSAN=${ssan} \
          -DCMAKE_INSTALL_PREFIX=${install_prefix} \
          -DENABLE_TESTING=OFF \
          -DENABLE_CONSOLE_COMPILATION=${build_console} \
          -DENABLE_PACK_ONE=${package_one} \
          -DENABLE_COMPRESSED_DEBUG_INFO=${enable_compressed_debug_info} \
          -DENABLE_PACKAGE_TAR=${package_tar} \
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
    package_tar=$5
    install_prefix=$6

    mkdir -p ${build_dir}

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
            echo "####### target package file is ${outputDir}/${pkg_name}"
        done
    fi

    popd
}

function _find_dump_syms_tool {
    if [[ -x ${build_dir}/third-party/install/bin/dump_syms ]]; then
        dump_syms_tool_dir=${build_dir}/third-party/install/bin
    elif [[ -x /opt/vesoft/third-party/2.0/bin/dump_syms ]]; then
        dump_syms_tool_dir=/opt/vesoft/third-party/2.0/bin
    else
        echo ">>> Failed to find the dump_syms tool <<<"
        exit 1
    fi
}

# This is only for releasing the disk resources.
function _strip_unnecessary_binaries {
    for bin in $(ls -1 -F ${build_dir}/bin/ | grep -v [/$] | sed -e '/nebula-metad/d;/nebula-graphd/d;/nebula-storaged/d'); do
        if ! (strip ${build_dir}/bin/${bin}); then
            echo ">>> strip ${bin} failed: $?. <<<"
            exit 1
        fi
    done
}

function dump_syms {
    _strip_unnecessary_binaries
    _find_dump_syms_tool
    dump_syms=${dump_syms_tool_dir}/dump_syms

    syms_dir=${build_dir}/symbols/
    rm -rf ${syms_dir} && mkdir -p ${syms_dir}

    pack=`ls ${build_dir}/cpack_output/`
    tmp=${pack#nebula-graph}
    ver=${tmp%.*}

    for bin in nebula-graphd nebula-storaged nebula-metad; do
        if ! (${dump_syms} ${build_dir}/bin/${bin} > ${syms_dir}/${bin}${ver}.sym); then
            echo ">>> dump ${bin} symbols failed: $?. <<<"
            exit 1
        fi
    done
}

# The main
build $version $enablesanitizer $static_sanitizer $build_type "OFF" "/usr/local/nebula"
package $strip_enable
if [[ "$dump_symbols" == "ON" ]]; then
    echo ">>> start dump symbols <<<"
    dump_syms
fi

# tar package
build $version $enablesanitizer $static_sanitizer $build_type "ON" "/"
package $strip_enable
