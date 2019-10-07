#!/bin/bash
#
#  package nebula cpp client lib as one deb/rpm
# ./package_libcppclient.sh <version> <packageType> 
#

RPM_SEPC=nebula_client.spec
trap "rm -rf $RPM_SEPC _debian" EXIT

VERSION=$1
TYPE=$2
CURRENT_DIR=`pwd`
BUILD_DIR=${CURRENT_DIR}/../build
INCLUDE_DIR=${CURRENT_DIR}/../src/client/cpp/include
LIB_DIR=${BUILD_DIR}/src/client/cpp/lib/_build
USAGE="Usage: ${0} <version> <RPM/DEB>"

function build {
    if [[ -d $BUILD_DIR ]]; then
        rm -rf ${BUILD_DIR}/*
    else
        mkdir ${BUILD_DIR}
    fi

    pushd ${BUILD_DIR}

    cmake -DCMAKE_BUILD_TYPE=Release -DNEBULA_BUILD_VERSION=${VERSION} -DENABLE_CPPCLIENT_LIB=ON ..

    if !( make nebula_cpp_client -j $(nproc) ); then
        echo ">>> build nebula failed <<<"
        exit -1
    fi

    popd
}

function createRpmSpec {
    cat > $RPM_SEPC <<EOF
Name: nebula_client
Version: %{_version}
Release: 0%{?dist}
Summary: nebula_client
Group: vesoft-inc
License: Apache 2.0 + Common Clause 1.0 

%description

%prep
%build
%install
mkdir -p \${RPM_BUILD_ROOT}/usr/local/include
mkdir -p \${RPM_BUILD_ROOT}/usr/local/lib
cp -r %{_include_dir}/nebula \${RPM_BUILD_ROOT}/usr/local/include
cp %{_lib_dir}/libnebula_cpp_client.so \${RPM_BUILD_ROOT}/usr/local/lib

%files
%doc
/usr/local/include/nebula
/usr/local/lib/libnebula_cpp_client.so

%define debug_package %{nil}
%define __os_install_post %{nil}
%changelog

EOF

}

function createDebSpec {
    mkdir -p _debian/DEBIAN
    control_file=_debian/DEBIAN/control
    cat > $control_file <<EOF
Package: nebula_client
Version: $VERSION
Section: nebula_client
Priority: optional
Architecture: amd64
Depends:
Maintainer: Vesoft Inc <info@vesoft.com>
Description: Vesoft nebula_client-$VERSION package
EOF

    mkdir -p _debian/usr/local/include
    mkdir -p _debian/usr/local/lib
    cp -r $INCLUDE_DIR/nebula _debian/usr/local/include
    cp $LIB_DIR/libnebula_cpp_client.so _debian/usr/local/lib
}

function make_rpm {
    createRpmSpec
    rpmbuild -D"_version $VERSION" -D"_include_dir $INCLUDE_DIR" -D"_lib_dir $LIB_DIR" -bb $RPM_SEPC
}

function make_deb {
    createDebSpec
    dpkg-deb --build _debian
    mv _debian.deb nebula_client-$VERSION-amd64.deb
}

# main
if [[ -z $VERSION ]]; then
    echo "version is null, exit"
    echo $USAGE
    exit -1
fi

build

[[ $TYPE != RPM ]] && [[ $TYPE != DEB ]] && TYPE=RPM
[[ $TYPE == RPM ]] && make_rpm
[[ $TYPE == DEB ]] && make_deb

exit 0
