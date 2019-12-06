#!/bin/bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# step 1: ./build_dep.sh [N] # N: no need packages
# step 2: source ~/.bashrc
#

DIR=/tmp/download
mkdir $DIR
trap "rm -fr $DIR" EXIT

url_addr=https://nebula-graph.oss-accelerate.aliyuncs.com/third-party
no_deps=0

[[ $1 == N ]] && no_deps=1

function yum_install {
    # clean the installed third-party
    result=`rpm -qa | grep vs-nebula-3rdparty | wc -l`
    if [[ $result -ne 0 ]]; then
        echo "#### vs-nebula-3rdparty have been installed, uninstall it firstly ####"
        sudo rpm -e vs-nebula-3rdparty
    fi

    sudo yum -y install wget \
        make \
        glibc-devel \
        m4 \
        perl-WWW-Curl \
        ncurses-devel \
        readline-devel \
        maven \
        java-1.8.0-openjdk \
        unzip

    # feroda and centos7 has it
    if [[ $1 == 1 || $1 == 2 ]]; then
        sudo yum -y install perl-Data-Dumper
    fi

    if [[ $1 == 3 ]]; then
        sudo yum -y install perl-YAML \
            perl-CGI \
            perl-DBI \
            perl-Pod-Simple
    fi

    installPackage $1
    addAlias $1
    return 0
}

function aptget_install {
    # clean the installed third-party
    result=`dpkg -l | grep vs-nebula-3rdparty | wc -l`
    if [[ $result -ne 0 ]]; then
        echo "#### vs-nebula-3rdparty have been installed, uninstall it firstly ####"
        sudo dpkg -P vs-nebula-3rdparty
    fi

    sudo apt-get -y install wget \
        make \
        gcc-multilib \
        m4 \
        libncurses5-dev \
        libreadline-dev \
        python \
        maven \
        openjdk-8-jdk \
        unzip

    installPackage $1
    addAlias $1
    return 0
}

function installPackage {
    versions=(empty fedora29 centos7.5 centos6.5 ubuntu18 ubuntu16)
    package_name=${versions[$1]}
    echo "###### start install dep in $package_name ######"
    [[ $package_name = empty ]] && return 0
    [[ ! -n $package_name ]] && return 0
    if [[ no_deps -eq 0 ]]; then
        echo "###### There is no need to rely on packages ######"
        cd $DIR
        wget ${url_addr}/${package_name}.tar.gz
        tar xf ${package_name}.tar.gz
        cd ${package_name}
        ./install.sh
    fi
    return 0
}

function addAlias {
    echo "export PATH=/opt/nebula/third-party/bin:\$PATH" >> ~/.bashrc
    echo "export ACLOCAL_PATH=/opt/nebula/third-party/share/aclocal-1.15:/opt/nebula/third-party/share/aclocal" >> ~/.bashrc
    echo "alias cmake='/opt/nebula/third-party/bin/cmake -DCMAKE_C_COMPILER=/opt/nebula/third-party/bin/gcc -DCMAKE_CXX_COMPILER=/opt/nebula/third-party/bin/g++'" >> ~/.bashrc
    echo "alias ctest='/opt/nebula/third-party/bin/ctest'" >> ~/.bashrc
    if [[ $1 == 4 || $1 == 5 ]]; then
        echo "export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:\$LIBRARY_PATH" >> ~/.bashrc
    fi
    return 0
}

# fedora29:1, centos7.5:2, centos6.5:3, ubuntu18:4, ubuntu16:5
function getSystemVer {
    if [[ -e /etc/redhat-release ]]; then
        if [[ -n `cat /etc/redhat-release|grep Fedora` ]]; then
            echo 1
        elif [[ -n `cat /etc/redhat-release|grep "release 7."` ]]; then
            echo 2
        elif [[ -n `cat /etc/redhat-release|grep "release 6."` ]]; then
            echo 3
        else
            echo 0
        fi
    elif [[ -e /etc/issue ]]; then
        result=`cat /etc/issue|cut -d " " -f 2 |cut -d "." -f 1`
        if [[ result -eq 18 ]]; then
            echo 4
        elif [[ result -eq 16 ]]; then
            echo 5
        else
            echo 0
        fi
    else
        echo 0
    fi
}

version=$(getSystemVer)

set -e

case $version in
    1|2|3)
        yum_install $version
        ;;
    4|5)
        aptget_install $version
        ;;
    *)
        echo "unknown system"
        exit -1
        ;;
esac

echo "###### install succeed ######"
exit 0
