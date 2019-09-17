#!/bin/bash
# step 1: ./build_dep.sh <C|U>   C: the user in China, U: the user in US
# step 2: source ~/.bashrc

url_addr=https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb
if [[ $1 != C ]] && [[ $1 != U ]]; then
    echo "input is wrong, use default url[$url_addr]"
fi

if [[ $1 == U ]]; then
    url_addr=https://nebula-graph-us.oss-us-west-1.aliyuncs.com/build-deb/
fi

# fedora
function fedora_install {
    echo "###### start install dep in fedora ######"
#   sudo yum -y install autoconf autoconf-archive automake bison boost boost-devel boost-static bzip2-devel cmake curl flex gcc gcc-c++ gperf  java-1.8.0-openjdk java-1.8.0-openjdk-devel krb5-devel libstdc++-static libstdc++-devel libunwind libunwind-devel libtool make maven ncurses ncurses-devel openssl openssl-devel perl perl-WWW-Curl python readline readline-devel unzip  xz-devel
    mkdir download
    pushd download
        wget $url_addr/vs-nebula-3rdparty.fc.x86_64.rpm
#sudo rpm -ivh vs-nebula-3rdparty.fc.x86_64.rpm
    popd
    rm -rf download/
}

# centos6
function centos6_install {
    echo "###### start install dep in centos6.5 ######"
    mkdir download
    pushd download
        sudo yum -y install wget libtool autoconf autoconf-archive automake perl-WWW-Curl perl-YAML perl-CGI perl-DBI perl-Pod-Simple glibc-devel libstdc++-static ncurses-devel readline-devel maven java-1.8.0-openjdk unzip
        wget $url_addr/centos-6.5.tar.gz
        tar xf centos-6.5.tar.gz
        pushd centos-6.5
            sudo rpm -ivh *.rpm
        popd

        wget $url_addr/vs-nebula-3rdparty.el6.x86_64.rpm
        sudo rpm -ivh vs-nebula-3rdparty.el6-5.x86_64.rpm
    popd
    rm -rf download/
    echo "export PATH=/opt/nebula/autoconf/bin:/opt/nebula/automake/bin:/opt/nebula/libtool/bin:/opt/nebula/gettext/bin:/opt/nebula/flex/bin:/opt/nebula/binutils/bin:$PATH" >> ~/.bashrc
    echo "export ACLOCAL_PATH=/opt/nebula/automake/share/aclocal-1.15:/opt/nebula/libtool/share/aclocal:/opt/nebula/autoconf-archive/share/aclocal" >> ~/.bashrc
}

# centos7
function centos7_install {
    echo "###### start install dep in centos7.5 ######"
    mkdir download
    pushd download
        sudo yum -y install wget libtool autoconf autoconf-archive automake ncurses-devel readline-devel perl-WWW-Curl libstdc++-static maven java-1.8.0-openjdk unzip
        wget $url_addr/centos-7.5-1804.tar.gz
        tar xf centos-7.5-1804.tar.gz
        pushd centos-7.5-1804
            sudo rpm -ivh *.rpm
        popd

        wget $url_addr/vs-nebula-3rdparty.el7.x86_64.rpm
        sudo rpm -ivh vs-nebula-3rdparty.el7-5.x86_64.rpm
    popd
    rm -rf download/
}

# ubuntu1604
function ubuntu16_install {
    echo "###### start install dep in ubuntu16 ######"
    mkdir download
    pushd download
        sudo yum -y install wget libtool autoconf autoconf-archive automake perl-WWW-Curl perl-YAML perl-CGI glibc-devel libstdc++-static ncurses ncurses-devel readline readline-devel maven java-1.8.0-openjdk unzip
        wget $url_addr/ubuntu1804.tar.gz
        tar xf ubuntu1804.tar.gz
        pushd ubuntu1804/
            sudo rpm -ivh *.rpm
        popd

        wget $url_addr/vs-nebula-3rdparty.ubuntu1604.amd64.deb
        sudo rpm -ivh vs-nebula-3rdparty.ubuntu1604.amd64.deb
    popd
    rm -rf download/
}

# ubuntu1804
function ubuntu18_install {
    echo "###### start install dep in ubuntu18 ######"
    mkdir download
    pushd download
        sudo yum -y install wget libtool autoconf autoconf-archive automake perl-WWW-Curl perl-YAML perl-CGI glibc-devel libstdc++-static ncurses ncurses-devel readline readline-devel maven java-1.8.0-openjdk unzip
        wget $url_addr/ubuntu1804.tar.gz
        tar xf ubuntu1804.tar.gz
        pushd ubuntu1804/
            sudo rpm -ivh *.rpm
        popd

        wget $url_addr/vs-nebula-3rdparty.ubuntu1604.amd64.deb
        sudo rpm -ivh vs-nebula-3rdparty.ubuntu1804.amd64.deb
        wget http://ftp.gnu.org/gnu/bison/bison-3.0.5.tar.gz
        tar xf bison-3.0.5.tar.gz
        pushd bison-3.0.5/
            ./configure
            make && sudo make install
        popd
    popd
    rm -rf download/
}

function addAlias {
    echo "alias cmake='/opt/nebula/cmake/bin/cmake -DCMAKE_C_COMPILER=/opt/nebula/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/opt/nebula/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/opt/nebula/gperf/bin -DNEBULA_FLEX_ROOT=/opt/nebula/flex -DNEBULA_BOOST_ROOT=/opt/nebula/boost -DNEBULA_OPENSSL_ROOT=/opt/nebula/openssl -DNEBULA_KRB5_ROOT=/opt/nebula/krb5 -DNEBULA_LIBUNWIND_ROOT=/opt/nebula/libunwind -DNEBULA_BISON_ROOT=/opt/nebula/bison'" >> ~/.bashrc
    echo "alias ctest='/opt/nebula/cmake/bin/ctest'" >> ~/.bashrc
    echo "export LD_LIBRARY_PATH=/opt/nebula/gcc/lib64:$LD_LIBRARY_PATH" >> ~/.bashrc
}

# fedora:1, centos7:2, centos6:3, ubuntu18:4, ubuntu16:5
function getSystemVer {
    result=`cat /proc/version|grep fc`
    if [[ -n $result ]]; then
        return 1
    fi
    result=`cat /proc/version|grep el7`
    if [[ -n $result ]]; then
        return 2
    fi
    result=`cat /proc/version|grep el6`
    if [[ -n $result ]]; then
        return 3
    fi
    result=`cat /proc/version|grep ubuntu1~18`
    if [[ -n $result ]]; then
        return 4
    fi
    result=`cat /proc/version|grep ubuntu1~16`
    if [[ -n $result ]]; then
        return 5
    fi
    return 0
}

getSystemVer

case $? in
    1)
        fedora_install
        exit 0
        ;;
    2)
        centos7_install
        addAlias
        exit 0
        ;;
    3)
        centos6_install
        addAlias
        exit 0
        ;;
    4)
        ubuntu18_install
        addAlias
        exit 0
        ;;
    5)
        ubuntu16_install
        addAlias
        exit 0
        ;;
    *)
        echo "unknown system"
        exit -1
        ;;
esac
