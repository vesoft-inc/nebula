FROM centos:centos7.5.1804

RUN cd && \
    yum -y update && yum install -y git libtool autoconf \
    autoconf-archive automake perl-WWW-Curl libstdc++-static maven \
    java-1.8.0-openjdk wget make bison ncurses-devel xz-devel curl && \
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | bash && \
    yum install -y git-lfs && git lfs install && \
    wget -q https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/centos-7.5-1804.tar.gz && \
    useradd -U -m -r engshare && \
    chmod -R 755 /home/engshare && \
    tar -xvzf centos-7.5-1804.tar.gz && cd centos-7.5-1804/ && \
    rpm -ivh *.rpm && cd && rm -rf centos-7.5-1804.tar.gz centos-7.5-1804/ && \
    echo 'alias cmake="/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses"' >> ~/.bashrc && \
    echo 'alias ctest="/home/engshare/cmake/bin/ctest"' >> ~/.bashrc

# ThirdParty
RUN cd && git clone --depth=1 https://github.com/vesoft-inc/nebula-3rdparty.git && \
    cd nebula-3rdparty && \
    /home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses ./ && \
    make -j$(nproc) && make install && cd && rm -rf nebula-3rdparty
