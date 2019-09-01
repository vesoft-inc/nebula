FROM ubuntu:18.04

WORKDIR /root

RUN apt-get update -y && apt-get install -y git git-lfs \
    gcc-multilib libtool autoconf autoconf-archive automake python maven \
    openjdk-8-jdk make gcc bison liblzma-dev wget && \
    wget https://nebula-graph.oss-cn-hangzhou.aliyuncs.com/build-deb/ubuntu1804.tar.gz && \
    git lfs install && \
    adduser --system --group --home /home/engshare engshare && \
    chmod -R 755 /home/engshare && \
    tar -xvzf ubuntu1804.tar.gz && cd ubuntu1804/ && \
    dpkg -i *.deb  && cd && rm -rf ubuntu1804.tar.gz ubuntu1804 && \
    echo 'alias cmake="/home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_READLINE_ROOT=/home/engshare/readline -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses -DNEBULA_READLINE_ROOT=/home/engshare/readline"' >> ~/.bashrc && \
    echo 'alias ctest="/home/engshare/cmake/bin/ctest"' >> ~/.bashrc

# ThirdParty
RUN ln -s /usr/lib/x86_64-linux-gnu /usr/lib64 && \
    git clone --depth=1 https://github.com/vesoft-inc/nebula-3rdparty.git && \
    cd nebula-3rdparty && \
    /home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_READLINE_ROOT=/home/engshare/readline -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses -DNEBULA_READLINE_ROOT=/home/engshare/readline ./ && \
    make -j$(nproc) && \
    make install && cd && rm -rf nebula-3rdparty
