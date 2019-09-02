FROM nebula/build-centos as builder

RUN git clone --depth=1 https://github.com/vesoft-inc/nebula.git && \
    cd nebula && mkdir -p .build && cd .build && \
    /home/engshare/cmake/bin/cmake -DCMAKE_C_COMPILER=/home/engshare/gcc/bin/gcc -DCMAKE_CXX_COMPILER=/home/engshare/gcc/bin/g++ -DNEBULA_GPERF_BIN_DIR=/home/engshare/gperf/bin -DNEBULA_FLEX_ROOT=/home/engshare/flex -DNEBULA_BOOST_ROOT=/home/engshare/boost -DNEBULA_OPENSSL_ROOT=/home/engshare/openssl -DNEBULA_KRB5_ROOT=/home/engshare/krb5 -DNEBULA_LIBUNWIND_ROOT=/home/engshare/libunwind -DNEBULA_NCURSES_ROOT=/home/engshare/ncurses .. && \
    make -j$(nproc)

FROM centos:centos7.5.1804

WORKDIR /root

COPY --from=builder /root/nebula/.build /root/nebula/dist

RUN yum update -y && yum install -y make cmake && \
    cd /root/nebula/dist && make install && cd && rm -rf /root/nebula
