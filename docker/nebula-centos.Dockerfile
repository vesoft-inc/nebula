FROM nebula/build-centos as builder

RUN git clone --depth=1 https://github.com/vesoft-inc/nebula.git && \
    cd nebula && mkdir -p .build && cd .build && \
    ${cmake} .. && make -j$(nproc)

FROM centos:centos7.5.1804

WORKDIR /root

COPY --from=builder /root/nebula/.build /root/nebula/dist

RUN yum update -y && yum install -y make cmake && \
    cd /root/nebula/dist && make install && cd && rm -rf /root/nebula
