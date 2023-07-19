FROM vesoft/nebula-dev:centos7 as builder

ENV BUILD_IN_DOCKER TRUE
ARG VERSION=

COPY . /home/nebula/BUILD

RUN cd /home/nebula/BUILD/package \
  && ./package.sh -v "${VERSION}"
  
FROM centos:7 as graphd

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /usr/local/nebula/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-graph.rpm /usr/local/nebula/nebula-graphd.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm

EXPOSE 9669 19669 19670

ENTRYPOINT ["/usr/local/nebula/bin/nebula-graphd", "--flagfile=/usr/local/nebula/etc/nebula-graphd.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as metad

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /usr/local/nebula/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-meta.rpm /usr/local/nebula/nebula-metad.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm

EXPOSE 9559 9560 19559 19560

ENTRYPOINT ["/usr/local/nebula/bin/nebula-metad", "--flagfile=/usr/local/nebula/etc/nebula-metad.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as storaged

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /usr/local/nebula/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-storage.rpm /usr/local/nebula/nebula-storaged.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm

EXPOSE 9777 9778 9779 9780 19779 19780

ENTRYPOINT ["/usr/local/nebula/bin/nebula-storaged", "--flagfile=/usr/local/nebula/etc/nebula-storaged.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as tools

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-tool.rpm /usr/local/nebula/nebula-tool.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && rm -rf *.rpm

# default entrypoint
ENTRYPOINT ["/usr/local/nebula/bin/db_dump"]
