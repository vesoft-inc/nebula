FROM vesoft/nebula-dev:centos7 as builder

ENV BUILD_IN_DOCKER TRUE
ARG VERSION=

COPY . /home/nebula/BUILD

RUN cd /home/nebula/BUILD/package \
  && ./package.sh -v "${VERSION}"
  
FROM centos:7 as graphd

RUN groupadd -r nebula && useradd -r -g nebula -m -d /home/nebula nebula

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /tmp/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-graph.rpm /tmp/nebula-graphd.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh /tmp/*.rpm \
  && mkdir -p ./{logs,data,pids} \
  && chown -R nebula:nebula /usr/local/nebula \
  && rm -rf /tmp/*.rpm

EXPOSE 9669 19669 19670

USER nebula
ENTRYPOINT ["/usr/local/nebula/bin/nebula-graphd", "--flagfile=/usr/local/nebula/etc/nebula-graphd.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as metad

RUN groupadd -r nebula && useradd -r -g nebula -m -d /home/nebula nebula

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /tmp/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-meta.rpm /tmp/nebula-metad.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh /tmp/*.rpm \
  && mkdir -p ./{logs,data,pids} \
  && chown -R nebula:nebula /usr/local/nebula \
  && rm -rf /tmp/*.rpm

EXPOSE 9559 9560 19559 19560

USER nebula
ENTRYPOINT ["/usr/local/nebula/bin/nebula-metad", "--flagfile=/usr/local/nebula/etc/nebula-metad.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as storaged

RUN groupadd -r nebula && useradd -r -g nebula -m -d /home/nebula nebula

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-common.rpm /tmp/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-storage.rpm /tmp/nebula-storaged.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh /tmp/*.rpm \
  && mkdir -p ./{logs,data,pids} \
  && chown -R nebula:nebula /usr/local/nebula \
  && rm -rf /tmp/*.rpm

EXPOSE 9777 9778 9779 9780 19779 19780

USER nebula
ENTRYPOINT ["/usr/local/nebula/bin/nebula-storaged", "--flagfile=/usr/local/nebula/etc/nebula-storaged.conf", "--daemonize=false", "--containerized=true"]

FROM centos:7 as tools

RUN groupadd -r nebula && useradd -r -g nebula -m -d /home/nebula nebula

COPY --from=builder /home/nebula/BUILD/pkg-build/cpack_output/nebula-*-tool.rpm /tmp/nebula-tool.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh /tmp/*.rpm \
  && chown -R nebula:nebula /usr/local/nebula \
  && rm -rf /tmp/*.rpm

USER nebula
# default entrypoint
ENTRYPOINT ["/usr/local/nebula/bin/db_dump"]
