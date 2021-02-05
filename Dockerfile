FROM vesoft/nebula-dev:centos7 as builder

COPY . /home/nebula/BUILD

ARG BRANCH=master

RUN cd /home/nebula/BUILD/package \
  && ./package.sh -v $(git rev-parse --short HEAD) -n OFF -b ${BRANCH}

FROM centos:7

COPY --from=builder /home/nebula/BUILD/build/cpack_output/nebula-*-common.rpm /usr/local/nebula/nebula-common.rpm
COPY --from=builder /home/nebula/BUILD/build/cpack_output/nebula-*-graph.rpm /usr/local/nebula/nebula-graphd.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm

EXPOSE 9669 19669 19670

ENTRYPOINT ["/usr/local/nebula/bin/nebula-graphd", "--flagfile=/usr/local/nebula/etc/nebula-graphd.conf", "--daemonize=false"]
