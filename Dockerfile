FROM vesoft/nebula-dev:centos7 as builder

COPY . /home/nebula/BUILD

RUN cd /home/nebula/BUILD/package \
  && ./package.sh -v $(git rev-parse --short HEAD) -n OFF

FROM centos:7

COPY --from=builder /home/nebula/BUILD/build/cpack_output/nebula-*-graph.rpm /usr/local/nebula/nebula-graphd.rpm

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm

EXPOSE 3699 13000 13002

ENTRYPOINT ["./bin/nebula-graphd", "--flagfile=./etc/nebula-graphd.conf", "--daemonize=false"]
