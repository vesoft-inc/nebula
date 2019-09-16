FROM vesoft/nebula-dev:centos as builder

LABEL maintainer "yee.yi@vesoft.com"

COPY . /home/nebula/BUILD

RUN cd /home/nebula/BUILD/package \
  && . /etc/profile.d/devtoolset-8-enable.sh \
  && ./make_srpm.sh -v $(git rev-parse --short HEAD) -p /home/nebula

FROM centos:7

COPY --from=builder /home/nebula/RPMS/x86_64/*.rpm /usr/local/nebula/

WORKDIR /usr/local/nebula

RUN rpm -ivh *.rpm \
  && mkdir -p ./{logs,data,pids} \
  && rm -rf *.rpm
