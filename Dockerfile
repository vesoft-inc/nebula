# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

FROM centos:7.6.1810

MAINTAINER darion.wang "darion.wang@vesoft.com"

ARG NEBULA_VERSION

RUN yum -y install wget \
                   openssh-clients \
                   openssh-server net-tools

RUN wget -O nebula.el7-5.x86_64.rpm \
    https://github.com/vesoft-inc/nebula/releases/download/v${NEBULA_VERSION}/nebula-${NEBULA_VERSION}.el7-5.x86_64.rpm && \
    rpm -ivh nebula.el7-5.x86_64.rpm && \
    rm nebula.el7-5.x86_64.rpm && \
    mkdir -p /usr/local/nebula/logs

WORKDIR /root

