#!/bin/bash

# download deps for build nebula
# TODO: we should check dependence's version after adapt to different system versions

sudo yum -y install gcc gcc-c++ libstdc++-static cmake make autoconf automake
sudo yum -y install flex gperf libtool bison unzip boost boost-devel boost-static
sudo yum -y install krb5 openssl openssl-devel libunwind libunwind-devel
sudo yum -y install ncurses ncurses-devel readline readline-devel python
sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel
sudo yum -y install git-lfs

