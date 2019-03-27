#!/bin/sh

# download deps 

sudo yum -y install  autoconf automake libtool cmake bison unzip boost gperf
sudo yum -y install krb5 openssl openssl-devel libunwind libunwind-devel 
sudo yum -y install ncurses ncurses-devel readline readline-devel 

