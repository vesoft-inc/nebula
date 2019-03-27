#!/bin/sh

#
# Create a SRPM which can be used to build nebula
#
# ./make-srpm.sh <rpmbuild dir>
#

rpmbuilddir=$1
if [ "$1" == "" ];then
  rpmbuilddir="/tmp"
fi
prefix="/usr/local"
bindir="$prefix/bin"
datadir="$prefix/scripts"
sysconfdir="$prefix/etc"
version="1.0.0"
RELEASE="1"

echo "current rpmbuild is [ $rpmbuilddir ]"

# because of use the static third-party, the rpmbuild can't get the file,so change the check to warnning
sudo sed -i "s/_unpackaged_files_terminate_build      1/_unpackaged_files_terminate_build      0/g" /usr/lib/rpm/macros

# TODO: get version from .git tag describe

sed -i "s/@VERSION@/$version/g" nebula.spec
sed -i "s/@RELEASE@/$version/g" nebula.spec

rpmbuild -D"_topdir $rpmbuilddir" -D"_bindir $bindir" -D"_datadir $datadir" -D"_sysconfdir $sysconfdir" -ba nebula.spec
