#!/bin/bash

#  used to build nebula rpm files
# ./make-srpm.sh -v <version> -p <build path>, the version should be match tag name

# the format of publish version is 'version-release'
version=""
rpmbuilddir=""

prefix="/usr/local/nebula"
bindir="$prefix/bin"
datadir="$prefix/scripts"
sysconfdir="$prefix/etc"
sharedir="$prefix/share"
resourcesdir="$sharedir/resources"

# parsing arguments
while getopts v:p: opt;
do
    case $opt in
        v)
            version=$OPTARG
            ;;
        p)
            rpmbuilddir=$OPTARG
            ;;
        ?)
            echo "Invalid option, use default arguments"
            ;;
    esac
done

# version is null, get from tag name
[[ -z $version ]] && version=`git describe --match 'v*' | sed 's/^v//'`
[[ -z ${rpmbuilddir} ]] && rpmbuilddir="/tmp"

if [[ -z $version ]]; then
    echo "version is null, exit"
    exit -1
fi

rpm_version=""
rpm_release=""

# get release if the version has '-'
if expr index $version '-' > /dev/null; then
    rpm_version=`echo $version | cut -d - -f 1`
    rpm_release=`echo $version | cut -d - -f 2`
else
    rpm_version=$version
    rpm_release=0
fi

echo "current version is [ $rpm_version ], release is [$rpm_release]"
echo "current rpmbuild is [ $rpmbuilddir ]"

# modify nebula.spec's version
sed -i "s/@VERSION@/$rpm_version/g" nebula.spec
sed -i "s/@RELEASE@/$rpm_release/g" nebula.spec

# do rpmbuild
rpmbuild -D"_topdir $rpmbuilddir" -D"_bindir $bindir" \
         -D"_datadir $datadir" -D"_sysconfdir $sysconfdir" \
         -D"_sharedir $sharedir" -D"_resourcesdir $resourcesdir" \
         -D"_version $rpm_version" -D"_release $rpm_release" \
         -D"_install_dir $prefix" \
         -ba nebula.spec
