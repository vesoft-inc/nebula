#!/usr/bin/env bash
#
# This script  upload package to oss.
#
# - OSS_ENDPOINT
# - OSS_ID
# - OSS_SECRET
# - filepath
# - tag
# - nightly
#
# Example:
#
#   upload-oss.sh OSS_ENDPOINT=xxx OSS_ID=xxx OSS_SECRET=xxx tag=v0.1.0 filepath=xxx
#   upload-oss.sh OSS_ENDPOINT=xxx OSS_ID=xxx OSS_SECRET=xxx filepath=xxx nightly=true

set -e

for op in $@; do
  eval "$op"
done

if [[ $nightly != "" ]]; then
  OSS_SUBDIR=`date -u +%Y.%m.%d`
  OSS_URL="oss://nebula-graph/package"/nightly/${OSS_SUBDIR}
else
  OSS_SUBDIR=`echo $tag |sed 's/^v//'`
  OSS_URL="oss://nebula-graph/package"/${OSS_SUBDIR}
fi

echo "Uploading oss... "
ossutil64 -e ${OSS_ENDPOINT} -i ${OSS_ID} -k ${OSS_SECRET} -f cp ${filepath} ${OSS_URL}/$(basename ${filepath})
