#!/usr/bin/env bash
#
# This script accepts the following 4 parameters to upload a release asset using the GitHub API v3.
#
# - github_token
# - repo
# - tag
# - filepath
#
# Example:
#
#   upload-github-release-asset.sh github_token=TOKEN repo=vesoft-inc/nebula tag=v0.1.0 filepath=./asset.zip

set -e

for op in $@; do
  eval "$op"
done

GH_RELEASE="https://api.github.com/repos/$repo/releases/tags/$tag"

upload_url=$(curl -s --request GET --url $GH_RELEASE | grep -oP '(?<="upload_url": ")[^"]*' | cut -d'{' -f1)

content_type=$(file -b --mime-type $filepath)

filename=$(basename "$filepath")

echo "Uploading asset... "

curl --silent \
     --request POST \
     --url "$upload_url?name=$filename" \
     --header "authorization: Bearer $github_token" \
     --header "content-type: $content_type" \
     --data-binary @"$filepath"
