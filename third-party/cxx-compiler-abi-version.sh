#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -e
abi="unknown"
cxx_cmd=${CXX:-g++}
link_flags="-std=c++14 -static-libstdc++ -static-libgcc"
[[ $disable_cxx11_abi -ne 0 ]] && extra_flags="$extra_flags -D_GLIBCXX_USE_CXX11_ABI=0"
tmpdir=$(mktemp -q -d /tmp/nebula-compiler-test.XXXX 2>/dev/null)
object=$tmpdir/a.out.o

$cxx_cmd $link_flags $extra_flags -g -x c++ - -c -o $object > /dev/null <<EOF
#include <string>
void foobar(std::string) {
}
EOF

string=$(nm -C $object | sed -nr 's/.*foobar.*(std.*string).*/\1/p')

[[ $string = 'std::__cxx11::basic_string' ]] && abi=11
[[ $string = 'std::string' ]] && abi=98

echo $abi
