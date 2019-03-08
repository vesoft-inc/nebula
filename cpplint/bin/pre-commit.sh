#!/bin/bash

# Copyright (c) 2018 - present, VE Software Inc. All rights reserved
#
# This source code is licensed under Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory)

CPPLINT=`dirname $0`/../../cpplint/cpplint.py

if [ $# -eq 0 ];then
    # Since cpplint.py could only apply on our working tree,
    # so we forbid committing with unstaged changes present.
    # Otherwise, lints can be bypassed via changing without commit.
    if ! git diff-files --exit-code --quiet
    then
        echo "You have unstaged changes, please stage or stash them first."
        exit 1
    fi
    CHECK_FILES=$(git diff --name-only --diff-filter=ACMRTUXB HEAD | egrep '.*\.cpp$|.*\.h$|.*\.inl$' | grep -v 'com_vesoft_client_NativeClient.h')
else
    CHECK_FILES=$(find $@ -not \( -path src/CMakeFiles -prune \) \
                          -not \( -path src/interface/gen-cpp2 -prune \) \
                          -name "*.[h]" -o -name "*.cpp" -o -name '*.inl' \
                          | grep -v 'GraphScanner.*' | grep -v 'GraphParser.*')
fi

# No changes on interested files
if [[ -z $CHECK_FILES ]]
then
    exit 0
fi

echo "Performing C++ linters..."

CPPLINT_EXTENS=cpp,h,inl
CPPLINT_FILTER=-whitespace/indent,-build/include_what_you_use,-readability/todo,-build/include,-build/header_guard,-runtime/references,-build/c++11

python $CPPLINT --quiet --extensions=$CPPLINT_EXTENS \
                --filter=$CPPLINT_FILTER --linelength=100 $CHECK_FILES 2>&1

result=$?
if [ $result -eq 0 ]
then
    exit 0
else
    echo "cpplint code style check failed, please fix and recommit."
    exit 1
fi
