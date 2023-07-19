#!/bin/bash

# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

CPPLINT=`dirname $0`/../../.linters/cpp/cpplint.py
CHECKKEYWORD=`dirname $0`/../../.linters/cpp/checkKeyword.py

echo "Performing checkout keyword..."
python3 $CHECKKEYWORD

if [ $? -ne 0 ]; then
    echo "Checkout keyword failed"
    exit 1
fi

if [ $# -eq 0 ];then
    # Since cpplint.py could only apply on our working tree,
    # so we forbid committing with unstaged changes present.
    # Otherwise, lints can be bypassed via changing without commit.
    if ! git diff-files --exit-code --quiet
    then
        echo "You have unstaged changes, please stage or stash them first."
        exit 1
    fi
    CHECK_FILES=$(git diff --name-only --diff-filter=ACMRTUXB HEAD | egrep '.*\.cpp$|.*\.h$' | grep -v 'com_vesoft_client_NativeClient.h' | grep -v 'com_vesoft_nebula_NebulaCodec.h')
else
    CHECK_FILES=$(find $@ -not \( -path src/CMakeFiles -prune \) \
                          -not \( -path src/interface/gen-cpp2 -prune \) \
                          -name "*.[h]" -o -name "*.cpp" \
                          | grep -v 'GraphScanner.*' | grep -v 'GraphParser.*' \
                          | grep -v 'com_vesoft_client_NativeClient.h' \
                          | grep -v 'com_vesoft_nebula_NebulaCodec.h')
fi

# No changes on interested files
if [[ -z $CHECK_FILES ]]; then
    echo "There's no source files to perform C++ linters..."
    exit 0
fi

echo "Performing C++ linters..."

CPPLINT_EXTENS=cpp,h
CPPLINT_FILTER=-whitespace/indent,-build/include_what_you_use,-readability/todo,-build/include,-build/header_guard,-runtime/references,-build/c++11

python3 $CPPLINT --quiet --extensions=$CPPLINT_EXTENS \
                --filter=$CPPLINT_FILTER --linelength=100 $CHECK_FILES 2>&1

result=$?
if [ ! $result -eq 0 ]; then
    echo "cpplint code style check failed, please fix and recommit."
    exit 1
fi

echo "Performing C++ code format check..."

CLANG_FALLBACK=/opt/vesoft/toolset/clang/10.0.0/
if [ -z "$CLANG_HOME" ] && [ -d "$CLANG_FALLBACK" ]; then
    CLANG_HOME=$CLANG_FALLBACK
fi

CLANG_FORMAT=$(command -v clang-format-10)
if [ -z "$CLANG_FORMAT" ]; then
    CLANG_FORMAT=$CLANG_HOME/bin/clang-format
fi

CLANG_FORMAT_DIFF=$(command -v clang-format-diff-10)
if [ -z "$CLANG_FORMAT_DIFF" ]; then
    CLANG_FORMAT_DIFF=$CLANG_HOME/share/clang/clang-format-diff.py
fi

if [ -z "$CLANG_FORMAT" ] || [ -z "$CLANG_FORMAT_DIFF" ]; then
    if [ ! -d "$CLANG_HOME" ]; then
        echo "The $CLANG_HOME directory was not found."
    fi
    if [ -z "$CLANG_FORMAT" ]; then
        echo "Could not find clang-format"
    fi
    if [ -z "$CLANG_FORMAT_DIFF" ]; then
        echo "Could not find clang-format-diff"
    fi
    echo "source changes cannot be automatically formatted."
    exit 0
fi

git diff -U0 --no-color --staged | "$CLANG_FORMAT_DIFF" -i -p1 -binary "$CLANG_FORMAT"
git add $CHECK_FILES
