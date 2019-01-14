#!/bin/bash

# Copyright (c) 2018 - present, VE Software Inc. All rights reserved
#
# This source code is licensed under Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory)

PROJECT_PATH=$(cd `dirname $0`; pwd)
PROJECT_NAME="${PROJECT_PATH##*/}"
CPPLINT_FILE=$PROJECT_PATH"/../../cpplint/cpplint.py"

if [ $# -eq 0 ];then
  CHECK_FILES=$(git diff --name-only HEAD)
else
  CHECK_FILES=$(find $1 -not \( -path src/CMakeFiles -prune \)  -not \( -path src/interface/gen-cpp2 -prune \) -name "*.[h]" -o -name "*.cpp" | grep -v 'GraphScanner.*' | grep -v 'GraphParser.*')
fi

CPPLINT_EXTENS=cpp,h
CPPLINT_FITER=-whitespace/indent,-build/include_what_you_use,-readability/todo,-build/include,-build/header_guard
python $CPPLINT_FILE --extensions=$CPPLINT_EXTENS --filter=$CPPLINT_FITER --linelength=100 $CHECK_FILES 2>&1 

result=$?
if [ $result -eq 0 ]
then
  exit 0
else
  echo "cpplint code style check failed, please fix and recommit."
  exit 1
fi
