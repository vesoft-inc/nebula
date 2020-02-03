#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

cxx_cmd=${CXX:-g++}
link_flags="-std=c++14 -static-libstdc++ -static-libgcc"

# Compile source from stdin and run.
# input envs:
#   $output, which is the expected output.
#   $exit_code, which is the exit status of the compiled program.
function compile-and-run {
    local tmpdir=$(mktemp -q -d /tmp/nebula-compiler-test.XXXX 2>/dev/null)
    [[ -z $tmpdir ]] && {
        echo "Failed"
        echo "Create directory /tmp/nebula-compiler-test.XXXX failed."
        exit 1;
    }

    local exe=$tmpdir/a.out
    local logfile=$tmpdir/testing.log
    $cxx_cmd $link_flags $extra_flags -g0 -x c++ - -o $exe &> $logfile

    [[ $? -eq 0 ]] || {
        echo "Failed"
        echo "See $logfile for details." 1>&2;
        exit 1;
    }

    true_output=$($exe 2>&1)
    true_exit_code=$?
    # Convert non-zero to 1
    [[ $true_exit_code -ne 0 ]] && true_exit_code=1

    if [[ -n "$output" ]] && [[ ! "$true_output" = "$output" ]]
    then
        echo "expected: $output" >> $logfile
        echo "actual: $true_output" >> $logfile
        echo "Failed"
        echo "See $logfile for details." 1>&2;
        exit 1;
    fi

    if [[ -n "$exit_code" ]] && [[ $true_exit_code -ne $exit_code ]]
    then
        echo "expected: $exit_code" >> $logfile
        echo "actual: $true_exit_code" >> $logfile
        echo "Failed"
        echo "See $logfile for details." 1>&2;
        exit 1;
    fi

    rm -rf $tmpdir
}

# make_unique, regex, (templated)lambda, move capture
echo -n "Performing regular C++14 tests..."
output="Nebula" exit_code=0 compile-and-run <<EOF
#include <iostream>
#include <string>
#include <regex>
#include <memory>
int main() {
    auto hello = std::make_unique<std::string>("Hello");
    auto _666_ = [hello = std::move(hello)] (auto bravo) {
        return *hello + " " + *bravo;
    } (std::make_unique<std::string>("Nebula Graph"));
    std::smatch sm;
    if (std::regex_match(_666_, sm,
                std::regex(R"(^(Hello) (Nebula) (Graph)$)"))) {
        std::cout << sm[2].str() << std::endl;
    }
    return 0;
}
EOF
echo "OK"

echo -n "Performing LeakSanitizer tests..."
exit_code=1 extra_flags="-fsanitize=address -g" compile-and-run <<EOF
int main() {
    new int;
    return 0;
}
EOF
echo "OK"

exit 0
