# Nebula Graph Database fuzzy testing

Nebula-graph uses [libfuzzer](http://llvm.org/docs/LibFuzzer.html) for fuzzy test.
If you want to use fuzzy test, then you need to use the [Clang](https://clang.llvm.org/) to compile Nebula.
In order to better show the test results, it is recommended to enable the addresssanitizer(-DENABLE_ASAN=ON) option when compiling.

# Running fuzz tests locally

``` shell
    ./parser_fuzzer -dict=nebula.dict
```
