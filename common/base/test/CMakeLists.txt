add_executable(cord_test CordTest.cpp $<TARGET_OBJECTS:base_obj>)
target_link_libraries(
    cord_test
    folly
    gtest
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME cord_test COMMAND cord_test)


add_executable(cord_bm CordBenchmark.cpp $<TARGET_OBJECTS:base_obj>)
target_link_libraries(
    cord_bm
    follybenchmark
    folly
    glog
    gflags
    boost_regex
    double-conversion
    dl
    -pthread
)

add_executable(murmurhash2_test MurmurHash2Test.cpp)
target_link_libraries(
    murmurhash2_test
    gtest
    gtest_main
    folly
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME murmurhash2_test COMMAND murmurhash2_test)

add_executable(hash_bm HashBenchmark.cpp)
target_link_libraries(
    hash_bm
    follybenchmark
    folly
    glog
    gflags
    boost_regex
    double-conversion
    dl
    -pthread
)
