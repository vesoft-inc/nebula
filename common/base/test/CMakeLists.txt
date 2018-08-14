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

