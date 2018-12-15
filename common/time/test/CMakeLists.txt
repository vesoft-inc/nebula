add_executable(
    duration_test
    DurationTest.cpp
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:thread_obj>
)

target_link_libraries(
    duration_test
    folly
    gtest
    glog
    gflags
    double-conversion
    event
    dl
    -pthread
)

add_test(NAME duration_test COMMAND duration_test)

add_executable(
    duration_bm
    DurationBenchmark.cpp
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:thread_obj>
)

target_link_libraries(
    duration_bm
    follybenchmark
    folly
    glog
    gflags
    boost_regex
    double-conversion
    event
    dl
    -pthread
)
