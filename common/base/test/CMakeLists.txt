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

add_executable(murmurhash2_test MurmurHash2Test.cpp $<TARGET_OBJECTS:base_obj>)
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

add_executable(
    configuration_test
    ConfigurationTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    configuration_test
    gtest
    gtest_main
    folly
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME configuration_test COMMAND configuration_test)

add_executable(
    status_test
    StatusTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    status_test
    gtest
    gtest_main
    folly
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME status_test COMMAND status_test)

add_executable(
    status_or_test
    StatusOrTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    status_or_test
    gtest
    gtest_main
    folly
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME status_or_test COMMAND status_or_test)

add_executable(hash_bm HashBenchmark.cpp $<TARGET_OBJECTS:base_obj>)
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
