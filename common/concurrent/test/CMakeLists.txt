add_executable(
    concurrent_test
    BarrierTest.cpp
    LatchTest.cpp
    $<TARGET_OBJECTS:concurrent_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(
    concurrent_test
    event
    gtest
    gtest_main
    glog
    gflags
    pthread
)
add_test(NAME concurrent_test COMMAND concurrent_test)
