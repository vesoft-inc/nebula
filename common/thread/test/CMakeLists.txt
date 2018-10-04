add_executable(
        thread_test
        ThreadTest.cpp
        GenericWorkerTest.cpp
        GenericThreadPoolTest.cpp
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:concurrent_obj>
        $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(thread_test event gtest gtest_main glog gflags pthread)
add_test(NAME thread_test COMMAND thread_test)
