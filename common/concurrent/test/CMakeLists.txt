add_executable(concurrent_test ThreadTest.cpp
        GenericWorkerTest.cpp
        GenericThreadPoolTest.cpp
        ThreadLocalPtrTest.cpp
        $<TARGET_OBJECTS:concurrent_obj>
        $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(concurrent_test ev gtest gtest_main pthread)
add_test(NAME concurrent_test COMMAND concurrent_test)
