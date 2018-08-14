add_executable(proc_accessor_test ProcAccessorTest.cpp $<TARGET_OBJECTS:proc_obj>)
target_link_libraries(
    proc_accessor_test
    folly
    gtest
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME proc_accessor_test COMMAND proc_accessor_test)

