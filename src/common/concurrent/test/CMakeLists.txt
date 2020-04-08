nebula_add_test(
    NAME
        concurrent_test
    SOURCES
        BarrierTest.cpp LatchTest.cpp
    LIBRARIES
        concurrent_lib
        thread_lib
        time_lib
        gtest
        gtest_main
)
