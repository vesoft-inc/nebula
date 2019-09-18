nebula_add_test(
    NAME
        concurrent_test
    SOURCES
        BarrierTest.cpp LatchTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:concurrent_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        gtest_main
)
