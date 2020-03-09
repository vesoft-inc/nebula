nebula_add_test(
    NAME reservoir_sampling_test
    SOURCES ReservoirSamplingTest.cpp
    OBJECTS $<TARGET_OBJECTS:time_obj>
    LIBRARIES gtest gtest_main
)
