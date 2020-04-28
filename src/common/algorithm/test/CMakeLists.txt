nebula_add_test(
    NAME reservoir_sampling_test
    SOURCES ReservoirSamplingTest.cpp
    OBJECTS $<TARGET_OBJECTS:time_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME topn_test
    SOURCES TopNTest.cpp
    LIBRARIES gtest gtest_main
)
