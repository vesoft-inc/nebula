nebula_add_test(
    NAME idgenerator_test
    SOURCES IdGeneratorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_concurrent_obj>
        $<TARGET_OBJECTS:idgenerator_obj>
    LIBRARIES
        gtest gtest_main
)
