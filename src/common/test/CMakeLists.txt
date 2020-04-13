nebula_add_test(
    NAME nebula_key_utils_test
    SOURCES NebulaKeyUtilsTest.cpp
    OBJECTS $<TARGET_OBJECTS:keyutils_obj>
    LIBRARIES gtest
)