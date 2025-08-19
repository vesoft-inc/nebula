# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


nebula_add_test(
    NAME ann_index_test
    SOURCES AnnIndexTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj> $<TARGET_OBJECTS:vector_index_obj>
    LIBRARIES gtest gtest_main ${VECTOR_INDEX_LIBRARIES} ${THRIFT_LIBRARIES}
)

nebula_add_executable(
    NAME ann_benchmark
    SOURCES ANNBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj> $<TARGET_OBJECTS:memory_obj> $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:time_obj> $<TARGET_OBJECTS:vector_index_obj>
    LIBRARIES gtest gtest_main jemalloc ${VECTOR_INDEX_LIBRARIES} ${THRIFT_LIBRARIES}
)
