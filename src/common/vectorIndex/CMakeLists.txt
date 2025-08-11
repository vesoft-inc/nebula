# Copyright (c) 2025 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library (
  vector_index_obj OBJECT
  VectorIndex.cpp
  IVFIndex.cpp
  HNSWIndex.cpp
)

# Enable OpenMP if available
find_package(OpenMP)
if (OpenMP_CXX_FOUND)
  target_link_libraries(vector_index_obj PUBLIC OpenMP::OpenMP_CXX)
endif()

nebula_add_subdirectory(test)
