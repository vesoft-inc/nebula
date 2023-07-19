# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

set(THRIFT1 ${Fbthrift_BIN})

set(meta_v2_sources
  gen-cpp2/meta_constants.cpp
  gen-cpp2/meta_data.cpp
  gen-cpp2/meta_metadata.cpp
  gen-cpp2/meta_types.cpp
  )

add_custom_command(
  OUTPUT ${meta_v2_sources}
  COMMAND "${THRIFT1}" "--strict" "--allow-neg-enum-vals" "--gen" "mstch_cpp2:include_prefix=\"meta/upgrade/v2\",process_in_event_base,stack_arguments" "-o" "." "${CMAKE_CURRENT_SOURCE_DIR}/meta.thrift"
  DEPENDS meta.thrift
  )

nebula_add_library(meta_v2_thrift_obj OBJECT ${meta_v2_sources})

target_compile_options(meta_v2_thrift_obj PRIVATE "-Wno-pedantic")
target_compile_options(meta_v2_thrift_obj PRIVATE "-Wno-extra")
target_compile_options(meta_v2_thrift_obj PRIVATE "-Wno-deprecated-declarations")
