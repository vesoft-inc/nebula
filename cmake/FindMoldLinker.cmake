# Copyright (c) 2023 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#

find_program (MOLD NAMES "mold")
if (NOT MOLD)
  message(FATAL_ERROR "Could not find the mold linker")
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12.1.0)
  get_filename_component(MOLD_PATH ${MOLD} DIRECTORY)
  nebula_add_exe_linker_flag(-B${MOLD_PATH}/../libexec/mold)
  nebula_add_shared_linker_flag(-B${MOLD_PATH}/../libexec/mold)
else()
  nebula_add_exe_linker_flag(-fuse-ld=mold)
  nebula_add_shared_linker_flag(-fuse-ld=mold)
endif()
