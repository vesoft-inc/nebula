#
# Requirements:
# Please provide the following two variables before using these macros:
#   ${THRIFT1} - path/to/bin/thrift1
#   ${THRIFT_TEMPLATES} - path/to/include/thrift/templates
#   ${THRIFTCPP2} - path/to/lib/thriftcpp2
#
#
# bypass_source_check
# This tells cmake to ignore if it doesn't see the following sources in
# the library that will be installed. Thrift files are generated at compile
# time so they do not exist at source check time
#
# Params:
#   @sources - The list of files to ignore in source check
#

macro(bypass_source_check sources)
set_source_files_properties(
  ${sources}
    PROPERTIES GENERATED TRUE
  )
endmacro()


#
# thrift_generate
# This is used to codegen thrift files using the thrift compiler
# Params:
#   @file_name - The name of tge thrift file
#   @services  - A list of services that are declared in the thrift file
#   @output_path - The directory where the thrift file lives
#
# Output:
#  file-cpp2-target     - A custom target to add a dependenct
#  ${file-cpp2-HEADERS} - The generated Header Files.
#  ${file-cpp2-SOURCES} - The generated Source Files.
#
# Notes:
# If any of the fields is empty, it is still required to provide an empty string
#
# When using file_cpp2-SOURCES it should always call:
#   bypass_source_check(${file_cpp2-SOURCES})
# This will prevent cmake from complaining about missing source files
#

macro(thrift_generate file_name services file_path output_path include_prefix)
set("${file_name}-cpp2-HEADERS"
  ${output_path}/gen-cpp2/${file_name}_constants.h
  ${output_path}/gen-cpp2/${file_name}_data.h
  ${output_path}/gen-cpp2/${file_name}_types.h
  ${output_path}/gen-cpp2/${file_name}_types_custom_protocol.h
  ${output_path}/gen-cpp2/${file_name}_types.tcc
)

set("${file_name}-cpp2-SOURCES"
  ${output_path}/gen-cpp2/${file_name}_constants.cpp
  ${output_path}/gen-cpp2/${file_name}_data.cpp
  ${output_path}/gen-cpp2/${file_name}_types.cpp
)

foreach(service ${services})
  set("${file_name}-cpp2-HEADERS"
    ${${file_name}-cpp2-HEADERS}
    ${output_path}/gen-cpp2/${service}.h
    ${output_path}/gen-cpp2/${service}.tcc
    ${output_path}/gen-cpp2/${service}AsyncClient.h
    ${output_path}/gen-cpp2/${service}_custom_protocol.h
  )
  set("${file_name}-cpp2-SOURCES"
    ${${file_name}-cpp2-SOURCES}
    ${output_path}/gen-cpp2/${service}.cpp
    ${output_path}/gen-cpp2/${service}AsyncClient.cpp
    ${output_path}/gen-cpp2/${service}_processmap_binary.cpp
    ${output_path}/gen-cpp2/${service}_processmap_compact.cpp
  )
endforeach()

add_custom_command(
  OUTPUT ${${file_name}-cpp2-HEADERS} ${${file_name}-cpp2-SOURCES}
  COMMAND ${THRIFT1}
    --strict "--allow-neg-enum-vals"
    --templates ${THRIFT_TEMPLATES}
    --gen "mstch_cpp2:include_prefix=${include_prefix},process_in_event_base,stack_arguments"
    --gen "java:hashcode"
    --gen "go"
    -o "." "${file_path}/${file_name}.thrift"
  DEPENDS "${file_path}/${file_name}.thrift"
  COMMENT "Generating thrift files for ${file_name}"
)

bypass_source_check(${file_name}_cpp2-SOURCES)
add_library(
  "${file_name}_thrift_obj"
  OBJECT
  ${${file_name}-cpp2-SOURCES}
)
if(NOT "${file_name}" STREQUAL "common")
add_dependencies(
  "${file_name}_thrift_obj"
  "common_thrift_obj"
)
endif()
endmacro()
