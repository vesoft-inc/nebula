set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)

add_custom_command(
    OUTPUT
    gen-cpp2/old_meta_constants.cpp
    gen-cpp2/old_meta_data.cpp
    gen-cpp2/old_meta_types.cpp
    COMMAND "${THRIFT1}" "--strict" "--allow-neg-enum-vals" "--templates" "${THRIFT_TEMPLATES}" "--gen" "mstch_cpp2:include_prefix=\"meta/upgradeData/oldThrift\",process_in_event_base,stack_arguments"   "-o" "." "${CMAKE_CURRENT_SOURCE_DIR}/old_meta.thrift"
    DEPENDS old_meta.thrift
)

nebula_add_library(
    old_meta_thrift_obj OBJECT
    gen-cpp2/old_meta_constants.cpp
    gen-cpp2/old_meta_data.cpp
    gen-cpp2/old_meta_types.cpp
)

target_compile_options(old_meta_thrift_obj PRIVATE "-Wno-pedantic")
target_compile_options(old_meta_thrift_obj PRIVATE "-Wno-extra")
