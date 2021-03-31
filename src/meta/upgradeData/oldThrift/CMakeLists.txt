set(THRIFT1 ${Fbthrift_BIN})

add_custom_command(
    OUTPUT
    gen-cpp2/old_meta_constants.cpp
    gen-cpp2/old_meta_data.cpp
    gen-cpp2/old_meta_metadata.cpp
    gen-cpp2/old_meta_types.cpp
    COMMAND "${THRIFT1}" "--strict" "--allow-neg-enum-vals" "--gen" "mstch_cpp2:include_prefix=\"meta/upgradeData/oldThrift\",process_in_event_base,stack_arguments"   "-o" "." "${CMAKE_CURRENT_SOURCE_DIR}/old_meta.thrift"
    DEPENDS old_meta.thrift
)

nebula_add_library(
    old_meta_thrift_obj OBJECT
    gen-cpp2/old_meta_constants.cpp
    gen-cpp2/old_meta_data.cpp
    gen-cpp2/old_meta_types.cpp
    gen-cpp2/old_meta_metadata.cpp
)

target_compile_options(old_meta_thrift_obj PRIVATE "-Wno-pedantic")
target_compile_options(old_meta_thrift_obj PRIVATE "-Wno-extra")
target_compile_options(old_meta_thrift_obj PRIVATE "-Wno-deprecated-declarations")
