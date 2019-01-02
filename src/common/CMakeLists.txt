add_custom_target(
    common ALL
    DEPENDS base_obj time network fs concurrent thread
)

add_subdirectory(base)
add_subdirectory(time)
add_subdirectory(network)
add_subdirectory(thrift)
add_subdirectory(fs)
add_subdirectory(concurrent)
add_subdirectory(thread)
add_subdirectory(process)
