add_custom_target(
    common ALL
    DEPENDS base_obj time proc network fs concurrent thread
)

add_subdirectory(base)
add_subdirectory(time)
add_subdirectory(proc)
add_subdirectory(network)
add_subdirectory(fs)
add_subdirectory(concurrent)
add_subdirectory(thread)

