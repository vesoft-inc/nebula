add_custom_target(
    common ALL
    DEPENDS base_obj time proc network fs
)

add_subdirectory(base)
add_subdirectory(time)
add_subdirectory(proc)
add_subdirectory(network)
add_subdirectory(fs)

