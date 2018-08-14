add_custom_target(
    client ALL
    DEPENDS base_obj vgraph_thrift_obj
)

add_subdirectory(cpp)
