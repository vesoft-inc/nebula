nebula_add_executable(
    NAME
        data-inspector
    SOURCES
        Main.cpp
        Inspector.cpp
        Inspector26.cpp
    OBJECTS
        $<TARGET_OBJECTS:disk_man_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:process_obj>
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        ${ROCKSDB_LIBRARIES}
)

install(
    TARGETS
        data-inspector
    PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
    DESTINATION
        bin
    COMPONENT
        tool
)
