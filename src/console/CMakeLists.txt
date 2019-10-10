nebula_add_library(
    console_obj OBJECT
    CliManager.cpp
    CmdProcessor.cpp
)

nebula_add_executable(
    NAME
        nebula
    SOURCES
        NebulaConsole.cpp
    OBJECTS
        $<TARGET_OBJECTS:console_obj>
        $<TARGET_OBJECTS:client_cpp_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
        $<TARGET_OBJECTS:graph_thrift_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:process_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        ${Readline_LIBRARY}
        ${NCURSES_LIBRARY}
)

install(
    TARGETS
        nebula
    DESTINATION
        bin
    COMPONENT
        console
)
