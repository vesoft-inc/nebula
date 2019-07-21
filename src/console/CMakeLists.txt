add_library(console_obj OBJECT CliManager.cpp CmdProcessor.cpp)
nebula_add_base_obj(console_obj)

add_executable(
    nebula
    NebulaConsole.cpp
    $<TARGET_OBJECTS:console_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:thread_obj>
)
nebula_link_libraries(
    nebula
    ${THRIFT_LIBRARIES}
    wangle
    ${Readline_LIBRARY}
    ${NCURSES_LIBRARY}
)

install(TARGETS nebula DESTINATION bin)

#add_subdirectory(test)

