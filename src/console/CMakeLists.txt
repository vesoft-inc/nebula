add_library(console_obj OBJECT CliManager.cpp CmdProcessor.cpp)
add_dependencies(console_obj base_obj)

add_executable(
    nebula
    NebulaConsole.cpp
    $<TARGET_OBJECTS:console_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:thread_obj>
)
nebula_link_libraries(
    nebula
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    event
    ${COMPRESSION_LIBRARIES}
    glog
    gflags
    resolv
    ${Readline_LIBRARY}
    ${NCURSES_LIBRARY}
)

install(TARGETS nebula DESTINATION bin)

#add_subdirectory(test)

