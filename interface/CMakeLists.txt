add_custom_command(
    OUTPUT 
        gen-cpp2/GraphDbService.cpp
        gen-cpp2/GraphDbService_client.cpp
        gen-cpp2/GraphDbService_processmap_binary.cpp
        gen-cpp2/GraphDbService_processmap_compact.cpp
        gen-cpp2/vgraph_constants.cpp
        gen-cpp2/vgraph_data.cpp
        gen-cpp2/vgraph_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"interface\",process_in_event_base,stack_arguments" "--gen" "java:hashcode" "--gen" "go" "--gen" "py:new_style"   "-o" "." "./vgraph.thrift"
    DEPENDS vgraph.thrift
)


add_custom_command(
    OUTPUT 
        gen-cpp2/ConsensusService.cpp
        gen-cpp2/ConsensusService_client.cpp
        gen-cpp2/ConsensusService_processmap_binary.cpp
        gen-cpp2/ConsensusService_processmap_compact.cpp
        gen-cpp2/consensus_constants.cpp
        gen-cpp2/consensus_data.cpp
        gen-cpp2/consensus_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"interface\",process_in_event_base,stack_arguments" "--gen" "java:hashcode" "--gen" "go" "--gen" "py:new_style"   "-o" "." "./consensus.thrift"
    DEPENDS consensus.thrift
)


add_custom_target(
    clean-interface
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py"
)


add_library(
    vgraph_thrift_obj OBJECT
    gen-cpp2/GraphDbService.cpp
    gen-cpp2/GraphDbService_client.cpp
    gen-cpp2/GraphDbService_processmap_binary.cpp
    gen-cpp2/GraphDbService_processmap_compact.cpp
    gen-cpp2/vgraph_constants.cpp
    gen-cpp2/vgraph_data.cpp
    gen-cpp2/vgraph_types.cpp
)
add_dependencies(vgraph_thrift_obj tgt_fbthrift)


add_library(
    consensus_thrift_obj OBJECT
    gen-cpp2/ConsensusService.cpp
    gen-cpp2/ConsensusService_client.cpp
    gen-cpp2/ConsensusService_processmap_binary.cpp
    gen-cpp2/ConsensusService_processmap_compact.cpp
    gen-cpp2/consensus_constants.cpp
    gen-cpp2/consensus_data.cpp
    gen-cpp2/consensus_types.cpp
)
add_dependencies(consensus_thrift_obj tgt_fbthrift)
