set(RAFTEX_TEST_LIBS
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:disk_man_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:log_str_list_iterator_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:ssl_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:meta_client_stats_obj>
    $<TARGET_OBJECTS:storage_client_stats_obj>
    $<TARGET_OBJECTS:kv_stats_obj>
)


nebula_add_test(
    NAME
        leader_election_test
    SOURCES
        LeaderElectionTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        log_append_test
    SOURCES
        LogAppendTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        log_cas_test
    SOURCES
        LogCASTest.cpp
        RaftexTestBase.cpp
         TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        learner_test
    SOURCES
        LearnerTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        log_command_test
    SOURCES
        LogCommandTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        raft_case_test
    SOURCES
        RaftCase.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        leader_transfer_test
    SOURCES
        LeaderTransferTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

nebula_add_test(
    NAME
        member_change_test
    SOURCES
        MemberChangeTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    OBJECTS
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

