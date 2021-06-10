set(RAFTEX_TEST_LIBS
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:disk_man_obj>
    $<TARGET_OBJECTS:common_raftex_thrift_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_time_obj>
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
        snapshot_test
    SOURCES
        SnapshotTest.cpp
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

