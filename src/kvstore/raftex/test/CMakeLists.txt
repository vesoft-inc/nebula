set(RAFTEX_TEST_LIBS
    raftex_lib
    raftex_thrift_lib
    wal_lib
    base_lib
    thread_lib
    fs_lib
    network_lib
    thrift_lib
    time_lib
    kvstore_storage_utils_lib
)


nebula_add_test(
    NAME
        leader_election_test
    SOURCES
        LeaderElectionTest.cpp
        RaftexTestBase.cpp
        TestShard.cpp
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

