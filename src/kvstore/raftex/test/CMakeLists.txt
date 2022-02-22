set(RAFTEX_TEST_LIBS
    raftex_obj
    wal_obj
    disk_man_obj
    raftex_thrift_obj
    common_thrift_obj
    meta_thrift_obj
    base_obj
    log_str_list_iterator_obj
    datatypes_obj
    wkt_wkb_io_obj
    thread_obj
    fs_obj
    network_obj
    thrift_obj
    time_obj
    ssl_obj
    stats_obj
    meta_client_stats_obj
    storage_client_stats_obj
    kv_stats_obj
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
    LIBRARIES
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
    LIBRARIES
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
    LIBRARIES
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
    LIBRARIES
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
    LIBRARIES
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
    LIBRARIES
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
    LIBRARIES
        ${RAFTEX_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)

