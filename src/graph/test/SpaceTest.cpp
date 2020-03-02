/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class SpaceTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getClient();
    }
protected:
    static std::unique_ptr<GraphClient>         client_;
};

std::unique_ptr<GraphClient>         SpaceTest::client_{nullptr};

TEST_F(SpaceTest, TestRename) {
    // rename space which use default config and without schema
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE space_1";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "RENAME SPACE space_1 TO space_2";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "DESC SPACE space_2";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_2", 100, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    // rename space which has schema
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE space_3(partition_num=9, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "USE space_3";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "CREATE TAG tag_1(); CREATE TAG tag_2();";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "RENAME SPACE space_3 TO space_4";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "DESC SPACE space_4";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_4", 9, 1, "utf8", "utf8_bin"}
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));

        cmd = "USE space_4";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "SHOW TAGS";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> tagsExpected{
            {"tag_1"}, {"tag_2"}
        };
        ASSERT_TRUE(verifyResult(resp, tagsExpected, true, {0}));
    }
    // Rename the existent space
    {
        cpp2::ExecutionResponse resp;
        auto cmd = "RENAME SPACE space_2 TO space_4";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // check the number of spaces
    {
        cpp2::ExecutionResponse resp;
        auto cmd = "SHOW SPACES";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> tagsExpected{
            {"space_2"}, {"space_4"}
        };
        ASSERT_TRUE(verifyResult(resp, tagsExpected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto cmd = "DROP SPACE space_2; DROP SPACE space_4;";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SpaceTest, CopySpaceTest) {
    // prepare
    cpp2::ExecutionResponse resp;
    std::string cmd = "CREATE SPACE schema_space(partition_num=9, replica_factor=1)";
    auto code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "USE schema_space";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "CREATE TAG tag_1(name string); CREATE EDGE edge_1(name string);";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "CREATE TAG INDEX tag_index_1 on tag_1(name);"
          "CREATE EDGE INDEX edge_index_1 on edge_1(name);";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    // Copy schemas into empty space
    {
        cmd = "CREATE SPACE empty_space";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "USE empty_space";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "COPY SCHEMA FROM schema_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "SHOW TAGS;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<std::string>> expected1{
                {"tag_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected1, true, {0}));

        cmd = "SHOW EDGES;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected2{
                {"edge_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected2, true, {0}));

        cmd = "SHOW TAG INDEXES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected3{
                {"tag_index_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected3, true, {0}));

        cmd = "SHOW EDGE INDEXES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected4{
                {"edge_index_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected4, true, {0}));
    }
    // Copy schemas without index
    {
        cmd = "CREATE SPACE empty_space_2";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "USE empty_space_2";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "COPY SCHEMA FROM schema_space NO INDEX;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "SHOW TAGS;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<std::string>> expected1{
                {"tag_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected1, true, {0}));

        cmd = "SHOW EDGES;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected2{
                {"edge_1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected2, true, {0}));

        cmd = "SHOW TAG INDEXES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());

        cmd = "SHOW EDGE INDEXES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());
    }
    // Copy schemas into the same space
    {
        cmd = "USE schema_space";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "COPY SCHEMA FROM schema_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Copy schemas into space which has existent tag
    {
        cmd = "COPY SCHEMA FROM schema_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Copy schemas into the not empty space
    {
        cmd = "CREATE SPACE space_9(partition_num=9, replica_factor=1)";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "USE space_9";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "CREATE TAG tag_2(name string); CREATE EDGE edge_2(name string);";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "COPY SCHEMA FROM schema_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "SHOW TAGS;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<std::string>> expected1{
                {"tag_1"}, {"tag_2"},
        };
        ASSERT_TRUE(verifyResult(resp, expected1, true, {0}));

        cmd = "SHOW EDGES;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected2{
                {"edge_1"}, {"edge_2"},
        };
        ASSERT_TRUE(verifyResult(resp, expected2, true, {0}));
    }
    {
        cmd = "DROP SPACE space_9; DROP SPACE empty_space;"
              "DROP SPACE empty_space_2; DROP SPACE schema_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SpaceTest, TestTruncate) {
    FLAGS_heartbeat_interval_secs = 1;
    // prepare
    cpp2::ExecutionResponse resp;
    std::string cmd = "CREATE SPACE current_space(partition_num=9, replica_factor=1)";
    auto code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "USE current_space";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "CREATE TAG person(name string, age int); CREATE EDGE like(likeness double);";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    sleep(FLAGS_heartbeat_interval_secs + 1);

    cmd = "INSERT VERTEX person(name, age) VALUES "
          "hash(\"Lily\"):(\"Lily\", 18), hash(\"Tom\"):(\"Tom\", 19);"
          "INSERT EDGE like(likeness) VALUES hash(\"Lily\") -> hash(\"Tom\"):(85.0)";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

    cmd = "FETCH PROP ON person hash(\"Lily\")";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    ASSERT_EQ(1, resp.get_rows()->size());

    cmd = "FETCH PROP ON like hash(\"Lily\") -> hash(\"Tom\")";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    ASSERT_EQ(1, resp.get_rows()->size());

    // truncate data succeed
    {
        cmd = "TRUNCATE SPACE current_space;";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cmd = "SHOW SPACES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> spaceExpected{
            {"current_space"},
        };
        ASSERT_TRUE(verifyResult(resp, spaceExpected, true));

        cmd = "SHOW TAGS";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> tagExpected{
            {"person"},
        };
        ASSERT_TRUE(verifyResult(resp, tagExpected, true, {0}));

        cmd = "SHOW EDGES";
        code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> edgeExpected{
            {"like"},
        };
        ASSERT_TRUE(verifyResult(resp, edgeExpected, true, {0}));

        sleep(FLAGS_heartbeat_interval_secs + 1);

        cpp2::ExecutionResponse fetchResp;
        cmd = "FETCH PROP ON person hash(\"Lily\")";
        code = client_->execute(cmd, fetchResp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, fetchResp.get_rows());

        cmd = "FETCH PROP ON like hash(\"Lily\") -> hash(\"Tom\")";
        code = client_->execute(cmd, fetchResp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, fetchResp.get_rows());
    }
    cmd = "DROP SPACE current_space";
    code = client_->execute(cmd, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
}
}   // namespace graph
}   // namespace nebula
