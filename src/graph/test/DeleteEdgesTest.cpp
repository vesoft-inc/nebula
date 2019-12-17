/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class DeleteEdgesTest : public TestBase {
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

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());
    }

    static void TearDownTestCase() {
        ASSERT_TRUE(removeData());
        client_.reset();
    }

protected:
    static AssertionResult prepareSchema();

    static AssertionResult removeData();

    static std::unique_ptr<NebulaClientImpl>         client_;
};

std::unique_ptr<NebulaClientImpl>         DeleteEdgesTest::client_{nullptr};

AssertionResult DeleteEdgesTest::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG person(name string, age int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE friend(intimacy int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE schoolmate(likeness int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE transfer(money int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    return TestOK();
}

AssertionResult DeleteEdgesTest::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed, code:" << static_cast<int>(code);
        }
    }

     return TestOK();
}

TEST_F(DeleteEdgesTest, DeleteEdges) {
    // Insert vertices and edges
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "uuid(\"Zhangsan\"):(\"Zhangsan\", 22), uuid(\"Lisi\"):(\"Lisi\", 23),"
                          "uuid(\"Jack\"):(\"Jack\", 18), uuid(\"Rose\"):(\"Rose\", 19)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE friend(intimacy) VALUES "
                          "uuid(\"Zhangsan\")->uuid(\"Lisi\")@15:(90), "
                          "uuid(\"Zhangsan\")->uuid(\"Jack\")@12:(50),"
                          "uuid(\"Jack\")->uuid(\"Rose\")@13:(100)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "uuid(\"Zhangsan\")->uuid(\"Jack\"):(60),"
                          "uuid(\"Lisi\")->uuid(\"Rose\"):(70)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE transfer(money) VALUES "
                          "uuid(\"Zhangsan\")->uuid(\"Lisi\")@1561013236:(33),"
                          "uuid(\"Zhangsan\")->uuid(\"Lisi\")@1561013237:(77)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Traverse
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"),uuid(\"Jack\") OVER friend "
                          "YIELD $^.person.name, friend.intimacy, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 90, "Lisi"},
            {"Zhangsan", 50, "Jack"},
            {"Jack", 100, "Rose"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"), uuid(\"Lisi\") OVER schoolmate "
                          "YIELD $^.person.name, schoolmate.likeness, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 60, "Jack"},
            {"Lisi", 70, "Rose"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\") OVER transfer YIELD $^.person.name,"
                          "transfer._rank, transfer.money, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 1561013236, 33, "Lisi"},
            {"Zhangsan", 1561013237, 77, "Lisi"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete edges
    {
        cpp2::ExecutionResponse resp;
        std::string cmd ="DELETE EDGE friend uuid(\"Zhangsan\")->uuid(\"Lisi\")@15,"
                         "uuid(\"Jack\")->uuid(\"Rose\")@13";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd ="DELETE EDGE schoolmate uuid(\"Lisi\")->uuid(\"Rose\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd ="DELETE EDGE transfer uuid(\"Zhangsan\")->uuid(\"Lisi\")@1561013237";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Traverse again
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"),uuid(\"Jack\") OVER friend "
                          "YIELD $^.person.name, friend.intimacy, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 50, "Jack"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"), uuid(\"Lisi\") OVER schoolmate "
                          "YIELD $^.person.name, schoolmate.likeness, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 60, "Jack"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"), uuid(\"Jack\") OVER transfer "
                          "YIELD $^.person.name, transfer._rank, transfer.money, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 1561013236, 33, "Lisi"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete non-existing edges and a same edge
    {
        cpp2::ExecutionResponse resp;
        std::string cmd ="DELETE EDGE friend uuid(\"Zhangsan\")->uuid(\"Rose\"), 1008->1009@17,"
                         "uuid(\"Zhangsan\")->uuid(\"Lisi\")@15";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Traverse again
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Zhangsan\"),uuid(\"Jack\") OVER friend "
                          "YIELD $^.person.name, friend.intimacy, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, std::string>;
        std::vector<valueType> expected = {
            {"Zhangsan", 50, "Jack"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
