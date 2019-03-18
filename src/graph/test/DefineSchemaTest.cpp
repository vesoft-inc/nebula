/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "fs/TempDir.h"
#include "graph/GraphFlags.h"

namespace nebula {
namespace graph {

class DefineSchemaTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};


TEST_F(DefineSchemaTest, DISABLED_Simple) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE TAG person(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG person";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"email", "string"},
            {"name", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE TAG account(id int, balance double)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG account";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int"},
            {"balance", "double"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE EDGE friend_of person -> person()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE friend_of";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", "person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE EDGE transfer "
                            "account -> account(amount double, time int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE transfer";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 4>> expected{
            {"account", "account", "amount", "double"},
            {"account", "account", "time", "int"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // compound sentences
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE friend_of; DESCRIBE EDGE transfer";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 4>> expected{
            {"account", "account", "amount", "double"},
            {"account", "account", "time", "int"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE transfer; DESCRIBE EDGE friend_of";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", "person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(DefineSchemaTest, metaCommunication) {
    using nebula::meta::TestUtils;
    using nebula::fs::TempDir;
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);

    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto sc = TestUtils::mockServer(FLAGS_meta_server_port, rootPath.path());

    {
        cpp2::ExecutionResponse resp;
        std::string query = "add hosts(\"127.0.0.1:1000\", \"127.0.0.1:1100\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "show hosts";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"127.0.0.1", "1000"},
            {"127.0.0.1", "1100"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "create space (\"default_space\", 9, 3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
