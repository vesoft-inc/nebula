/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/test/TestEnv.h"
#include "vgraphd/test/TestBase.h"


namespace vesoft {
namespace vgraph {

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


TEST_F(DefineSchemaTest, Simple) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE TAG person(name string, email string, age int16, gender string)";
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
            {"age", "int16"},
            {"gender", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DEFINE TAG account(id int64, balance double)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG account";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int64"},
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
                            "account -> account(amount double, time int64)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE transfer";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 4>> expected{
            {"account", "account", "amount", "double"},
            {"account", "account", "time", "int64"},
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
            {"account", "account", "time", "int64"},
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

}   // namespace vgraph
}   // namespace vesoft
