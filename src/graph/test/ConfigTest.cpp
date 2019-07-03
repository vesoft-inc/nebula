
/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(load_config_interval_secs);

namespace nebula {
namespace graph {

class ConfigTest: public TestBase {
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

TEST_F(ConfigTest, ConfigTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);

    // set/get without declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES storage:k0=123";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GET VARIABLES storage:k0";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // update immutable config will fail, read-only
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES storage:k0=0 AS INT";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES storage:k0=123";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES storage:k0";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"STORAGE", "k0", "INT64", "IMMUTABLE", "0"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // set and get config after declaration
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES storage:k1=0 AS INT MUTABLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES storage:k1=123";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES storage:k1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"STORAGE", "k1", "INT64", "MUTABLE", "123"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES meta:k1=1.0 AS DOUBLE MUTABLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES meta:k1=3.14";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES meta:k1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"META", "k1", "DOUBLE", "MUTABLE", "3.140000"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES storage:k2=false AS BOOL MUTABLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES storage:k2=True";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES storage:k2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"STORAGE", "k2", "BOOL", "MUTABLE", "True"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES meta:k2=nebula AS STRING MUTABLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES meta:k2=abc";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES meta:k2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"META", "k2", "STRING", "MUTABLE", "abc"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // list configs in a specified module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES storage";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES meta";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(2, resp.get_rows()->size());
    }
    // set and get a config of all module
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES meta:k3=nebula AS STRING MUTABLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        declare = "DECLARE VARIABLES graph:k3=nebula AS STRING MUTABLE";
        code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        declare = "DECLARE VARIABLES storage:k3=nebula AS STRING MUTABLE";
        code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES k3=vesoft";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES k3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"GRAPH", "k3", "STRING", "MUTABLE", "vesoft"},
            {"META", "k3", "STRING", "MUTABLE", "vesoft"},
            {"STORAGE", "k3", "STRING", "MUTABLE", "vesoft"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES graph:k3=abc";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "UPDATE VARIABLES meta:k3=bcd";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "UPDATE VARIABLES storage:k3=cde";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES k3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"GRAPH", "k3", "STRING", "MUTABLE", "abc"},
            {"META", "k3", "STRING", "MUTABLE", "bcd"},
            {"STORAGE", "k3", "STRING", "MUTABLE", "cde"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


}  // namespace graph
}  // namespace nebula
