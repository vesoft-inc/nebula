
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

TEST_F(ConfigTest, ConfigTestWithSpace) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE my_space(partition_num=9, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    FLAGS_load_config_interval_secs = 1;

    // set/get without declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES my_space:storage:k1=123";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "GET VARIABLES my_space:storage:k1 as INT";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // set and get config after declaration
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES my_space:storage:k1=0 AS INT";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES my_space:storage:k1=123";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES my_space:storage:k1 as INT";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, int64_t>>
            expected { {"my_space", "STORAGE", "k1", "int64", 123} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES my_space:meta:k1=1.0 AS DOUBLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES my_space:meta:k1=3.14";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES my_space:meta:k1 as DOUBLE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, double>>
            expected { {"my_space", "META", "k1", "double", 3.14} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES my_space:storage:k2=false AS BOOL";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES my_space:storage:k2=True";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES my_space:storage:k2 as BOOL";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, bool>> expected {
            {"my_space", "STORAGE", "k2", "bool", true}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES my_space:meta:k2=nebula AS STRING";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES my_space:meta:k2=abc";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES my_space:meta:k2 as STRING";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"my_space", "META", "k2", "string", "abc"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // get config of wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES my_space:meta:k2 as INT";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // list configs in all spaces
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(4, resp.get_rows()->size());
    }
    // list configs in a specified space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES my_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(4, resp.get_rows()->size());
    }
    // list configs in a specified module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES my_space:storage";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(2, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES my_space:meta";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(2, resp.get_rows()->size());
    }
}

TEST_F(ConfigTest, ConfigTestWithoutSpace) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    sleep(FLAGS_load_data_interval_secs + 1);
    FLAGS_load_config_interval_secs = 1;

    // set/get without declaration in default space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES :storage:k1=123";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "GET VARIABLES :storage:k1 as INT";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // set and get config after declaration in default space
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES :storage:k1=0 AS INT";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES :storage:k1=123";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES :storage:k1 as INT";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, int64_t>>
            expected { {"", "STORAGE", "k1", "int64", 123} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES :meta:k1=1.0 AS DOUBLE";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES :meta:k1=3.14";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES :meta:k1 as DOUBLE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, double>>
            expected { {"", "META", "k1", "double", 3.14} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES :storage:k2=false AS BOOL";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES :storage:k2=True";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES :storage:k2 as BOOL";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, std::string, std::string, bool>> expected {
            {"", "STORAGE", "k2", "bool", true}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string declare = "DECLARE VARIABLES :meta:k2=nebula AS STRING";
        auto code = client->execute(declare, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string query = "UPDATE VARIABLES :meta:k2=abc";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES :meta:k2 as STRING";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"", "META", "k2", "string", "abc"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // get config of wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET VARIABLES :meta:k2 as INT";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}


}  // namespace graph
}  // namespace nebula
