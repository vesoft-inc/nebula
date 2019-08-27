
/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestBase.h"
#include "graph/test/TestEnv.h"
#include "meta/ClientBasedGflagsManager.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class ConfigTest : public TestBase {
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

void mockRegisterGflags(meta::ClientBasedGflagsManager* cfgMan) {
    {
        auto module = meta::cpp2::ConfigModule::STORAGE;
        auto type = meta::cpp2::ConfigType::INT64;
        int64_t value = 0L;
        cfgMan->registerConfig(module, "k0", type, meta::cpp2::ConfigMode::IMMUTABLE,
                               meta::toThriftValueStr(type, value)).get();
        cfgMan->registerConfig(module, "k1", type, meta::cpp2::ConfigMode::MUTABLE,
                               meta::toThriftValueStr(type, value)).get();
    }
    {
        auto module = meta::cpp2::ConfigModule::STORAGE;
        auto type = meta::cpp2::ConfigType::BOOL;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        bool value = false;
        cfgMan->registerConfig(module, "k2", type, mode, meta::toThriftValueStr(type, value)).get();
    }
    {
        auto module = meta::cpp2::ConfigModule::META;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        auto type = meta::cpp2::ConfigType::DOUBLE;
        double value = 1.0;
        cfgMan->registerConfig(module, "k1", type, mode, meta::toThriftValueStr(type, value)).get();
    }
    {
        auto module = meta::cpp2::ConfigModule::META;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        auto type = meta::cpp2::ConfigType::STRING;
        std::string value = "nebula";
        cfgMan->registerConfig(module, "k2", type, mode, meta::toThriftValueStr(type, value)).get();
    }
    {
        auto type = meta::cpp2::ConfigType::STRING;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        std::string value = "nebula";
        cfgMan->registerConfig(meta::cpp2::ConfigModule::GRAPH, "k3",
                               type, mode, meta::toThriftValueStr(type, value)).get();
        cfgMan->registerConfig(meta::cpp2::ConfigModule::META, "k3",
                               type, mode, meta::toThriftValueStr(type, value)).get();
        cfgMan->registerConfig(meta::cpp2::ConfigModule::STORAGE, "k3",
                               type, mode, meta::toThriftValueStr(type, value)).get();
    }
}

TEST_F(ConfigTest, ConfigTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);

    mockRegisterGflags(gEnv->gflagsManager());

    // set/get without declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES storage:notRegistered=123";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GET VARIABLES storage:notRegistered";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // update immutable config will fail, read-only
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE VARIABLES storage:k0=123";
        auto code = client->execute(query, resp);
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
        std::string query = "UPDATE VARIABLES storage:k1=123";
        auto code = client->execute(query, resp);
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
        std::string query = "UPDATE VARIABLES meta:k1=3.14";
        auto code = client->execute(query, resp);
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
        std::string query = "UPDATE VARIABLES storage:k2=True";
        auto code = client->execute(query, resp);
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
        std::string query = "UPDATE VARIABLES meta:k2=abc";
        auto code = client->execute(query, resp);
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
        ASSERT_EQ(4, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW VARIABLES meta";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
    }
    // set and get a config of all module
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
