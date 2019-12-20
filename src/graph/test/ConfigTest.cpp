
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
#include "storage/test/TestUtils.h"

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

std::vector<meta::cpp2::ConfigItem> mockRegisterGflags() {
    std::vector<meta::cpp2::ConfigItem> configItems;
    {
        auto module = meta::cpp2::ConfigModule::STORAGE;
        auto type = meta::cpp2::ConfigType::INT64;
        int64_t value = 0L;
        configItems.emplace_back(meta::toThriftConfigItem(module, "k0", type,
                                 meta::cpp2::ConfigMode::IMMUTABLE,
                                 meta::toThriftValueStr(type, value)));
        configItems.emplace_back(meta::toThriftConfigItem(module, "k1", type,
                                 meta::cpp2::ConfigMode::MUTABLE,
                                 meta::toThriftValueStr(type, value)));
    }
    {
        auto module = meta::cpp2::ConfigModule::STORAGE;
        auto type = meta::cpp2::ConfigType::BOOL;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        bool value = false;
        configItems.emplace_back(meta::toThriftConfigItem(module, "k2", type, mode,
                                 meta::toThriftValueStr(type, value)));
    }
    {
        auto module = meta::cpp2::ConfigModule::GRAPH;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        auto type = meta::cpp2::ConfigType::DOUBLE;
        double value = 1.0;
        configItems.emplace_back(meta::toThriftConfigItem(module, "k1", type, mode,
                                 meta::toThriftValueStr(type, value)));
    }
    {
        auto module = meta::cpp2::ConfigModule::GRAPH;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        auto type = meta::cpp2::ConfigType::STRING;
        std::string value = "nebula";
        configItems.emplace_back(meta::toThriftConfigItem(module, "k2", type, mode,
                                 meta::toThriftValueStr(type, value)));
    }
    {
        auto type = meta::cpp2::ConfigType::STRING;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        std::string value = "nebula";
        configItems.emplace_back(meta::toThriftConfigItem(meta::cpp2::ConfigModule::GRAPH, "k3",
                                 type, mode, meta::toThriftValueStr(type, value)));
        configItems.emplace_back(meta::toThriftConfigItem(meta::cpp2::ConfigModule::STORAGE, "k3",
                                 type, mode, meta::toThriftValueStr(type, value)));
    }
    {
        auto module = meta::cpp2::ConfigModule::STORAGE;
        auto type = meta::cpp2::ConfigType::NESTED;
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        std::string value = R"({
            "disable_auto_compactions":"false",
            "write_buffer_size":"1048576"
        })";
        configItems.emplace_back(meta::toThriftConfigItem(module, "k4", type, mode,
                                 meta::toThriftValueStr(type, value)));
    }
    return configItems;
}

TEST_F(ConfigTest, ConfigTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);

    auto configItems = mockRegisterGflags();
    auto ret = gEnv->gflagsManager()->registerGflags(std::move(configItems));
    ASSERT_TRUE(ret.ok());

    // set/get without declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:notRegistered=123";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GET CONFIGS storage:notRegistered";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // update immutable config will fail, read-only
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k0=123";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k0";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"STORAGE", "k0", "INT64", "IMMUTABLE", "0"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // set and get config after declaration
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k1=123";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"STORAGE", "k1", "INT64", "MUTABLE", "123"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k1=3.14";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS graph:k1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>>
            expected { {"GRAPH", "k1", "DOUBLE", "MUTABLE", "3.140000"} };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k2=True";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS storage:k2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"STORAGE", "k2", "BOOL", "MUTABLE", "True"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k2=abc";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS graph:k2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"GRAPH", "k2", "STRING", "MUTABLE", "abc"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // list configs in a specified module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS storage";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(5, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CONFIGS graph";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // set and get a config of all module
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS k3=vesoft";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS k3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"GRAPH", "k3", "STRING", "MUTABLE", "vesoft"},
            {"STORAGE", "k3", "STRING", "MUTABLE", "vesoft"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS graph:k3=abc";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "UPDATE CONFIGS storage:k3=cde";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET CONFIGS k3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 5>> expected {
            {"GRAPH", "k3", "STRING", "MUTABLE", "abc"},
            {"STORAGE", "k3", "STRING", "MUTABLE", "cde"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "UPDATE CONFIGS storage:k4 = {"
                                "disable_auto_compactions = true,"
                                "level0_file_num_compaction_trigger=4"
                            "}";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}  // namespace graph
}  // namespace nebula
