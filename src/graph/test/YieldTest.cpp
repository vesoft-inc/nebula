/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"

namespace nebula {
namespace graph {

class YieldTest : public TestBase {
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


TEST_F(YieldTest, Basic) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"1"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected{
            {1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1+1, '1+1', (int)3.14, (string)(1+1), (string)true";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"(1+1)"}, {"\"1+1\""}, {"(int)3.140000"}, {"(string)(1+1)"}, {"(string)true"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t, std::string, std::string>> expected{
            {2, "1+1", 3, "2", "true"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD \"Hello\", hash(\"Hello\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"\"Hello\""}, {"hash(\"Hello\")"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<std::string, int64_t>> expected{
            {"Hello", std::hash<std::string>()("Hello")}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, hashCall) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(\"Boris\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash(\"Boris\")"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t>> expected{
            std::hash<std::string>{}("Boris")
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(123)";
        auto code = client->execute(query, resp);
        std::vector<std::string> expectedColNames{
            {"hash(123)"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            std::hash<int64_t>{}(123)
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(123 + 456)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash((123+456))"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t>> expected{
            std::hash<int64_t>{}(123 + 456)
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(123.0)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash(123.000000)"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t>> expected{
            std::hash<double>{}(123.0)
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(!0)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash(!(0))"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t>> expected{
            std::hash<bool>{}(!0)
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, Logic) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD NOT 0 || 0 AND 0 XOR 0";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            1
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD !0 OR 0 && 0 XOR 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            0
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD (NOT 0 || 0) AND 0 XOR 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            1
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 2.5 % 1.2 ^ 1.6";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<double>> expected{
            2.5
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD (5 % 3) ^ 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            3
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, inCall) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD udf_is_in(1,0,1,2), 123";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *resp.get_error_msg();
        std::vector<std::string> expectedColNames{
            {"udf_is_in(1,0,1,2)"}, {"123"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<bool, uint64_t>> expected{
            {true, 123}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}
}   // namespace graph
}   // namespace nebula
