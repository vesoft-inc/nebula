/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"

namespace nebula {
namespace graph {

class YieldTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TraverseTestBase::TearDown();
    }
};


TEST_F(YieldTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1";
        auto code = client_->execute(query, resp);
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
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"(1+1)"}, {"1+1"}, {"(int)3.140000000000000"}, {"(string)(1+1)"}, {"(string)true"}
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
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"Hello"}, {"hash(Hello)"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<std::string, int64_t>> expected{
            {"Hello", std::hash<std::string>()("Hello")}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, HashCall) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD hash(\"Boris\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash(Boris)"}
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
        auto code = client_->execute(query, resp);
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
        auto code = client_->execute(query, resp);
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
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"hash(123.000000000000000)"}
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
        auto code = client_->execute(query, resp);
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD NOT 0 || 0 AND 0 XOR 0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            1
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD !0 OR 0 && 0 XOR 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            0
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD (NOT 0 || 0) AND 0 XOR 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            1
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 2.5 % 1.2 ^ 1.6";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<double>> expected{
            2.5
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD (5 % 3) ^ 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected{
            3
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, InCall) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD udf_is_in(1,0,1,2), 123";
        auto code = client_->execute(query, resp);
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

TEST_F(YieldTest, YieldPipe) {
    std::string go = "GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team";
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.team WHERE 1 == 1";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.team WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.*";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string, int64_t, std::string> result(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.* WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string, int64_t, std::string> result(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.*,hash(123) as hash WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string, int64_t, std::string, int64_t> result(
                    player.name(), std::get<2>(serve),
                    std::get<0>(serve), std::hash<int32_t>{}(123));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, YieldPipeDistinct) {
    {
        // duplicate
        std::string fmt = "GO 2 STEPS FROM %ld OVER like YIELD like._dst AS dst"
                            "| YIELD DISTINCT $-.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // duplicate without distinct
        std::string fmt = "GO 2 STEPS FROM %ld OVER like YIELD like._dst AS dst"
                            "| YIELD $-.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // no duplicate
        std::string fmt = "GO FROM %ld OVER like YIELD like._dst AS dst"
                            "| YIELD DISTINCT $-.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, YieldVar) {
    std::string var = " $var = GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team;";
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.team WHERE 1 == 1";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.team WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string> result(std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.*";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<std::string, int64_t, std::string> result(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.* WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string, int64_t, std::string> result(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $var.*, hash(123) as hash WHERE $var.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t>> expected;
        for (auto &serve : player.serves()) {
            if (std::get<2>(serve) <= 2005) {
                continue;
            }
            std::tuple<std::string, int64_t, std::string, int64_t> result(
                    player.name(), std::get<2>(serve),
                    std::get<0>(serve), std::hash<int32_t>{}(123));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, YieldVarDistinct) {
    {
        // duplicate
        std::string fmt = "$a = GO 2 STEPS FROM %ld OVER like YIELD like._dst AS dst;"
                            "YIELD DISTINCT $a.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // duplicate without distinct
        std::string fmt = "$a = GO 2 STEPS FROM %ld OVER like YIELD like._dst AS dst;"
                            "YIELD $a.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // no duplicate
        std::string fmt = "$a = GO FROM %ld OVER like YIELD like._dst AS dst;"
                            "YIELD DISTINCT $a.dst AS dst";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, Error) {
    {
        cpp2::ExecutionResponse resp;
        // Reference input in a single yield sentence is meaningless.
        auto query = "yield $-";
        auto code = client_->execute(query, resp);
        UNUSED(code);
    }
    std::string var = " $var = GO FROM %ld OVER serve YIELD "
                     "$^.player.name AS name, serve.start_year AS start, $$.team.name AS team;";
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Not support reference input and variable
        auto fmt = var + "YIELD $var.team WHERE $-.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Not support reference two diffrent variable
        auto fmt = var + "YIELD $var.team WHERE $var1.start > 2005";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Reference a non-existed prop is meaningless.
        auto fmt = var + "YIELD $var.abc";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Reference a non-existed prop is meaningless.
        std::string fmt = "GO FROM %ld OVER like | YIELD $-.abc;";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Reference properties in single yield sentence is meaningless.
        auto fmt = var + "YIELD $$.a.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD $^.a.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = var + "YIELD a.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(YieldTest, calculateOverflow) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 9223372036854775807+1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775807-2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775807+-2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1-(-9223372036854775807)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 9223372036854775807*2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775807*-2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 9223372036854775807*-2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775807*2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1/0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 2%0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 9223372036854775807";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Negation of -9223372036854775808 incurs a runtime error under UBSan
#ifndef BUILT_WITH_SANITIZER
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775808";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
#endif
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -2*4611686018427387904";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775808*1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD -9223372036854775809";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(YieldTest, AggCall) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD COUNT(1), $-.name";
        auto code = client_->execute(query, resp);
        // Error would be reported when no input
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD COUNT(*), 1+1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected{
            {1, 2}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Carmelo Anthony"];
        auto *fmt = "GO FROM %ld OVER like "
                    "YIELD $$.player.age AS age, "
                    "like.likeness AS like"
                    "| YIELD COUNT(*), $-.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Test input
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Carmelo Anthony"];
        auto *fmt = "GO FROM %ld OVER like "
                    "YIELD $$.player.age AS age, "
                    "like.likeness AS like"
                    "| YIELD AVG($-.age), SUM($-.like), COUNT(*), 1+1";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<double, uint64_t, uint64_t, int64_t>> expected = {
                {34.666666666666664, 270, 3, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Yield field has not input
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Carmelo Anthony"];
        auto *fmt = "GO FROM %ld OVER like "
                    "| YIELD COUNT(*)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
                {3},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Yield field has not input
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Carmelo Anthony"];
        auto *fmt = "GO FROM %ld OVER like "
                    "| YIELD 1";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
                {1}, {1}, {1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // input is empty
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Nobody"];
        auto *fmt = "GO FROM %ld OVER like "
                    "| YIELD 1";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.get_rows() == nullptr);
    }
    // Test var
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Carmelo Anthony"];
        auto *fmt = "$var = GO FROM %ld OVER like "
                    "YIELD $$.player.age AS age, "
                    "like.likeness AS like;"
                    "YIELD AVG($var.age), SUM($var.like), COUNT(*)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<double, uint64_t, uint64_t>> expected = {
                {34.666666666666664, 270, 3},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, EmptyInput) {
    std::string name = "NON EXIST VERTEX ID";
    int64_t nonExistPlayerID = std::hash<std::string>()(name);
    auto iter = players_.begin();
    while (iter != players_.end()) {
        if (iter->vid() == nonExistPlayerID) {
            ++nonExistPlayerID;
            iter = players_.begin();
            continue;
        }
        ++iter;
    }

    {
        cpp2::ExecutionResponse resp;
        std::string fmt = "GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team"
                     "| YIELD $-.team";
        auto query = folly::stringPrintf(fmt.c_str(), nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"$-.team"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string fmt = " $var = GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team;"
                     "YIELD $var.team";
        auto query = folly::stringPrintf(fmt.c_str(), nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"$var.team"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team "
                    "| YIELD $-.name as name WHERE $-.start > 20000 "
                    "| YIELD $-.name AS name";
        auto query = folly::stringPrintf(fmt, players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(YieldTest, DuplicateColumn) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "YIELD 1, 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"1"}, {"1"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected{
            {1, 1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        std::string go = "GO FROM %ld OVER serve YIELD "
                         "$^.player.name as team, serve.start_year as start, $$.team.name as team";
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| YIELD $-.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
}

TEST_F(YieldTest, PipeYieldGo) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        std::string fmt = "GO FROM %ld OVER serve YIELD serve._src as id |"
                          "YIELD $-.id as id | "
                          "GO FROM $-.id OVER serve YIELD $$.team.name as name";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected = {
            {"Spurs"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        std::string fmt = "$var = GO FROM %ld OVER serve YIELD serve._src as id |"
                          "YIELD $-.id as id; "
                          "GO FROM $var.id OVER serve YIELD $$.team.name as name";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected = {
            {"Spurs"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        std::string fmt = "$var = GO FROM %ld OVER serve YIELD serve._src as id;"
                          "$var2 = YIELD $var.id as id; "
                          "GO FROM $var2.id OVER serve YIELD $$.team.name as name";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected = {
            {"Spurs"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(YieldTest, WithComment) {
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute("YIELD 1--1", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::tuple<int64_t>> expected = {
            {2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute("YIELD 1-- 1", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();

        std::vector<std::tuple<int64_t>> expected = {
            {1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}
}   // namespace graph
}   // namespace nebula

