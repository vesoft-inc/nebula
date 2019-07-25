/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace graph {

class SetTest : public TraverseTestBase {
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

TEST_F(SetTest, UnionTest) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : manu.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    manu.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "(GO FROM %ld OVER like | "
                    "GO FROM $- OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like | "
                    "GO FROM $- OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "(GO FROM %ld OVER like | "
                    "GO FROM $- OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER like | "
                    "GO FROM $- OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<1>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "(GO FROM %ld OVER like UNION GO FROM %ld OVER like)"
                    " | GO FROM $- OVER serve"
                    " YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &like : tony.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<1>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
    }
}

TEST_F(SetTest, NoInput) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name";
        auto &nobody = players_["Nobody"];
        auto query = folly::stringPrintf(fmt, nobody.vid(), nobody.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(SetTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM 123 OVER like"
                     " | (GO FROM $- OVER serve"
                     " UNION GO FROM 456 OVER serve)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(SetTest, ExecutionError) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name1, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name as name, $$.team.name as player"
                    " UNION "
                    "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as name, serve.start_year as player";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}
}  // namespace graph
}  // namespace nebula
