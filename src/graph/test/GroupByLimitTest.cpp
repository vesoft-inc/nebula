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

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class GroupByLimitTest : public TraverseTestBase {
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

TEST_F(GroupByLimitTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM 1 OVER server | LIMIT -1, 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM 1 OVER server | LIMIT 1, -2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(GroupByLimitTest, LimitTest) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name AS name"
                    " | ORDER BY $-.name | LIMIT 5";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {
                {"76ers"},
                {"Bulls"},
                {"Hawks"},
                {"Hornets"},
                {"Kings"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    // Test limit skip,count
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name AS name | "
                    "ORDER BY $-.name | LIMIT 2,2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {
                {"Hawks"},
                {"Hornets"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));

        // use OFFSET
        auto *fmt1 = "GO FROM %ld OVER serve YIELD $$.team.name AS name | "
                     "ORDER BY $-.name | LIMIT 2 OFFSET 2";
        query = folly::stringPrintf(fmt1, player.vid());
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    // test pipe output
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER like YIELD $$.player.name AS name, like._dst AS id "
                    "| ORDER BY $-.name | LIMIT 1 "
                    "| GO FROM $-.id OVER like YIELD $$.player.name AS name "
                    "| ORDER BY $-.name | LIMIT 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {
                {"LeBron James"},
                {"Marco Belinelli"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    // Test count is 0
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Danny Green"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name AS name | LIMIT 1 OFFSET 0";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    // Test less limit
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Danny Green"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name AS name | LIMIT 5";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
    }
    // Test empty result
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Danny Green"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name AS name | LIMIT 3, 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
