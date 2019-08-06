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

TEST_F(GroupByLimitTest, GroupByTest) {
    // group one col
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name as name, serve._dst as id, "
                    "serve.start_year as start_year, serve.end_year as end_year"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id), $-.start_year as start_year, AVG($-.end_year)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t >> expected = {
                {2, 2018, 2018},
                {1, 2017, 2018},
                {1, 2016, 2017},
                {1, 2009, 2010},
                {1, 2007, 2009},
                {1, 2012, 2013},
                {1, 2015, 2016},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // group multi col, on alisa col
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Aron Baynes"];
        auto &player2 = players_["Tracy McGrady"];
        auto *fmt = "GO FROM %ld,%ld OVER serve "
                    "YIELD $$.team.name as name, serve._dst as id, "
                    "serve.start_year as start_year, serve.end_year as end_year"
                    "| GROUP BY $-.name, start_year "
                    "YIELD $-.name as teamName, $-.start_year as start_year, "
                    "MAX($-.start_year), MIN($-.end_year), COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, uint64_t, uint64_t, uint64_t , uint64_t>> expected = {
                {"Celtics", 2017, 2017, 2019, 1},
                {"Magic", 2000, 2000, 2004, 1},
                {"Pistons", 2015, 2015, 2017, 1},
                {"Raptors", 1997, 1997, 2000, 1},
                {"Rockets", 2004, 2004, 2010, 1},
                {"Spurs", 2013, 2013, 2013, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // group has all cal fun
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name as name, $$.player.age as dst_age, "
                    "$$.player.age as src_age, like.likeness as likeness"
                    "| GROUP BY $-.name YIELD $-.name as name, "
                    "SUM($-.dst_age) as sum_dst_age, AVG($-.dst_age) as avg_dst_age,"
                    "MAX($-.src_age) as max_src_age, MIN($-.src_age) as min_src_age,"
                    "COUNT($-.likeness), COUNT_DISTINCT($-.likeness)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string,  // name
                               int64_t,      // sum_dst_age
                               uint64_t,     // avg_dst_age
                               uint64_t ,    // max_src_age
                               uint64_t,     // min_src_age
                               uint64_t,     // COUNT
                               uint64_t>     // COUNT_DISTINCT
                                   > expected = {
                {"LeBron James", 68, 34, 34, 34, 2, 1},
                {"Chris Paul", 66, 33, 33, 33, 2, 1},
                {"Dwyane Wade", 37, 37, 37, 37, 1, 1},
                {"Carmelo Anthony", 34, 34, 34, 34, 1, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // COUNT(*) nonsupport

    // group nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name as name, serve._dst as id"
                    " | GROUP BY $-.start_year YIELD COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }

    // field nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.team.name as name, serve._dst as id"
                    " | GROUP BY $-.name YIELD COUNT($-.start_year)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(GroupByLimitTest, GroupByLimitTest) {
}

TEST_F(GroupByLimitTest, GroupByOrderByLimitTest) {
}
}   // namespace graph
}   // namespace nebula
