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
                    "YIELD $$.team.name as name, "
                    "serve._dst as id, "
                    "serve.start_year as start_year, "
                    "serve.end_year as end_year"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id), "
                    "$-.start_year as start_year, "
                    "AVG($-.end_year)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<uint64_t, uint64_t, double >> expected = {
                {2, 2018, 2018.5},
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
                    "YIELD $$.team.name as name, "
                    "serve._dst as id, "
                    "serve.start_year as start, "
                    "serve.end_year as end"
                    "| GROUP BY $-.name, start_year "
                    "YIELD $-.name as teamName, "
                    "$-.start as start_year, "
                    "MAX($-.start), "
                    "MIN($-.end), "
                    "AVG($-.end) as avg_end_year, "
                    "STD($-.end) as std_end_year, "
                    "COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string,  // teamName
                    uint64_t,                // start_year
                    uint64_t,                // MAX($-.start_year)
                    uint64_t,                // MIN($-.end_year)
                    double ,                 // avg_end_year
                    double ,                 // std_end_year
                    uint64_t>                // COUNT($-.id)
                    > expected = {
                {"Celtics", 2017, 2017, 2019, 2019, 0, 1},
                {"Magic", 2000, 2000, 2004, 2004, 0, 1},
                {"Pistons", 2015, 2015, 2017, 2017, 0, 1},
                {"Raptors", 1997, 1997, 2000, 2000, 0, 1},
                {"Rockets", 2004, 2004, 2010, 2010, 0, 1},
                {"Spurs", 2013, 2013, 2013, 2014, 1, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // group has all cal fun
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name as name, "
                    "$$.player.age as dst_age, "
                    "$$.player.age as src_age, "
                    "like.likeness as likeness"
                    "| GROUP BY $-.name "
                    "YIELD $-.name as name, "
                    "SUM($-.dst_age) as sum_dst_age, "
                    "AVG($-.dst_age) as avg_dst_age, "
                    "MAX($-.src_age) as max_src_age, "
                    "MIN($-.src_age) as min_src_age, "
                    "BIT_AND(1) as bit_and, "
                    "BIT_OR(2) as bit_or, "
                    "BIT_XOR(3) as bit_xor, "
                    "COUNT($-.likeness), "
                    "COUNT_DISTINCT($-.likeness)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string,   // name
                               int64_t,       // sum_dst_age
                               double ,       // avg_dst_age
                               uint64_t,      // max_src_age
                               uint64_t,      // min_src_age
                               uint64_t,      // bit_and
                               uint64_t,      // bit_or
                               uint64_t,      // bit_xor
                               uint64_t,      // COUNT($-.likeness)
                               uint64_t>      // COUNT_DISTINCT($-.likeness)
                               > expected = {
                {"LeBron James", 68, 34, 34, 34, 1, 2, 0, 2, 1},
                {"Chris Paul", 66, 33, 33, 33, 1, 2, 0, 2, 1},
                {"Dwyane Wade", 37, 37, 37, 37, 1, 2, 3, 1, 1},
                {"Carmelo Anthony", 34, 34, 34, 34, 1, 2, 3, 1, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // group has fun col
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name as name"
                    "| GROUP BY $-.name, SUM(1) "
                    "YIELD $-.name as name, "
                    "SUM(2) as sum, "
                    "COUNT(1) as count";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, uint64_t>> expected = {
                {"LeBron James", 4, 2},
                {"Chris Paul", 4, 2},
                {"Dwyane Wade", 2, 1},
                {"Carmelo Anthony", 2, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }


    // output next
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Paul Gasol"];
        auto *fmt = "GO FROM %ld OVER like "
                    "YIELD $$.player.age as age, "
                    "like._dst as id "
                    "| GROUP BY $-.id "
                    "YIELD $-.id as id, "
                    "SUM(age) as age |"
                    "GO FROM $-.id OVER serve "
                    "YIELD $$.team.name as name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {
                {"Grizzlies"},
                {"Raptors"},
                {"Lakers"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // COUNT(*) nonsupport

    // group input nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name as name, "
                    "serve._dst as id"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }

    // group alias nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name as name, "
                    "serve._dst as id"
                    "| GROUP BY team "
                    "YIELD COUNT($-.id), "
                    "$-.name as teamName";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }

    // field nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name as name, "
                    "serve._dst as id"
                    "| GROUP BY $-.name "
                    "YIELD COUNT($-.start_year)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(GroupByLimitTest, GroupByLimitTest) {
}

TEST_F(GroupByLimitTest, GroupByOrderByLimitTest) {
}
}  // namespace graph
}  // namespace nebula
