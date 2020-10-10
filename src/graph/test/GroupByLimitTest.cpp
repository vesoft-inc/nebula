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
    // Use groupby without input
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name"
                    "| GROUP BY 1+1 "
                    "YIELD COUNT(1), 1+1";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        LOG(INFO) << "ERROR " << *resp.get_error_msg();
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Use var
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve.end_year AS end_year "
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($var)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Use dst
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve.end_year AS end_year "
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($$.team.name)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Group input nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Group alias nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY team "
                    "YIELD COUNT($-.id), "
                    "$-.name AS teamName";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Field nonexistent
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY $-.name "
                    "YIELD COUNT($-.start_year)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Use SUM(*)
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY $-.name "
                    "YIELD SUM(*)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Use cal fun has more than two inputs
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY $-.name "
                    "YIELD COUNT($-.name, $-.id)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Group col has cal fun
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id"
                    "| GROUP BY $-.name, SUM($-.id) "
                    "YIELD $-.name,  SUM($-.id)";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Yield without group by
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "COUNT(serve._dst) AS id";
        auto query = folly::stringPrintf(fmt, player.vid());
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
                {"Hornets"},
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
    // Group one col
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id, "
                    "serve.start_year AS start_year, "
                    "serve.end_year AS end_year"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id), "
                    "$-.start_year AS start_year, "
                    "AVG($-.end_year) as avg";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<uint64_t, uint64_t, double >> expected = {
                {2, 2018, 2018.5, },
                {1, 2017, 2018.0},
                {1, 2016, 2017.0},
                {1, 2009, 2010.0},
                {1, 2007, 2009.0},
                {1, 2012, 2013.0},
                {1, 2015, 2016.0},
                {1, 2010, 2012.0},
                {1, 2013, 2015.0},
        };
       ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Has alias col
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Aron Baynes"];
        auto &player2 = players_["Tracy McGrady"];
        auto *fmt = "GO FROM %ld,%ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id, "
                    "serve.start_year AS start, "
                    "serve.end_year AS end"
                    "| GROUP BY teamName, start_year "
                    "YIELD $-.name AS teamName, "
                    "$-.start AS start_year, "
                    "MAX($-.start), "
                    "MIN($-.end), "
                    "AVG($-.end) AS avg_end_year, "
                    "STD($-.end) AS std_end_year, "
                    "COUNT($-.id)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"teamName"}, {"start_year"}, {"MAX($-.start)"}, {"MIN($-.end)"},
            {"avg_end_year"}, {"std_end_year"}, {"COUNT($-.id)"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string,  // teamName
                    uint64_t,                // start_year
                    uint64_t,                // MAX($-.start_year)
                    uint64_t,                // MIN($-.end_year)
                    double ,                 // avg_end_year
                    double ,                 // std_end_year
                    uint64_t>                // COUNT($-.id)
                    > expected = {
                {"Celtics", 2017, 2017, 2019, 2019.0, 0, 1},
                {"Magic", 2000, 2000, 2004, 2004.0, 0, 1},
                {"Pistons", 2015, 2015, 2017, 2017.0, 0, 1},
                {"Raptors", 1997, 1997, 2000, 2000.0, 0, 1},
                {"Rockets", 2004, 2004, 2010, 2010.0, 0, 1},
                {"Spurs", 2013, 2013, 2013, 2014.0, 1, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Group has all cal fun
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name AS name, "
                    "$$.player.age AS dst_age, "
                    "$$.player.age AS src_age, "
                    "like.likeness AS likeness"
                    "| GROUP BY $-.name "
                    "YIELD $-.name AS name, "
                    "SUM($-.dst_age) AS sum_dst_age, "
                    "AVG($-.dst_age) AS avg_dst_age, "
                    "MAX($-.src_age) AS max_src_age, "
                    "MIN($-.src_age) AS min_src_age, "
                    "BIT_AND(1) AS bit_and, "
                    "BIT_OR(2) AS bit_or, "
                    "BIT_XOR(3) AS bit_xor, "
                    "COUNT($-.likeness), "
                    "COUNT_DISTINCT($-.likeness)";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"},
            {"sum_dst_age"},
            {"avg_dst_age"},
            {"max_src_age"},
            {"min_src_age"},
            {"bit_and"},
            {"bit_or"},
            {"bit_xor"},
            {"COUNT($-.likeness)"},
            {"COUNT_DISTINCT($-.likeness)"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

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
                {"LeBron James", 68, 34.0, 34, 34, 1, 2, 0, 2, 1},
                {"Chris Paul", 66, 33.0, 33, 33, 1, 2, 0, 2, 1},
                {"Dwyane Wade", 37, 37.0, 37, 37, 1, 2, 3, 1, 1},
                {"Carmelo Anthony", 34, 34.0, 34, 34, 1, 2, 3, 1, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Group has fun col
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name AS name"
                    "| GROUP BY $-.name, abs(5) "
                    "YIELD $-.name AS name, "
                    "SUM(1.5) AS sum, "
                    "COUNT(*) AS count,"
                    "1+1 AS cal";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"sum"}, {"count"}, {"cal"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, double, uint64_t, uint64_t>> expected = {
                {"LeBron James", 3.0, 2, 2},
                {"Chris Paul", 3.0, 2, 2},
                {"Dwyane Wade", 1.5, 1, 2},
                {"Carmelo Anthony", 1.5, 1, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Output next
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Paul Gasol"];
        auto *fmt = "GO FROM %ld OVER like "
                    "YIELD $$.player.age AS age, "
                    "like._dst AS id "
                    "| GROUP BY $-.id "
                    "YIELD $-.id AS id, "
                    "SUM($-.age) AS age "
                    "| GO FROM $-.id OVER serve "
                    "YIELD $$.team.name AS name, "
                    "$-.age AS sumAge";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"sumAge"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t>> expected = {
                {"Grizzlies", 34},
                {"Raptors", 34},
                {"Lakers", 40},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(GroupByLimitTest, GroupByOrderByLimitTest) {
    // Test with OrderBy
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name AS name"
                    "| GROUP BY $-.name, abs(5) "
                    "YIELD $-.name AS name, "
                    "SUM(1.5) AS sum, "
                    "COUNT(*) AS count "
                    "| ORDER BY $-.sum, $-.name";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"sum"}, {"count"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, double, uint64_t>> expected = {
                {"Carmelo Anthony", 1.5, 1},
                {"Dwyane Wade", 1.5, 1},
                {"Chris Paul", 3.0, 2},
                {"LeBron James", 3.0, 2},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    // Test with Limit
    {
        cpp2::ExecutionResponse resp;
        auto &player1 = players_["Carmelo Anthony"];
        auto &player2 = players_["Dwyane Wade"];
        auto *fmt = "GO FROM %ld,%ld OVER like "
                    "YIELD $$.player.name AS name"
                    "| GROUP BY $-.name, abs(5) "
                    "YIELD $-.name AS name, "
                    "SUM(1.5) AS sum, "
                    "COUNT(*) AS count "
                    "| ORDER BY $-.sum, $-.name DESC | LIMIT 2";
        auto query = folly::stringPrintf(fmt, player1.vid(), player2.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"sum"}, {"count"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, double, uint64_t>> expected = {
                {"Dwyane Wade", 1.5, 1},
                {"Carmelo Anthony", 1.5, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
}

TEST_F(GroupByLimitTest, EmptyInput) {
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
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^.player.name as name, serve.start_year as start, $$.team.name as name"
                    "| GROUP BY $-.name YIELD $-.name AS name";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like "
                    "YIELD $$.player.name AS name"
                    "| GROUP BY $-.name, abs(5) "
                    "YIELD $-.name AS name, "
                    "SUM(1.5) AS sum, "
                    "COUNT(*) AS count "
                    "| ORDER BY $-.sum | LIMIT 2";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"sum"}, {"count"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team "
                    "| YIELD $-.name as name WHERE $-.start > 20000 "
                    "| GROUP BY $-.name YIELD $-.name AS name";
        auto query = folly::stringPrintf(fmt, players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as name, serve.start_year as start, $$.team.name as team "
                    "| YIELD $-.name as name WHERE $-.start > 20000 "
                    "| Limit 1";
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

TEST_F(GroupByLimitTest, DuplicateColumn) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Marco Belinelli"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $$.team.name AS name, "
                    "serve._dst AS id, "
                    "serve.start_year AS start_year, "
                    "serve.end_year AS start_year"
                    "| GROUP BY $-.start_year "
                    "YIELD COUNT($-.id), "
                    "$-.start_year AS start_year, "
                    "AVG($-.end_year) as avg";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}
}   // namespace graph
}   // namespace nebula

