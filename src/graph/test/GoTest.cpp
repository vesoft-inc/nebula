/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"
#include "meta/test/TestUtils.h"
#include "parser/GQLParser.h"
#include "graph/TraverseExecutor.h"
#include "graph/GoExecutor.h"


namespace nebula {
namespace graph {

class GoTest : public TraverseTestBase,
               public ::testing::WithParamInterface<bool>{
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
        FLAGS_filter_pushdown = GetParam();
        // ...
    }

    void TearDown() override {
        // ...
        TraverseTestBase::TearDown();
    }
};

TEST_P(GoTest, OneStepOutBound) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "YIELD %ld as vid | GO FROM $-.vid OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2003, 2005, "Hawks"},
            {player.name(), 2005, 2008, "Suns"},
            {player.name(), 2008, 2012, "Hornets"},
            {player.name(), 2012, 2016, "Spurs"},
            {player.name(), 2016, 2017, "Jazz"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Rajon Rondo"];
        auto *fmt = "GO FROM %ld OVER serve WHERE "
                    "serve.start_year >= 2013 && serve.end_year <= 2018 YIELD "
                    "$^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2014, 2015, "Mavericks"},
            {player.name(), 2015, 2016, "Kings"},
            {player.name(), 2016, 2017, "Bulls"},
            {player.name(), 2017, 2018, "Pelicans"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst as id"
                    "| GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
            {teams_["Trail Blazers"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "GO FROM %ld OVER like YIELD like._dst as id"
            "| ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
            {teams_["Trail Blazers"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, AssignmentSimple) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tracy McGrady"];
        auto *fmt = "$var = GO FROM %ld OVER like YIELD like._dst as id; "
                    "GO FROM $var.id OVER like";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<uint64_t>> expected = {
            {players_["Tracy McGrady"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, AssignmentPipe) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tracy McGrady"];
        auto *fmt =
            "$var = (GO FROM %ld OVER like YIELD like._dst as id | GO FROM $-.id OVER like YIELD "
            "like._dst as id);"
            "GO FROM $var.id OVER like";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<uint64_t>> expected = {
            {players_["Kobe Bryant"].vid()},
            {players_["Grant Hill"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, VariableUndefined) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM $var OVER like";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_P(GoTest, AssignmentEmptyResult) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "$var = GO FROM -1 OVER like YIELD like._dst as id; "
                     "GO FROM $var.id OVER like";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<uint64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, OneStepInBound) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve REVERSELY";
        auto &team = teams_["Thunders"];
        auto query = folly::stringPrintf(fmt, team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Russell Westbrook"].vid()},
            {players_["Kevin Durant"].vid()},
            {players_["James Harden"].vid()},
            {players_["Carmelo Anthony"].vid()},
            {players_["Paul George"].vid()},
            {players_["Ray Allen"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, DISABLED_OneStepInOutBound) {
    // Ever served in the same team
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._dst AS id"
                    " | GO FROM $-.id OVER serve REVERSELY";
        auto &player = players_["Kobe Bryant"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["LeBron James"].vid()},
            {players_["Rajon Rondo"].vid()},
            {players_["Kobe Bryant"].vid()},
            {players_["Steve Nash"].vid()},
            {players_["Paul Gasol"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["JaVale McGee"].vid()},
            {players_["Dwight Howard"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Ever been teammates
    {
    }
}

TEST_P(GoTest, Distinct) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Nobody"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD DISTINCT $^.player.name as name, $$.team.name as name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst as id"
                    "| GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve "
                    "YIELD DISTINCT serve._dst, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string>> expected = {
            {teams_["Spurs"].vid(), "Spurs"},
            {teams_["Hornets"].vid(), "Hornets"},
            {teams_["Trail Blazers"].vid(), "Trail Blazers"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 2 STEPS FROM %ld OVER like YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {3394245602834314645},
            {-7579316172763586624},
            {5662213458193308137},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, VertexNotExist) {
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
        auto *fmt = "GO FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD DISTINCT "
                    "$^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_P(GoTest, EmptyInputs) {
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
        auto *fmt = "GO FROM %ld OVER serve | GO FROM $-.serve_id OVER serve";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst as id"
                    "| GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst as id"
                    "| (GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve)";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string fmt = "GO FROM %ld over serve "
                          "YIELD serve._dst as id, serve.start_year as start "
                          "| YIELD $-.id as id WHERE $-.start > 20000"
                          "| Go FROM $-.id over serve";
        auto query = folly::stringPrintf(fmt.c_str(), players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_P(GoTest, MULTI_EDGES) {
    // Ever served in the same team
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Thunders"].vid(), 0},
            {0, players_["Paul George"].vid()},
            {0, players_["James Harden"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like REVERSELY "
                    "YIELD serve._dst, like._dst, serve._type, like._type";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> expected = {
            {0, players_["James Harden"].vid(), 0, -5},
            {0, players_["Dejounte Murray"].vid(), 0, -5},
            {0, players_["Paul George"].vid(), 0, -5},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like REVERSELY YIELD serve._src, like._src";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, player.vid()},
            {0, player.vid()},
            {0, player.vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like REVERSELY";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, players_["James Harden"].vid()},
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["Paul George"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * REVERSELY YIELD serve._dst, like._dst";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, players_["James Harden"].vid()},
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["Paul George"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * REVERSELY YIELD serve._src, like._src";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, player.vid()},
            {0, player.vid()},
            {0, player.vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * REVERSELY";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        // edges order: serve, like, teammate.
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {0, players_["James Harden"].vid(), 0},
            {0, players_["Dejounte Murray"].vid(), 0},
            {0, players_["Paul George"].vid(), 0},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like, teammate REVERSELY YIELD like.likeness, "
                    "teammate.start_year, $$.player.name";
        auto &player = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, std::string>> expected = {
            {95, 0, "Tim Duncan"},
            {95, 0, "Tony Parker"},
            {90, 0, "Tiago Splitter"},
            {99, 0, "Dejounte Murray"},
            {0, 2002, "Tim Duncan"},
            {0, 2002, "Tony Parker"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * REVERSELY YIELD like.likeness, teammate.start_year, "
                    "serve.start_year, $$.player.name";
        auto &player = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, std::string>> expected = {
            {95, 0, 0, "Tim Duncan"},
            {95, 0, 0, "Tony Parker"},
            {90, 0, 0, "Tiago Splitter"},
            {99, 0, 0, "Dejounte Murray"},
            {0, 2002, 0, "Tim Duncan"},
            {0, 2002, 0, "Tony Parker"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like "
                    "YIELD serve.start_year, like.likeness, serve._type, like._type";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> expected = {
            {2008, 0, 4, 0},
            {0, 90, 0, 5},
            {0, 90, 0, 5},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like";
        auto &player = players_["Shaquile O'Neal"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Magic"].vid(), 0},
            {teams_["Lakers"].vid(), 0},
            {teams_["Heat"].vid(), 0},
            {teams_["Suns"].vid(), 0},
            {teams_["Cavaliers"].vid(), 0},
            {teams_["Celtics"].vid(), 0},
            {0, players_["JaVale McGee"].vid()},
            {0, players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * YIELD serve._dst, like._dst";
        auto &player = players_["Dirk Nowitzki"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Mavericks"].vid(), 0},
            {0, players_["Steve Nash"].vid()},
            {0, players_["Jason Kidd"].vid()},
            {0, players_["Dwyane Wade"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER *";
        auto &player = players_["Paul Gasol"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        // edges order: serve, like, teammate
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {teams_["Grizzlies"].vid(), 0, 0},
            {teams_["Lakers"].vid(), 0, 0},
            {teams_["Bulls"].vid(), 0, 0},
            {teams_["Spurs"].vid(), 0, 0},
            {teams_["Bucks"].vid(), 0, 0},
            {0, players_["Kobe Bryant"].vid(), 0},
            {0, players_["Marc Gasol"].vid(), 0},
        };

        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt    = "GO FROM %ld OVER * YIELD $$.team.name, $$.player.name";
        auto &player = players_["LaMarcus Aldridge"];
        auto query   = folly::stringPrintf(fmt, player.vid());
        auto code    = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected = {
            {"Trail Blazers", ""},
            {"", "Tim Duncan"},
            {"", "Tony Parker"},
            {"Spurs", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "GO FROM %ld OVER like, serve YIELD like._dst as id"
            "| ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code  = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
            {teams_["Trail Blazers"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "GO FROM %ld OVER * YIELD like._dst as id"
            "| ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code  = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
            {teams_["Trail Blazers"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, ReferencePipeInYieldAndWhere) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id "
                            "| GO FROM $-.id OVER like "
                            "YIELD $-.name, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$-.name"}, {"$^.player.name"}, {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<uniform_tuple_t<std::string, 3>> expected = {
            {"Tim Duncan", "Manu Ginobili", "Tim Duncan"},
            {"Tim Duncan", "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", "Tony Parker", "Manu Ginobili"},
            {"Tim Duncan", "Tony Parker", "Tim Duncan"},
            {"Chris Paul", "LeBron James", "Ray Allen"},
            {"Chris Paul", "Carmelo Anthony", "Chris Paul"},
            {"Chris Paul", "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", "Dwyane Wade", "Chris Paul"},
            {"Chris Paul", "Dwyane Wade", "LeBron James"},
            {"Chris Paul", "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id "
                            "| GO FROM $-.id OVER like "
                            "WHERE $-.name != $$.player.name "
                            "YIELD $-.name, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$-.name"}, {"$^.player.name"}, {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<uniform_tuple_t<std::string, 3>> expected = {
            {"Tim Duncan", "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", "Tony Parker", "Manu Ginobili"},
            {"Chris Paul", "LeBron James", "Ray Allen"},
            {"Chris Paul", "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", "Dwyane Wade", "LeBron James"},
            {"Chris Paul", "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id "
                            "| GO FROM $-.id OVER like "
                            "YIELD $-.*, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        auto &manu = players_["Manu Ginobili"];
        auto &tony = players_["Tony Parker"];
        auto &lebron = players_["LeBron James"];
        auto &melo = players_["Carmelo Anthony"];
        auto &wade = players_["Dwyane Wade"];
        std::vector<std::tuple<std::string, uint64_t, std::string, std::string>> expected = {
            {"Tim Duncan", manu.vid(), "Manu Ginobili", "Tim Duncan"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "Manu Ginobili"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "Tim Duncan"},
            {"Chris Paul", lebron.vid(), "LeBron James", "Ray Allen"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "Chris Paul"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "Chris Paul"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "LeBron James"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, ReferenceVariableInYieldAndWhere) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "$var = GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id; "
                            "GO FROM $var.id OVER like "
                            "YIELD $var.name, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.name"}, {"$^.player.name"}, {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<uniform_tuple_t<std::string, 3>> expected = {
            {"Tim Duncan", "Manu Ginobili", "Tim Duncan"},
            {"Tim Duncan", "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", "Tony Parker", "Manu Ginobili"},
            {"Tim Duncan", "Tony Parker", "Tim Duncan"},
            {"Chris Paul", "LeBron James", "Ray Allen"},
            {"Chris Paul", "Carmelo Anthony", "Chris Paul"},
            {"Chris Paul", "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", "Dwyane Wade", "Chris Paul"},
            {"Chris Paul", "Dwyane Wade", "LeBron James"},
            {"Chris Paul", "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "$var = GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id; "
                            "GO FROM $var.id OVER like "
                            "WHERE $var.name != $$.player.name "
                            "YIELD $var.name, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.name"}, {"$^.player.name"}, {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<uniform_tuple_t<std::string, 3>> expected = {
            {"Tim Duncan", "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", "Tony Parker", "Manu Ginobili"},
            {"Chris Paul", "LeBron James", "Ray Allen"},
            {"Chris Paul", "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", "Dwyane Wade", "LeBron James"},
            {"Chris Paul", "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "$var = GO FROM hash('Tim Duncan'),hash('Chris Paul') OVER like "
                            "YIELD $^.player.name AS name, like._dst AS id; "
                            "GO FROM $var.id OVER like "
                            "YIELD $var.*, $^.player.name, $$.player.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        auto &manu = players_["Manu Ginobili"];
        auto &tony = players_["Tony Parker"];
        auto &lebron = players_["LeBron James"];
        auto &melo = players_["Carmelo Anthony"];
        auto &wade = players_["Dwyane Wade"];
        std::vector<std::tuple<std::string, uint64_t, std::string, std::string>> expected = {
            {"Tim Duncan", manu.vid(), "Manu Ginobili", "Tim Duncan"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "LaMarcus Aldridge"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "Manu Ginobili"},
            {"Tim Duncan", tony.vid(), "Tony Parker", "Tim Duncan"},
            {"Chris Paul", lebron.vid(), "LeBron James", "Ray Allen"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "Chris Paul"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "LeBron James"},
            {"Chris Paul", melo.vid(), "Carmelo Anthony", "Dwyane Wade"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "Chris Paul"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "LeBron James"},
            {"Chris Paul", wade.vid(), "Dwyane Wade", "Carmelo Anthony"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, NonexistentProp) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.test";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve yield $^.player.test";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD serve.test";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_P(GoTest, is_inCall) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE udf_is_in($$.team.name, \"Hawks\", \"Suns\") "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *resp.get_error_msg();

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2003, 2005, "Hawks"},
            {player.name(), 2005, 2008, "Suns"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst AS id"
                    "| GO FROM $-.id OVER serve WHERE udf_is_in($-.id, %ld, 123)";
        auto query = folly::stringPrintf(fmt,
                players_["Tim Duncan"].vid(), players_["Tony Parker"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst AS id"
                    "| GO FROM $-.id OVER serve WHERE udf_is_in($-.id, %ld, 123) && 1 == 1";
        auto query = folly::stringPrintf(fmt,
                players_["Tim Duncan"].vid(), players_["Tony Parker"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, ReturnTest) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE $A.dst == 123;"
                    "RETURN $rA IF $rA IS NOT NULL;" /* will not return */
                    "GO FROM $A.dst OVER serve"; /* 2nd hop */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Hornets"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE 1 == 1;"
                    "RETURN $rA IF $rA IS NOT NULL;" /* will return */
                    "GO FROM $A.dst OVER serve"; /* 2nd hop */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$A.dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dstA;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE $A.dstA == 123;"
                    "RETURN $rA IF $rA IS NOT NULL;" /* will not return */
                    "$B = GO FROM $A.dstA OVER like YIELD like._dst AS dstB;" /* 2nd hop */
                    "$rB = YIELD $B.* WHERE $B.dstB == 456;"
                    "RETURN $rB IF $rB IS NOT NULL;" /* will not return */
                    "GO FROM $B.dstB OVER serve"; /* 3rd hop */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Trail Blazers"].vid()},
            {teams_["Spurs"].vid()},
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE $A.dst == 123;"
                    "RETURN $rA IF $rA IS NOT NULL;"; /* will return nothing */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE 1 == 1;"
                    "RETURN $rA IF $rA IS NOT NULL;"; /* will return */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$A.dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE 1 == 1;"
                    "RETURN $B IF $B IS NOT NULL;"; /* will return error */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$A = GO FROM %ld OVER like YIELD like._dst AS dst;" /* 1st hop */
                    "$rA = YIELD $A.* WHERE 1 == 1;"
                    "RETURN $B IF $A IS NOT NULL;"; /* will return error */
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "RETURN $rA IF $rA IS NOT NULL;"; /* will return error */
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}


TEST_P(GoTest, ReverselyOneStep) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Tim Duncan') OVER like REVERSELY "
                     "YIELD like._dst";
        client_->execute(query, resp);
        std::vector<std::tuple<int64_t>> expected = {
            { players_["Tony Parker"].vid() },
            { players_["Manu Ginobili"].vid() },
            { players_["LaMarcus Aldridge"].vid() },
            { players_["Marco Belinelli"].vid() },
            { players_["Danny Green"].vid() },
            { players_["Aron Baynes"].vid() },
            { players_["Boris Diaw"].vid() },
            { players_["Tiago Splitter"].vid() },
            { players_["Dejounte Murray"].vid() },
            { players_["Shaquile O'Neal"].vid() },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Tim Duncan') OVER * REVERSELY "
                     "YIELD like._dst";
        client_->execute(query, resp);
        std::vector<std::tuple<int64_t>> expected = {
            { players_["Tony Parker"].vid() },
            { players_["Manu Ginobili"].vid() },
            { players_["LaMarcus Aldridge"].vid() },
            { players_["Marco Belinelli"].vid() },
            { players_["Danny Green"].vid() },
            { players_["Aron Baynes"].vid() },
            { players_["Boris Diaw"].vid() },
            { players_["Tiago Splitter"].vid() },
            { players_["Dejounte Murray"].vid() },
            { players_["Shaquile O'Neal"].vid() },
            { 0 },
            { 0 },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Tim Duncan') OVER like REVERSELY "
                      "YIELD $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string>> expected = {
            { "Tony Parker" },
            { "Manu Ginobili" },
            { "LaMarcus Aldridge" },
            { "Marco Belinelli" },
            { "Danny Green" },
            { "Aron Baynes" },
            { "Boris Diaw" },
            { "Tiago Splitter" },
            { "Dejounte Murray" },
            { "Shaquile O'Neal" },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Tim Duncan') OVER like REVERSELY "
                     "WHERE $$.player.age < 35 "
                     "YIELD $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string>> expected = {
            { "LaMarcus Aldridge" },
            { "Marco Belinelli" },
            { "Danny Green" },
            { "Aron Baynes" },
            { "Tiago Splitter" },
            { "Dejounte Murray" },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, OnlyIdTwoSteps) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 2 STEPS FROM %ld OVER like YIELD like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        client_->execute(query, resp);
        std::vector<std::tuple<VertexID>> expected = {
            {3394245602834314645},
            {-7579316172763586624},
            {-7579316172763586624},
            {5662213458193308137},
            {5662213458193308137}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, ReverselyTwoStep) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO 2 STEPS FROM hash('Kobe Bryant') OVER like REVERSELY "
                     "YIELD $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string>> expected = {
            { "Marc Gasol" },
            { "Vince Carter" },
            { "Yao Ming" },
            { "Grant Hill" },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO 2 STEPS FROM hash('Kobe Bryant') OVER * REVERSELY "
                     "YIELD $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string>> expected = {
            { "Marc Gasol" },
            { "Vince Carter" },
            { "Yao Ming" },
            { "Grant Hill" },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(GoTest, ReverselyWithPipe) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('LeBron James') OVER serve YIELD serve._dst AS id |"
                     "GO FROM $-.id OVER serve REVERSELY YIELD $^.team.name, $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string, std::string>> expected = {
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Dwyane Wade" },
            { "Cavaliers", "Dwyane Wade" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Danny Green" },
            { "Cavaliers", "Danny Green" },
            { "Cavaliers", "LeBron James" },
            { "Cavaliers", "LeBron James" },
            { "Cavaliers", "LeBron James" },
            { "Cavaliers", "LeBron James" },
            { "Heat", "Dwyane Wade" },
            { "Heat", "Dwyane Wade" },
            { "Heat", "LeBron James" },
            { "Heat", "Ray Allen" },
            { "Heat", "Shaquile O'Neal" },
            { "Heat", "Amar'e Stoudemire" },
            { "Lakers", "Kobe Bryant" },
            { "Lakers", "LeBron James" },
            { "Lakers", "Rajon Rondo" },
            { "Lakers", "Steve Nash" },
            { "Lakers", "Paul Gasol" },
            { "Lakers", "Shaquile O'Neal" },
            { "Lakers", "JaVale McGee" },
            { "Lakers", "Dwight Howard" },
        };

        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('LeBron James') OVER serve "
                        "YIELD serve._dst AS id |"
                     "GO FROM $-.id OVER serve REVERSELY "
                        "WHERE $$.player.name != 'LeBron James' "
                        "YIELD $^.team.name, $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string, std::string>> expected = {
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Dwyane Wade" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Danny Green" },
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Dwyane Wade" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Danny Green" },
            { "Heat", "Dwyane Wade" },
            { "Heat", "Dwyane Wade" },
            { "Heat", "Ray Allen" },
            { "Heat", "Shaquile O'Neal" },
            { "Heat", "Amar'e Stoudemire" },
            { "Lakers", "Kobe Bryant" },
            { "Lakers", "Rajon Rondo" },
            { "Lakers", "Steve Nash" },
            { "Lakers", "Paul Gasol" },
            { "Lakers", "Shaquile O'Neal" },
            { "Lakers", "JaVale McGee" },
            { "Lakers", "Dwight Howard" },
        };

        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Manu Ginobili') OVER like REVERSELY "
                    "YIELD like._dst AS id |"
                    "GO FROM $-.id OVER serve";
        client_->execute(query, resp);
        std::vector<std::tuple<int64_t>> expected = {
            { teams_["Spurs"].vid() },
            { teams_["Spurs"].vid() },
            { teams_["Hornets"].vid() },
            { teams_["Spurs"].vid() },
            { teams_["Hawks"].vid() },
            { teams_["76ers"].vid() },
            { teams_["Spurs"].vid() },
        };

        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Manu Ginobili') OVER * REVERSELY "
                    "YIELD like._dst AS id |"
                    "GO FROM $-.id OVER serve";
        client_->execute(query, resp);
        std::vector<std::tuple<int64_t>> expected = {
            { teams_["Spurs"].vid() },
            { teams_["Spurs"].vid() },
            { teams_["Hornets"].vid() },
            { teams_["Spurs"].vid() },
            { teams_["Hawks"].vid() },
            { teams_["76ers"].vid() },
            { teams_["Spurs"].vid() },
        };

        ASSERT_TRUE(verifyResult(resp, expected));
    }
    /**
     * TODO(dutor)
     * For the time being, reference to the pipe inputs is faulty.
     * Because there might be multiple associated records with the same column value,
     * which is used as the source id by the right statement.
     *
     * So the following case is disabled temporarily.
     */
    /*
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('LeBron James') OVER serve "
                        "YIELD serve._dst AS id, serve.start_year AS start, "
                            "serve.end_year AS end |"
                     "GO FROM $-.id OVER serve REVERSELY "
                        "WHERE $$.player.name != 'LeBron James' && "
                            "serve.start_year <= $-.end && serve.end_year >= $-.end "
                        "YIELD $^.team.name, $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string, std::string>> expected = {
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Danny Green" },
            { "Cavaliers", "Dwyane Wade" },
            { "Heat", "Dwyane Wade" },
            { "Heat", "Ray Allen" },
            { "Lakers", "Rajon Rondo" },
            { "Lakers", "JaVale McGee" },
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    */
}

TEST_P(GoTest, Bidirect) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve bidirect";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like bidirect";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Shaquile O'Neal"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like bidirect";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}, {"like._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Spurs"].vid(), 0},
            {0, players_["Tony Parker"].vid()},
            {0, players_["Manu Ginobili"].vid()},
            {0, players_["Tony Parker"].vid()},
            {0, players_["Manu Ginobili"].vid()},
            {0, players_["LaMarcus Aldridge"].vid()},
            {0, players_["Marco Belinelli"].vid()},
            {0, players_["Danny Green"].vid()},
            {0, players_["Aron Baynes"].vid()},
            {0, players_["Boris Diaw"].vid()},
            {0, players_["Tiago Splitter"].vid()},
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["Shaquile O'Neal"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * bidirect";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}, {"like._dst"}, {"teammate._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {teams_["Spurs"].vid(), 0, 0},
            {0, players_["Tony Parker"].vid(), 0},
            {0, players_["Manu Ginobili"].vid(), 0},
            {0, players_["Tony Parker"].vid(), 0},
            {0, players_["Manu Ginobili"].vid(), 0},
            {0, players_["LaMarcus Aldridge"].vid(), 0},
            {0, players_["Marco Belinelli"].vid(), 0},
            {0, players_["Danny Green"].vid(), 0},
            {0, players_["Aron Baynes"].vid(), 0},
            {0, players_["Boris Diaw"].vid(), 0},
            {0, players_["Tiago Splitter"].vid(), 0},
            {0, players_["Dejounte Murray"].vid(), 0},
            {0, players_["Shaquile O'Neal"].vid(), 0},
            {0, 0, players_["Tony Parker"].vid()},
            {0, 0, players_["Manu Ginobili"].vid()},
            {0, 0, players_["LaMarcus Aldridge"].vid()},
            {0, 0, players_["Danny Green"].vid()},
            {0, 0, players_["Tony Parker"].vid()},
            {0, 0, players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve bidirect YIELD $$.team.name";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected = {
            {"Spurs"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like bidirect YIELD $$.player.name";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string>> expected = {
            {"Tony Parker"},
            {"Manu Ginobili"},
            {"Tony Parker"},
            {"Manu Ginobili"},
            {"LaMarcus Aldridge"},
            {"Marco Belinelli"},
            {"Danny Green"},
            {"Aron Baynes"},
            {"Boris Diaw"},
            {"Tiago Splitter"},
            {"Dejounte Murray"},
            {"Shaquile O'Neal"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like bidirect "
                    "WHERE like.likeness > 90 "
                    "YIELD $^.player.name, like._dst, $$.player.name, like.likeness";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name", "like._dst", "$$.player.name", "like.likeness"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string, int64_t>> expected = {
            {"Tim Duncan", players_["Tony Parker"].vid(), "Tony Parker", 95},
            {"Tim Duncan", players_["Manu Ginobili"].vid(), "Manu Ginobili", 95},
            {"Tim Duncan", players_["Tony Parker"].vid(), "Tony Parker", 95},
            {"Tim Duncan", players_["Dejounte Murray"].vid(), "Dejounte Murray", 99},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER * bidirect "
                    "YIELD $^.player.name, serve._dst, $$.team.name, like._dst, $$.player.name";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve._dst"}, {"$$.team.name"}, {"like._dst"}, {"$$.player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<
                    std::tuple<std::string, int64_t, std::string, int64_t, std::string>
                   > expected = {
            {"Tim Duncan", teams_["Spurs"].vid(), "Spurs", 0, ""},
            {"Tim Duncan", 0, "", players_["Tony Parker"].vid(), "Tony Parker"},
            {"Tim Duncan", 0, "", players_["Manu Ginobili"].vid(), "Manu Ginobili"},
            {"Tim Duncan", 0, "", players_["Tony Parker"].vid(), "Tony Parker"},
            {"Tim Duncan", 0, "", players_["Manu Ginobili"].vid(), "Manu Ginobili"},
            {"Tim Duncan", 0, "", players_["LaMarcus Aldridge"].vid(), "LaMarcus Aldridge"},
            {"Tim Duncan", 0, "", players_["Marco Belinelli"].vid(), "Marco Belinelli"},
            {"Tim Duncan", 0, "", players_["Danny Green"].vid(), "Danny Green"},
            {"Tim Duncan", 0, "", players_["Aron Baynes"].vid(), "Aron Baynes"},
            {"Tim Duncan", 0, "", players_["Boris Diaw"].vid(), "Boris Diaw"},
            {"Tim Duncan", 0, "", players_["Tiago Splitter"].vid(), "Tiago Splitter"},
            {"Tim Duncan", 0, "", players_["Dejounte Murray"].vid(), "Dejounte Murray"},
            {"Tim Duncan", 0, "", players_["Shaquile O'Neal"].vid(), "Shaquile O'Neal"},
            // These response derives from the teamates, the fifth column has property
            // because that we collect props with column name.
            {"Tim Duncan", 0, "", 0, "Tony Parker"},
            {"Tim Duncan", 0, "", 0, "Manu Ginobili"},
            {"Tim Duncan", 0, "", 0, "Danny Green"},
            {"Tim Duncan", 0, "", 0, "LaMarcus Aldridge"},
            {"Tim Duncan", 0, "", 0, "Tony Parker"},
            {"Tim Duncan", 0, "", 0, "Manu Ginobili"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, FilterPushdown) {
    #define TEST_FILTER_PUSHDOWN_REWRITE(rewrite_expected, filter_pushdown)             \
        auto result = GQLParser().parse(query);                                         \
        ASSERT_TRUE(result.ok());                                                       \
        auto sentences = result.value()->sentences();                                   \
        ASSERT_EQ(sentences.size(), 1);                                                 \
        auto goSentence = static_cast<GoSentence*>(sentences[0]);                       \
        auto whereWrapper = std::make_unique<WhereWrapper>(goSentence->whereClause());  \
        auto filter = whereWrapper->filter_;                                            \
        ASSERT_NE(filter, nullptr);                                                     \
        auto isRewriteSucceded = whereWrapper->rewrite(filter);                         \
        ASSERT_EQ(rewrite_expected, isRewriteSucceded);                                 \
        std::string filterPushdown = "";                                                \
        if (isRewriteSucceded) {                                                        \
            filterPushdown = filter->toString();                                        \
        }                                                                               \
        LOG(INFO) << "Filter rewrite: " << filterPushdown;                             \
        ASSERT_EQ(filter_pushdown, filterPushdown);

    {
        // Filter pushdown: ((serve.start_year>2013)&&(serve.end_year<2018))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "((serve.start_year>2013)&&(serve.end_year<2018))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: !((serve.start_year>2013)&&(serve.end_year<2018))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE !(serve.start_year > 2013 && serve.end_year < 2018)";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "!(((serve.start_year>2013)&&(serve.end_year<2018)))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Celtics"].vid()},
            {teams_["Pelicans"].vid()},
            {teams_["Lakers"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: ((serve.start_year>2013)&&true)
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && $$.team.name == \"Kings\"";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "((serve.start_year>2013)&&true)");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Kings"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // will not pushdown
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name == \"Celtics\" && $$.team.name == \"Kings\"";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                false,
                "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: ((serve.start_year>2013)&&true)
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013"
                    " && (serve.end_year < 2018 || $$.team.name == \"Kings\")";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "((serve.start_year>2013)&&true)");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: (true&&(serve.start_year>2013))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE (serve.end_year < 2018 || $$.team.name == \"Kings\")"
                    "&& serve.start_year > 2013";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "(true&&(serve.start_year>2013))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // will not push down
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name == \"Celtics\" || $$.team.name == \"Kings\"";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                false,
                "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Celtics"].vid()},
            {teams_["Kings"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: (((serve.start_year>2013)&&(serve.end_year<2018))&&true)
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year < 2018"
                    " && $$.team.name == \"Kings\"";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "(((serve.start_year>2013)&&(serve.end_year<2018))&&true)");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Kings"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: (((serve.start_year>2013)&&true)&&(serve.end_year<2018))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013"
                    " && $$.team.name == \"Kings\""
                    " && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "(((serve.start_year>2013)&&true)&&(serve.end_year<2018))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Kings"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: ((true&&(serve.start_year>2013))&&(serve.end_year<2018))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name == \"Kings\""
                    " && serve.start_year > 2013"
                    " && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "((true&&(serve.start_year>2013))&&(serve.end_year<2018))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Kings"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // ((serve.start_year==2013)||((serve.start_year>2013)&&(serve.end_year<2018)))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year == 2013"
                    " OR serve.start_year > 2013 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "((serve.start_year==2013)||((serve.start_year>2013)&&(serve.end_year<2018)))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // (((serve.start_year>2013)&&(serve.end_year<=2015))
        // || ((serve.start_year>=2015)&&(serve.end_year<2018)))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year <= 2015"
                    " OR serve.start_year >= 2015 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "(((serve.start_year>2013)&&(serve.end_year<=2015))"
            "||((serve.start_year>=2015)&&(serve.end_year<2018)))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // ((((serve.start_year>2013)&&(serve.end_year<=2015))&&true)
        // ||((serve.start_year>=2015)&&(serve.end_year<2018)))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year <= 2015"
                    " && $$.team.name == \"Mavericks\""
                    " OR serve.start_year >= 2015 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "((((serve.start_year>2013)&&(serve.end_year<=2015))&&true)"
            "||((serve.start_year>=2015)&&(serve.end_year<2018)))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // will not push down the filter
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name == \"Pelicans\""
                    " OR serve.start_year > 2013 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                false,
                "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
            {teams_["Pelicans"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // will not push down the filter
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name == \"Pelicans\""
                    " OR serve.start_year > 2013 && serve.end_year <= 2015"
                    " OR serve.start_year >= 2015 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                false,
                "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
            {teams_["Pelicans"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // (((serve.start_year>2013)&&(serve.end_year<=2015))
        // XOR((serve.start_year>=2015)&&(serve.end_year<2018)))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year <= 2015"
                    " XOR serve.start_year >= 2015 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "(((serve.start_year>2013)&&(serve.end_year<=2015))"
            "XOR((serve.start_year>=2015)&&(serve.end_year<2018)))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // will not pushdown
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve.start_year > 2013 && serve.end_year <= 2015"
                    " && $$.team.name == \"Mavericks\""
                    " XOR serve.start_year >= 2015 && serve.end_year < 2018";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                false,
                "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Mavericks"].vid()},
            {teams_["Kings"].vid()},
            {teams_["Bulls"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown: (($^.player.name=="Tony Parker")&&(serve.start_year>2013))
        auto query = "GO FROM $-.id OVER serve "
                     "WHERE $^.player.name == \"Tony Parker\" && serve.start_year > 2013";

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "(($^.player.name==Tony Parker)&&(serve.start_year>2013))");

        auto *fmt = "GO FROM %ld OVER like YIELD like._dst AS id | ";
        auto newQuery = folly::stringPrintf(fmt, players_["Tim Duncan"].vid()).append(query);

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(newQuery, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Hornets"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Filter pushdown:
        // (((serve._src==5662213458193308137)&&(serve._rank==0))&&(serve._dst==7193291116733635180))
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE serve._src == %ld && serve._rank == 0 && serve._dst == %ld"
                    "YIELD serve._dst AS id";
        auto query = folly::stringPrintf(fmt,
                players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid(), teams_["Spurs"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "(((serve._src==5662213458193308137)&&(serve._rank==0))"
            "&&(serve._dst==7193291116733635180))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE udf_is_in(serve._dst, 1, 2, 3)";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
                true,
                "udf_is_in(serve._dst,1,2,3)");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE udf_is_in(serve._dst, %ld, 2, 3)";
        auto query = folly::stringPrintf(fmt,
                players_["Rajon Rondo"].vid(), teams_["Celtics"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            folly::stringPrintf("udf_is_in(serve._dst,%ld,2,3)", teams_["Celtics"].vid()));

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Celtics"].vid()}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE udf_is_in(\"test\", $$.team.name)";
        auto query = folly::stringPrintf(fmt, players_["Rajon Rondo"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            false,
            "");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE udf_is_in($^.player.name, \"Tim Duncan\")";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "udf_is_in($^.player.name,Tim Duncan)");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE !udf_is_in($^.player.name, \"Tim Duncan\")";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "!(udf_is_in($^.player.name,Tim Duncan))");

        cpp2::ExecutionResponse resp;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name CONTAINS Haw "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            false,
            "");

        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2003, 2005, "Hawks"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE (string)serve.start_year CONTAINS \"05\" "
                    "&& $^.player.name CONTAINS \"Boris\""
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());

        TEST_FILTER_PUSHDOWN_REWRITE(
            true,
            "(((string)serve.start_year CONTAINS 05)&&($^.player.name CONTAINS Boris))");

        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2005, 2008, "Suns"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
#undef TEST_FILTER_PUSHDWON_REWRITE
}

TEST_P(GoTest, DuplicateColumnName) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._dst, serve._dst";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"},
            {"serve._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Spurs"].vid(), teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._dst AS id, like.likeness AS id"
                    " | GO FROM $-.id OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_P(GoTest, Contains) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $$.team.name CONTAINS Haw "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2003, 2005, "Hawks"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE (string)serve.start_year CONTAINS \"05\" "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2005, 2008, "Suns"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE $^.player.name CONTAINS \"Boris\" "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"serve.end_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {player.name(), 2003, 2005, "Hawks"},
            {player.name(), 2005, 2008, "Suns"},
            {player.name(), 2008, 2012, "Hornets"},
            {player.name(), 2012, 2016, "Spurs"},
            {player.name(), 2016, 2017, "Jazz"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE !($^.player.name CONTAINS \"Boris\") "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve "
                    "WHERE \"Leo\" CONTAINS \"Boris\" "
                    "YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, WithIntermediateData) {
    // zero to zero
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 0 TO 0 STEPS FROM %ld OVER like YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // simple
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER like YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER like YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // With properties
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER like "
            "YIELD DISTINCT like._dst, like.likeness, $$.player.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID, int64_t, std::string>> expected = {
            {players_["Manu Ginobili"].vid(), 95, "Manu Ginobili"},
            {players_["LaMarcus Aldridge"].vid(), 90, "LaMarcus Aldridge"},
            {players_["Tim Duncan"].vid(), 95, "Tim Duncan"},
            {players_["Tony Parker"].vid(), 95, "Tony Parker"},
            {players_["Tony Parker"].vid(), 75, "Tony Parker"},
            {players_["Tim Duncan"].vid(), 75, "Tim Duncan"},
            {players_["Tim Duncan"].vid(), 90, "Tim Duncan"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER like "
            "YIELD DISTINCT like._dst, like.likeness, $$.player.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID, int64_t, std::string>> expected = {
            {players_["Manu Ginobili"].vid(), 95, "Manu Ginobili"},
            {players_["LaMarcus Aldridge"].vid(), 90, "LaMarcus Aldridge"},
            {players_["Tim Duncan"].vid(), 95, "Tim Duncan"},
            {players_["Tony Parker"].vid(), 95, "Tony Parker"},
            {players_["Tony Parker"].vid(), 75, "Tony Parker"},
            {players_["Tim Duncan"].vid(), 75, "Tim Duncan"},
            {players_["Tim Duncan"].vid(), 90, "Tim Duncan"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // empty starts before last step
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 1 TO 3 STEPS FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
            teams_["Spurs"].vid()
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 TO 3 STEPS FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
            teams_["Spurs"].vid()
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 2 TO 3 STEPS FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // reversely
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER like REVERSELY YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Damian Lillard"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER like REVERSELY YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Damian Lillard"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 2 TO 2 STEPS FROM %ld OVER like REVERSELY YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Damian Lillard"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // empty starts before last step
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 1 TO 3 STEPS FROM %ld OVER serve REVERSELY";
        auto query = folly::stringPrintf(fmt, teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
            players_["Tim Duncan"].vid(),
            players_["Tony Parker"].vid(),
            players_["Manu Ginobili"].vid(),
            players_["LaMarcus Aldridge"].vid(),
            players_["Rudy Gay"].vid(),
            players_["Marco Belinelli"].vid(),
            players_["Danny Green"].vid(),
            players_["Kyle Anderson"].vid(),
            players_["Aron Baynes"].vid(),
            players_["Boris Diaw"].vid(),
            players_["Tiago Splitter"].vid(),
            players_["Cory Joseph"].vid(),
            players_["David West"].vid(),
            players_["Jonathon Simmons"].vid(),
            players_["Dejounte Murray"].vid(),
            players_["Tracy McGrady"].vid(),
            players_["Paul Gasol"].vid(),
            players_["Marco Belinelli"].vid(),
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 TO 3 STEPS FROM %ld OVER serve REVERSELY";
        auto query = folly::stringPrintf(fmt, teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
            players_["Tim Duncan"].vid(),
            players_["Tony Parker"].vid(),
            players_["Manu Ginobili"].vid(),
            players_["LaMarcus Aldridge"].vid(),
            players_["Rudy Gay"].vid(),
            players_["Marco Belinelli"].vid(),
            players_["Danny Green"].vid(),
            players_["Kyle Anderson"].vid(),
            players_["Aron Baynes"].vid(),
            players_["Boris Diaw"].vid(),
            players_["Tiago Splitter"].vid(),
            players_["Cory Joseph"].vid(),
            players_["David West"].vid(),
            players_["Jonathon Simmons"].vid(),
            players_["Dejounte Murray"].vid(),
            players_["Tracy McGrady"].vid(),
            players_["Paul Gasol"].vid(),
            players_["Marco Belinelli"].vid(),
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Bidirectionally
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER like BIDIRECT YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Damian Lillard"].vid()},
            {players_["LeBron James"].vid()},
            {players_["Russell Westbrook"].vid()},
            {players_["Chris Paul"].vid()},
            {players_["Kyle Anderson"].vid()},
            {players_["Kevin Durant"].vid()},
            {players_["James Harden"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER like BIDIRECT YIELD DISTINCT like._dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Danny Green"].vid()},
            {players_["Aron Baynes"].vid()},
            {players_["Tiago Splitter"].vid()},
            {players_["Shaquile O'Neal"].vid()},
            {players_["Rudy Gay"].vid()},
            {players_["Damian Lillard"].vid()},
            {players_["LeBron James"].vid()},
            {players_["Russell Westbrook"].vid()},
            {players_["Chris Paul"].vid()},
            {players_["Kyle Anderson"].vid()},
            {players_["Kevin Durant"].vid()},
            {players_["James Harden"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // over *
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER * YIELD serve._dst, like._dst";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Thunders"].vid(), 0},
            {0, players_["Paul George"].vid()},
            {0, players_["James Harden"].vid()},
            {teams_["Pacers"].vid(), 0},
            {teams_["Thunders"].vid(), 0},
            {0, players_["Russell Westbrook"].vid()},
            {teams_["Thunders"].vid(), 0},
            {teams_["Rockets"].vid(), 0},
            {0, players_["Russell Westbrook"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER * YIELD serve._dst, like._dst";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {teams_["Thunders"].vid(), 0},
            {0, players_["Paul George"].vid()},
            {0, players_["James Harden"].vid()},
            {teams_["Pacers"].vid(), 0},
            {teams_["Thunders"].vid(), 0},
            {0, players_["Russell Westbrook"].vid()},
            {teams_["Thunders"].vid(), 0},
            {teams_["Rockets"].vid(), 0},
            {0, players_["Russell Westbrook"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // With properties
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER * "
            "YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, std::string>> expected = {
            {teams_["Thunders"].vid(), 0, 2008, 0, ""},
            {0, players_["Paul George"].vid(), 0, 90, "Paul George"},
            {0, players_["James Harden"].vid(), 0, 90, "James Harden"},
            {teams_["Pacers"].vid(), 0, 2010, 0, ""},
            {teams_["Thunders"].vid(), 0, 2017, 0, ""},
            {0, players_["Russell Westbrook"].vid(), 0, 95, "Russell Westbrook"},
            {teams_["Thunders"].vid(), 0, 2009, 0, ""},
            {teams_["Rockets"].vid(), 0, 2012, 0, ""},
            {0, players_["Russell Westbrook"].vid(), 0, 80, "Russell Westbrook"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER * "
            "YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, std::string>> expected = {
            {teams_["Thunders"].vid(), 0, 2008, 0, ""},
            {0, players_["Paul George"].vid(), 0, 90, "Paul George"},
            {0, players_["James Harden"].vid(), 0, 90, "James Harden"},
            {teams_["Pacers"].vid(), 0, 2010, 0, ""},
            {teams_["Thunders"].vid(), 0, 2017, 0, ""},
            {0, players_["Russell Westbrook"].vid(), 0, 95, "Russell Westbrook"},
            {teams_["Thunders"].vid(), 0, 2009, 0, ""},
            {teams_["Rockets"].vid(), 0, 2012, 0, ""},
            {0, players_["Russell Westbrook"].vid(), 0, 80, "Russell Westbrook"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 1 TO 2 STEPS FROM %ld OVER * REVERSELY YIELD serve._dst, like._dst";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["James Harden"].vid()},
            {0, players_["Paul George"].vid()},
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["Russell Westbrook"].vid()},
            {0, players_["Luka Doncic"].vid()},
            {0, players_["Russell Westbrook"].vid()},
        };
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 TO 2 STEPS FROM %ld OVER * REVERSELY YIELD serve._dst, like._dst";
        const auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["James Harden"].vid()},
            {0, players_["Paul George"].vid()},
            {0, players_["Dejounte Murray"].vid()},
            {0, players_["Russell Westbrook"].vid()},
            {0, players_["Luka Doncic"].vid()},
            {0, players_["Russell Westbrook"].vid()},
        };
    }
}

TEST_P(GoTest, ErrorMsg) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $$.player.name as name";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected = {""};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, ZeroStep) {
    // Zero step
    {
        // #2100
        // A cycle traversal
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 STEPS FROM %ld OVER serve BIDIRECT";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // a normal traversal
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO 0 STEPS FROM %ld OVER serve";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, issue2087_go_cover_input) {
    // input with src and dst properties
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._src as src, like._dst as dst "
            "| GO FROM $-.src OVER like "
            "YIELD $-.src as src, like._dst as dst, $^.player.name, $$.player.name ";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t, std::string, std::string>> expected = {
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(),
             "Tim Duncan", "Tony Parker"},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(),
             "Tim Duncan", "Manu Ginobili"},
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(),
             "Tim Duncan", "Tony Parker"},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(),
             "Tim Duncan", "Manu Ginobili"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // var
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$a = GO FROM %ld OVER like YIELD like._src as src, like._dst as dst; "
            "GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // with intermidate data
    // pipe
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "GO FROM %ld OVER like YIELD like._src as src, like._dst as dst "
            "| GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID, VertexID>> expected = {
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},

            {players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid()},
            {players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
            {players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid()},
            {players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid()},
            {players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // var with properties
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "GO FROM %ld OVER like YIELD like._src as src, like._dst as dst "
            "| GO 1 TO 2 STEPS FROM $-.src OVER like YIELD"
            " $-.src as src, $-.dst, like._dst as dst, like.likeness";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<VertexID, VertexID, VertexID, int64_t>> expected = {
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["Tony Parker"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Tony Parker"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Manu Ginobili"].vid(), 95},  // NOLINT

            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), 90},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), 90},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Manu Ginobili"].vid(), 95},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["LaMarcus Aldridge"].vid(), 90},  // NOLINT
            {players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid(), 90},  // NOLINT
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // partial neighbors
    // input
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like YIELD like._src AS src, like._dst AS dst "
            "| GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, teammate._dst AS dst";
        auto query = folly::stringPrintf(fmt, players_["Danny Green"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Danny Green"].vid()},  // NOLINT
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // var
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$a = GO FROM %ld OVER like YIELD like._src AS src, like._dst AS dst; "
            "GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, teammate._dst AS dst";
        auto query = folly::stringPrintf(fmt, players_["Danny Green"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid()},  // NOLINT
            {players_["Danny Green"].vid(), players_["Tim Duncan"].vid(), players_["Danny Green"].vid()},  // NOLINT
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_P(GoTest, issueBackTrackOverlap) {
    // require there are edges in one steps like below:
    // dst, src
    // 7  , 1
    // 1  , 7
    // In total , one src is anthoer one's dst
    {
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> expected = {
            {players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Tim Duncan"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tim Duncan"].vid()},  // NOLINT

            {players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tim Duncan"].vid()},  // NOLINT

            {players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tony Parker"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Manu Ginobili"].vid(), players_["Tim Duncan"].vid()},  // NOLINT
            {players_["Tony Parker"].vid(), players_["LaMarcus Aldridge"].vid(), players_["LaMarcus Aldridge"].vid(), players_["Tim Duncan"].vid()},  // NOLINT
        };
        {
            cpp2::ExecutionResponse resp;
            auto *fmt = "GO FROM %ld OVER like YIELD like._src as src, like._dst as dst "
                "| GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, like._src, like._dst";
            auto query = folly::stringPrintf(fmt, players_["Tony Parker"].vid());
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

            ASSERT_TRUE(verifyResult(resp, expected));
        }
        {
            cpp2::ExecutionResponse resp;
            auto *fmt = "$a = GO FROM %ld OVER like YIELD like._src as src, like._dst as dst; "
                "GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, like._src, like._dst";
            auto query = folly::stringPrintf(fmt, players_["Tony Parker"].vid());
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

            ASSERT_TRUE(verifyResult(resp, expected));
        }
    }
}

TEST_P(GoTest, NStepQueryHangAndOOM) {
    {
        std::vector<std::tuple<VertexID>> expected = {
                {players_["Tony Parker"].vid()},
                {players_["Manu Ginobili"].vid()},
                {players_["Tim Duncan"].vid()},
                {players_["Tim Duncan"].vid()},
                {players_["LaMarcus Aldridge"].vid()},
                {players_["Manu Ginobili"].vid()},
                {players_["Tony Parker"].vid()},
                {players_["Manu Ginobili"].vid()},
                {players_["Tim Duncan"].vid()},
                {players_["Tim Duncan"].vid()},
                {players_["Tony Parker"].vid()},
        };
        {
            cpp2::ExecutionResponse resp;
            auto *fmt = "GO 1 TO 3 STEPS FROM %ld OVER like YIELD like._dst as dst";
            auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            ASSERT_TRUE(verifyResult(resp, expected));
        }
        {
            cpp2::ExecutionResponse resp;
            auto *fmt = "YIELD %ld as id "
                        "| GO 1 TO 3 STEPS FROM $-.id OVER like YIELD like._dst as dst";
            auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            ASSERT_TRUE(verifyResult(resp, expected));
        }
        {
            cpp2::ExecutionResponse resp;
            auto *fmt = "GO 1 TO 40 STEPS FROM %ld OVER like YIELD like._dst as dst";
            auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
    }
}

INSTANTIATE_TEST_CASE_P(IfPushdownFilter, GoTest, ::testing::Bool());

}   // namespace graph
}   // namespace nebula
