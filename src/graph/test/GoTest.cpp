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


namespace nebula {
namespace graph {

class GoTest : public TraverseTestBase {
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

TEST_F(GoTest, OneStepOutBound) {
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


TEST_F(GoTest, AssignmentSimple) {
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


TEST_F(GoTest, AssignmentPipe) {
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


TEST_F(GoTest, VariableUndefined) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM $var OVER like";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(GoTest, AssignmentEmptyResult) {
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


// REVERSELY not supported yet
TEST_F(GoTest, DISABLED_OneStepInBound) {
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
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

// REVERSELY not supported yet
TEST_F(GoTest, DISABLED_OneStepInOutBound) {
    // Ever served in the same team
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve | GO FROM $-.serve_id OVER serve REVERSELY";
        auto &player = players_["Kobe Bryant"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["LeBron James"].vid()},
            {players_["Rajon Rondo"].vid()},
            {players_["Kobe Bryant"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Ever been teammates
    {
    }
}

TEST_F(GoTest, Distinct) {
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
}


TEST_F(GoTest, VertexNotExist) {
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
}

TEST_F(GoTest, MULTI_EDGES) {
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
        auto *fmt = "GO FROM %ld OVER serve, like REVERSELY YIELD serve._src, like._src";
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
        auto *fmt = "GO FROM %ld OVER serve, like REVERSELY YIELD serve._dst, like._dst";
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
            {0, player.vid()},
            {0, player.vid()},
            {0, player.vid()},
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
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {0, 0, player.vid()},
            {0, 0, player.vid()},
            {0, 0, player.vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like, teammate REVERSELY yield like.likeness, "
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
        auto *fmt = "GO FROM %ld OVER * REVERSELY yield like.likeness, teammate.start_year, "
                    "serve.start_year, $$.player.name";
        auto &player = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve, like yield serve.start_year, like.likeness";
        auto &player = players_["Russell Westbrook"];
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {2008, 0},
            {0, 90},
            {0, 90},
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
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {teams_["Grizzlies"].vid(), 0, 0},
            {teams_["Lakers"].vid(), 0, 0},
            {teams_["Bulls"].vid(), 0, 0},
            {teams_["Spurs"].vid(), 0, 0},
            {teams_["Bucks"].vid(), 0, 0},
            {0, 0, players_["Kobe Bryant"].vid()},
            {0, 0, players_["Marc Gasol"].vid()},
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

TEST_F(GoTest, ReferencePipeInYieldAndWhere) {
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


TEST_F(GoTest, ReferenceVariableInYieldAndWhere) {
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

TEST_F(GoTest, NotExistTagProp) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve yield $^.test";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve yield serve.test";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(GoTest, is_inCall) {
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

TEST_F(GoTest, returnTest) {
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

        std::vector<std::string> expectedColNames{
            {"$A.dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected;
        ASSERT_TRUE(verifyResult(resp, expected));
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


TEST_F(GoTest, ReverselyOneStep) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('Tim Duncan') OVER like REVERSELY "
                     "YIELD like._src";
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
                     "YIELD like._src";
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


TEST_F(GoTest, ReverselyTwoStep) {
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


TEST_F(GoTest, ReverselyWithPipe) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "GO FROM hash('LeBron James') OVER serve YIELD serve._dst AS id |"
                     "GO FROM $-.id OVER serve REVERSELY YIELD $^.team.name, $$.player.name";
        client_->execute(query, resp);
        std::vector<std::tuple<std::string, std::string>> expected = {
            { "Cavaliers", "Kyrie Irving" },
            { "Cavaliers", "Dwyane Wade" },
            { "Cavaliers", "Shaquile O'Neal" },
            { "Cavaliers", "Danny Green" },
            { "Cavaliers", "LeBron James" },
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
                    "YIELD like._src AS id |"
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
                    "YIELD like._src AS id |"
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

}   // namespace graph
}   // namespace nebula
