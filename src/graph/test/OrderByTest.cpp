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

class OrderByTest : public TraverseTestBase {
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

TEST_F(OrderByTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "ORDER BY ";
        auto query = fmt;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^.player.name as name, serve.start_year as start, $$.team.name"
                    "| ORDER BY $-.$$.team.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(OrderByTest, NoInput) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "ORDER BY $-.xx";
        auto query = fmt;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Nobody"];
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^.player.name as name, serve.start_year as start, $$.team.name as name"
                    "| ORDER BY $-.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(OrderByTest, WrongFactor) {
    std::string go = "GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team";
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        // Will not do sort if field name not exist in input schema
        auto fmt = go + "| ORDER BY $-.abc";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {player.name(), 2003, "Hawks"},
            {player.name(), 2005, "Suns"},
            {player.name(), 2008, "Hornets"},
            {player.name(), 2012, "Spurs"},
            {player.name(), 2016, "Jazz"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(OrderByTest, SingleFactor) {
    std::string go = "GO FROM %ld OVER serve YIELD "
                     "$^.player.name as name, serve.start_year as start, $$.team.name as team";
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| ORDER BY $-.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {player.name(), 2003, "Hawks"},
            {player.name(), 2008, "Hornets"},
            {player.name(), 2016, "Jazz"},
            {player.name(), 2012, "Spurs"},
            {player.name(), 2005, "Suns"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| ORDER BY $-.team";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {player.name(), 2008, "Hornets"},
            {player.name(), 2003, "Hawks"},
            {player.name(), 2016, "Jazz"},
            {player.name(), 2012, "Spurs"},
            {player.name(), 2005, "Suns"},
        };
        ASSERT_FALSE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| ORDER BY $-.team ASC";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {player.name(), 2003, "Hawks"},
            {player.name(), 2008, "Hornets"},
            {player.name(), 2016, "Jazz"},
            {player.name(), 2012, "Spurs"},
            {player.name(), 2005, "Suns"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto fmt = go + "| ORDER BY $-.team DESC";
        auto query = folly::stringPrintf(fmt.c_str(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {player.name(), 2005, "Suns"},
            {player.name(), 2012, "Spurs"},
            {player.name(), 2016, "Jazz"},
            {player.name(), 2008, "Hornets"},
            {player.name(), 2003, "Hawks"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
}

TEST_F(OrderByTest, MultiFactors) {
    std::string go = "GO FROM %ld,%ld OVER serve WHERE serve.start_year >= 2012 YIELD "
                    "$$.team.name as team, $^.player.name as player, "
                    "$^.player.age as age, serve.start_year as start";
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &aldridge = players_["LaMarcus Aldridge"];
        auto fmt = go + "| ORDER BY $-.team, $-.age";
        auto query = folly::stringPrintf(fmt.c_str(), boris.vid(), aldridge.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Jazz", boris.name(), 36, 2016},
            {"Spurs", aldridge.name(), 33, 2015},
            {"Spurs", boris.name(), 36, 2012},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &aldridge = players_["LaMarcus Aldridge"];
        auto fmt = go + "| ORDER BY $-.team, $-.age DESC";
        auto query = folly::stringPrintf(fmt.c_str(), boris.vid(), aldridge.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Jazz", boris.name(), 36, 2016},
            {"Spurs", boris.name(), 36, 2012},
            {"Spurs", aldridge.name(), 33, 2015},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &aldridge = players_["LaMarcus Aldridge"];
        auto fmt = go + "| ORDER BY $-.team DESC, $-.age";
        auto query = folly::stringPrintf(fmt.c_str(), boris.vid(), aldridge.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Spurs", aldridge.name(), 33, 2015},
            {"Spurs", boris.name(), 36, 2012},
            {"Jazz", boris.name(), 36, 2016},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &aldridge = players_["LaMarcus Aldridge"];
        auto fmt = go + "| ORDER BY $-.team DESC, $-.age DESC";
        auto query = folly::stringPrintf(fmt.c_str(), boris.vid(), aldridge.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Spurs", boris.name(), 36, 2012},
            {"Spurs", aldridge.name(), 33, 2015},
            {"Jazz", boris.name(), 36, 2016},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &aldridge = players_["LaMarcus Aldridge"];
        auto fmt = go + "| ORDER BY team DESC, age DESC";
        auto query = folly::stringPrintf(fmt.c_str(), boris.vid(), aldridge.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Spurs", boris.name(), 36, 2012},
            {"Spurs", aldridge.name(), 33, 2015},
            {"Jazz", boris.name(), 36, 2016},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
}

TEST_F(OrderByTest, InterimResult) {
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER like | ORDER BY $-.id | GO FROM $-.id over serve";
        auto query = folly::stringPrintf(fmt, boris.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            teams_["Spurs"].vid(),
            teams_["Spurs"].vid(),
            teams_["Hornets"].vid(),
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}
}   // namespace graph
}   // namespace nebula
