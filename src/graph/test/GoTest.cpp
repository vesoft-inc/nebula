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

DECLARE_int32(load_data_interval_secs);

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
        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve YIELD "
                    "$^[player].name, serve.start_year, serve.end_year, $$[team].name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
                    "$^[player].name, serve.start_year, serve.end_year, $$[team].name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        auto *fmt = "GO FROM %ld OVER like "
                    "| GO FROM $-.id OVER like | GO FROM $-.id OVER serve";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
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
        auto *fmt = "GO FROM %ld OVER like "
                    "| ( GO FROM $-.id OVER like | GO FROM $-.id OVER serve )";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
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
        auto *fmt = "GO FROM %ld OVER serve | GO FROM $-.id OVER serve REVERSELY";
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

}   // namespace graph
}   // namespace nebula
