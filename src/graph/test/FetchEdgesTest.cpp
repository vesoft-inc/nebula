/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 *
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
class FetchEdgesTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
    }

    void TearDown() override {
        TraverseTestBase::TearDown();
    }
};

TEST_F(FetchEdgesTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld"
                    " YIELD serve.start_year > 2001, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"(serve.start_year>2001)"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, bool, int64_t>> expected = {
            {player.vid(),
             team.vid(),
             std::get<1>(serve),
             std::get<2>(serve) > 2001,
             std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld@0"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve0 = player.serves()[0];
        auto &team0 = teams_[std::get<0>(serve0)];
        auto &serve1 = player.serves()[1];
        auto &team1 = teams_[std::get<0>(serve1)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld,%ld->%ld"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(
                fmt, player.vid(), team0.vid(), player.vid(), team1.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(),
             team0.vid(),
             std::get<1>(serve0),
             std::get<2>(serve0),
             std::get<3>(serve0)},
            {player.vid(),
             team1.vid(),
             std::get<1>(serve1),
             std::get<2>(serve1),
             std::get<3>(serve1)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._src AS src, serve._dst AS dst"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t> result(
                player.vid(),
                teams_[std::get<0>(serve)].vid(),
                std::get<1>(serve),
                std::get<2>(serve),
                std::get<3>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "$var = GO FROM %ld OVER serve"
                    " YIELD serve._src AS src, serve._dst AS dst;"
                    "FETCH PROP ON serve $var.src->$var.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t> result(
                player.vid(),
                teams_[std::get<0>(serve)].vid(),
                std::get<1>(serve),
                std::get<2>(serve),
                std::get<3>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve hash(\"%s\")->hash(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve uuid(\"%s\")->uuid(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0, 1, 2}));
    }
}

TEST_F(FetchEdgesTest, NoYield) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld@0";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve hash(\"%s\")->hash(\"%s\")";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve uuid(\"%s\")->uuid(\"%s\")";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0, 1, 2}));
    }
}

TEST_F(FetchEdgesTest, Distinct) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld,%ld->%ld"
                    " YIELD DISTINCT serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(
                fmt, player.vid(), team.vid(), player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<3>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld,%ld OVER serve YIELD serve._src AS src, serve._dst AS dst"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD DISTINCT serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t> result(
                player.vid(),
                teams_[std::get<0>(serve)].vid(),
                std::get<1>(serve),
                std::get<2>(serve),
                std::get<3>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "$var = GO FROM %ld,%ld OVER serve"
                    " YIELD serve._src AS src, serve._dst AS dst;"
                    "FETCH PROP ON serve $var.src->$var.dst"
                    " YIELD DISTINCT serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t> result(
                player.vid(),
                teams_[std::get<0>(serve)].vid(),
                std::get<1>(serve),
                std::get<2>(serve),
                std::get<3>(serve));
            expected.emplace_back(std::move(result));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto *fmt = "GO FROM %ld,%ld OVER serve YIELD serve._src AS src, serve._dst AS dst"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD DISTINCT serve._dst as dst";
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"}, {"serve._dst"}, {"serve._rank"}, {"dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t>> expected = {
            {tim.vid(), teams_["Spurs"].vid(), 0, teams_["Spurs"].vid()},
            {tony.vid(), teams_["Hornets"].vid(), 0, teams_["Hornets"].vid()},
            {tony.vid(), teams_["Spurs"].vid(), 0, teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(FetchEdgesTest, EmptyInput) {
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
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._src AS src, serve._dst AS dst"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD serve._src AS src, serve._dst AS dst, serve.start_year as start "
                    "| YIELD $-.src as src, $-.dst as dst WHERE $-.start > 20000"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(FetchEdgesTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") "
                     "YIELD $^.serve.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") "
                     "YIELD $$.serve.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve hash(\"Boris Diaw\")->hash(\"Spurs\") "
                     "YIELD abc.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(FetchEdgesTest, NonexistentEdge) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve hash(\"Zion Williamson\")->hash(\"Spurs\") "
                     "YIELD serve.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"}, {"serve._dst"}, {"serve._rank"}, {"serve.start_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve uuid(\"Zion Williamson\")->uuid(\"Spurs\") "
                     "YIELD serve.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"}, {"serve._dst"}, {"serve._rank"}, {"serve.start_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(FetchEdgesTest, DuplicateColumnName) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld"
                    " YIELD serve.start_year, serve.start_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve.start_year"},
            {"serve.start_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), std::get<1>(serve), std::get<2>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld"
                    " YIELD serve._src, serve._dst, serve._rank";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
            {"serve._src"},
            {"serve._dst"},
            {"serve._rank"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(),
             team.vid(),
             std::get<1>(serve),
             player.vid(),
             team.vid(),
             std::get<1>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._src AS src, serve._dst AS src"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(FetchEdgesTest, NonexistentProp) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld YIELD serve.start_year1";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}
}  // namespace graph
}  // namespace nebula
