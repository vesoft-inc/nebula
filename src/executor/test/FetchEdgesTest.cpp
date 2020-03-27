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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {"(serve.start_year>2001)"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<bool, int64_t>> expected = {
            {std::get<1>(serve) > 2001, std::get<2>(serve)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve0), std::get<2>(serve0)},
            {std::get<1>(serve1), std::get<2>(serve1)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t> result(std::get<1>(serve), std::get<2>(serve));
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t> result(std::get<1>(serve), std::get<2>(serve));
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
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {std::get<1>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {std::get<1>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t> result(std::get<1>(serve), std::get<2>(serve));
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
            {"serve.start_year"}, {"serve.end_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected;
        for (auto &serve : player.serves()) {
            std::tuple<int64_t, int64_t> result(std::get<1>(serve), std::get<2>(serve));
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
                    " YIELD DISTINCT serve._dst";
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
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

TEST_F(FetchEdgesTest, EmptyInput) {
    {
        cpp2::ExecutionResponse resp;
        auto &nobody = players_["Nobody"];
        auto *fmt = "GO FROM %ld OVER serve YIELD serve._src AS src, serve._dst AS dst"
                    "| FETCH PROP ON serve $-.src->$-.dst"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, nobody.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve.start_year"}, {"serve.end_year"}
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

TEST_F(FetchEdgesTest, NonExistEdge) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON serve hash(\"Zion Williamson\")->hash(\"Spurs\") "
                     "YIELD serve.start_year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve.start_year"}
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
            {"serve.start_year"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
}
}  // namespace graph
}  // namespace nebula
