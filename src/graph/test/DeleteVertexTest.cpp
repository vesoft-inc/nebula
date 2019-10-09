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

class DeleteVertexTest : public TraverseTestBase {
 protected:
  void SetUp() override {
    TraverseTestBase::SetUp();
  }

  void TearDown() override {
    TraverseTestBase::TearDown();
  }
};

TEST_F(DeleteVertexTest, base) {
    // Check
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Boris Diaw"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tony Parker"].vid()},
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete vertex
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "DELETE VERTEX %ld";
        auto query = folly::stringPrintf(fmt, players_["Tony Parker"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Check again
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Boris Diaw"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tony Parker"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve %ld->%ld"
                    " YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.vid(), team.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}  // namespace graph
}  // namespace nebula

