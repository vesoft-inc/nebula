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

class DeleteVerticesTest : public TraverseTestBase {
 protected:
  void SetUp() override {
    TraverseTestBase::SetUp();
  }

  void TearDown() override {
    TraverseTestBase::TearDown();
  }
};

TEST_F(DeleteVerticesTest, Base) {
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
        auto *fmt = "GO FROM %ld OVER like REVERSELY";
        auto query = folly::stringPrintf(fmt, players_["Tony Parker"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Marco Belinelli"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
            {players_["Boris Diaw"].vid()},
            {players_["Dejounte Murray"].vid()},
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
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
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
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), 0, std::get<1>(serve), std::get<2>(serve)},
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
        auto &player = players_["Tony Parker"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
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
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        LOG(INFO) << "Boris Diaw ID " << players_["Boris Diaw"].vid();
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
        LOG(INFO) << "Tim Duncan ID " << players_["Tim Duncan"].vid();
        LOG(INFO) << "Marco Belinelli ID " << players_["Marco Belinelli"].vid();
        LOG(INFO) << "LaMarcus Aldridge ID " << players_["LaMarcus Aldridge"].vid();
        LOG(INFO) << "Boris Diaw ID " << players_["Boris Diaw"].vid();
        LOG(INFO) << "Dejounte Murray ID " << players_["Dejounte Murray"].vid();
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like REVERSELY";
        auto query = folly::stringPrintf(fmt, players_["Tony Parker"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DeleteVerticesTest, DeleteMultiVertices) {
    // Check
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Chris Paul"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["LeBron James"].vid()},
            {players_["Carmelo Anthony"].vid()},
            {players_["Dwyane Wade"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete verticse
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "DELETE VERTEX %ld, %ld, %ld";
        auto query = folly::stringPrintf(fmt,
                                         players_["LeBron James"].vid(),
                                         players_["Carmelo Anthony"].vid(),
                                         players_["Dwyane Wade"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Check again
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Chris Paul"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DeleteVerticesTest, DeleteWithHash) {
    // Check
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Tracy McGrady"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Kobe Bryant"].vid()},
            {players_["Grant Hill"].vid()},
            {players_["Rudy Gay"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like REVERSELY";
        auto query = folly::stringPrintf(fmt, players_["Grant Hill"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Tracy McGrady"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto *fmt = "FETCH PROP ON player hash(\"%s\") YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve hash(\"%s\")->hash(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {player.vid(), team.vid(), 0, std::get<1>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete vertex
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "DELETE VERTEX hash(\"%s\")";
        auto query = folly::stringPrintf(fmt, players_["Grant Hill"].name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Check again
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like";
        auto query = folly::stringPrintf(fmt, players_["Tracy McGrady"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {players_["Kobe Bryant"].vid()},
            {players_["Rudy Gay"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like REVERSELY";
        auto query = folly::stringPrintf(fmt, players_["Grant Hill"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto *fmt = "FETCH PROP ON player hash(\"%s\") YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve hash(\"%s\")->hash(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Delete non-existing vertex
    {
        cpp2::ExecutionResponse resp;
        auto query = "DELETE VERTEX hash(\"Non-existing Vertex\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Delete a vertex without edges
    {
        // Insert a vertex without edges
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX player(name, age) "
                     "VALUES hash(\"A Loner\"): (\"A Loner\", 0)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "DELETE VERTEX hash(\"A Loner\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON player hash(\"A Loner\") "
                     "YIELD player.name, player.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(DeleteVerticesTest, DeleteWithUUID) {
    // Check
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto *fmt = "FETCH PROP ON player UUID(\"%s\") YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve UUID(\"%s\")->UUID(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {std::get<1>(serve), std::get<2>(serve)},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0, 1, 2}));
    }
    // Delete vertex
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "DELETE VERTEX UUID(\"%s\")";
        auto query = folly::stringPrintf(fmt, players_["Grant Hill"].name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto *fmt = "FETCH PROP ON player UUID(\"%s\") YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Grant Hill"];
        auto &serve = player.serves()[0];
        auto &team = teams_[std::get<0>(serve)];
        auto *fmt = "FETCH PROP ON serve UUID(\"%s\")->UUID(\"%s\") "
                    "YIELD serve.start_year, serve.end_year";
        auto query = folly::stringPrintf(fmt, player.name().c_str(), team.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0, 1, 2}));
    }
    // Delete non-existing vertex
    {
        cpp2::ExecutionResponse resp;
        auto query = "DELETE VERTEX UUID(\"Non-existing Vertex\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Delete a vertex without edges
    {
        // Insert a vertex without edges
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX player(name, age) "
                     "VALUES UUID(\"A Loner\"): (\"A Loner\", 0)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "DELETE VERTEX UUID(\"A Loner\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON player UUID(\"A Loner\") "
                     "YIELD player.name, player.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DeleteVerticesTest, DeleteNoEdges) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "create space deletenoedges_space";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "use deletenoedges_space";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG person(name string, age int)";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        sleep(2);
        query = "INSERT VERTEX person(name, age) VALUES 101:(\"Tony Parker\", 36)";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DELETE VERTEX 101";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "FETCH PROP ON person 101 yield person.name, person.age";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}  // namespace graph
}  // namespace nebula
