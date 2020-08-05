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

class FetchVerticesTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
    }

    void TearDown() override {
        TraverseTestBase::TearDown();
    }
};

TEST_F(FetchVerticesTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld "
                    "YIELD player.name, player.age, player.age > 30";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}, {"(player.age>30)"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t, bool>> expected = {
            {player.vid(), player.name(), player.age(), player.age() > 30},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld over like YIELD like._dst as id"
                    "| FETCH PROP ON player $-.id YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {players_["Tony Parker"].vid(), "Tony Parker", players_["Tony Parker"].age()},
            {players_["Tim Duncan"].vid(), "Tim Duncan", players_["Tim Duncan"].age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld over like YIELD like._dst as id"
                    "| FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}, {"$-.id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld over like YIELD like._dst as id"
                    "| FETCH PROP ON team $-.id YIELD team.name, $-.*";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"team.name"}, {"$-.id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "$var = GO FROM %ld over like YIELD like._dst as id;"
                    "FETCH PROP ON player $var.id YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {players_["Tony Parker"].vid(), "Tony Parker", players_["Tony Parker"].age()},
            {players_["Tim Duncan"].vid(), "Tim Duncan", players_["Tim Duncan"].age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "$var = GO FROM %ld over like YIELD like._dst as id;"
                    "FETCH PROP ON player $var.id YIELD player.name as name, player.age"
                    " | ORDER BY name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {players_["Tim Duncan"].vid(), "Tim Duncan", players_["Tim Duncan"].age()},
            {players_["Tony Parker"].vid(), "Tony Parker", players_["Tony Parker"].age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player hash(\"%s\") "
                    "YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player uuid(\"%s\") "
                    "YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
}

TEST_F(FetchVerticesTest, NoYield) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player hash(\"%s\")";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player uuid(\"%s\")";
        auto query = folly::stringPrintf(fmt, player.name().c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
}

TEST_F(FetchVerticesTest, Distinct) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld, %ld "
                    "YIELD DISTINCT player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &boris = players_["Boris Diaw"];
        auto &tony = players_["Tony Parker"];
        auto *fmt = "FETCH PROP ON player %ld, %ld "
                    "YIELD DISTINCT player.age";
        auto query = folly::stringPrintf(fmt, boris.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {boris.vid(), boris.age()},
            {tony.vid(), tony.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(FetchVerticesTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD $^.player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD $$.player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD abc.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(FetchVerticesTest, NonexistentTag) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON abc %ld";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(FetchVerticesTest, NonexistentVertex) {
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
        auto *fmt = "FETCH PROP ON player %ld";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve | FETCH PROP ON team $-";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FETCH PROP ON * %ld";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(FetchVerticesTest, FetchAll) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON * %ld ";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "YIELD %ld as id | FETCH PROP ON * $-.id ";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON * %ld, %ld ";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "FETCH PROP ON bachelor %ld ";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"bachelor.name"}, {"bachelor.speciality"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {player.vid(), bachelors_["Tim Duncan"].name(), bachelors_["Tim Duncan"].speciality()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "FETCH PROP ON * %ld ";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"},
            {"bachelor.name"}, {"bachelor.speciality"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t,
                    std::string, std::string>> expected = {
            {player.vid(), player.name(), player.age(),
                bachelors_["Tim Duncan"].name(), bachelors_["Tim Duncan"].speciality()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "YIELD %ld as id | FETCH PROP ON * $-.id ";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"},
            {"bachelor.name"}, {"bachelor.speciality"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t,
            std::string, std::string>> expected = {
            {player.vid(), player.name(), player.age(),
                bachelors_["Tim Duncan"].name(), bachelors_["Tim Duncan"].speciality()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "FETCH PROP ON * %ld, %ld ";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"},
            {"bachelor.name"}, {"bachelor.speciality"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t,
            std::string, std::string>> expected = {
            {player.vid(), player.name(), player.age(),
                bachelors_["Tim Duncan"].name(), bachelors_["Tim Duncan"].speciality()},
            {player.vid(), player.name(), player.age(),
                bachelors_["Tim Duncan"].name(), bachelors_["Tim Duncan"].speciality()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Tim Duncan"];
        auto *fmt = "FETCH PROP ON * %ld, %ld YIELD player.name, player.age ";
        auto query = folly::stringPrintf(fmt, player.vid(), player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.age"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {player.vid(), player.name(), player.age()},
            {player.vid(), player.name(), player.age()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(FetchVerticesTest, DuplicateColumnName) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name, player.name";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"player.name"}, {"player.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {player.vid(), player.name(), player.name()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "GO FROM %ld over like YIELD like._dst as id, like._dst as id"
                    "| FETCH PROP ON player $-.id YIELD player.name, player.age";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(FetchVerticesTest, NonexistentProp) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt = "FETCH PROP ON player %ld YIELD player.name1";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(FetchVerticesTest, EmptyInput) {
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
        std::string fmt = "GO FROM %ld over like YIELD like._dst as id "
                          "| FETCH PROP ON player $-.id";
        auto query = folly::stringPrintf(fmt.c_str(), nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string fmt = "GO FROM %ld over serve "
                          "YIELD serve._dst as id, serve.start_year as start "
                          "| YIELD $-.id as id WHERE $-.start > 20000"
                          "| FETCH PROP ON player $-.id";
        auto query = folly::stringPrintf(fmt.c_str(), players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

}  // namespace graph
}  // namespace nebula
