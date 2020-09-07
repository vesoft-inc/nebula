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

class SetTest : public TraverseTestBase {
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

TEST_F(SetTest, UnionAllTest) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : manu.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    manu.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id| "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " UNION ALL "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " UNION ALL "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " UNION ALL "
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " UNION ALL "
            "GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id UNION ALL GO FROM %ld OVER like YIELD "
            "like._dst as id)"
            " | GO FROM $-.id OVER serve"
            " YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        for (auto &like : tony.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
    }
    {
        cpp2::ExecutionResponse resp;
        // Although the corresponding column name and type not match,
        // we still do the union via implicit type casting for the query .
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name as name, $$.team.name as player"
                    " UNION ALL "
                    "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as name, serve.start_year";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"name"}, {"player"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, std::string> record(
                    tim.name(), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, std::string> record(
                    tony.name(),
                    folly::to<std::string>(std::get<2>(serve)));
            expected.emplace_back(std::move(record));
        }
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD $^.player.name as player, serve.start_year as start"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year";
        auto &nobody = players_["Nobody"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, nobody.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected;
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t> record(
                    tony.name(), std::get<2>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));

        std::vector<std::string> colsExpect = {"player", "start"};
        auto *cols = resp.get_column_names();
        ASSERT_EQ(cols->size(), colsExpect.size());
        for (decltype(colsExpect.size()) i = 0; i < colsExpect.size(); ++i) {
            ASSERT_EQ((*cols)[i], colsExpect[i]);
        }
    }
}

TEST_F(SetTest, UnionDistinct) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " UNION "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " UNION "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " UNION DISTINCT "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(SetTest, Minus) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " MINUS "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            if (player.name() == tony.name()) {
                continue;
            }
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(SetTest, Intersect) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " INTERSECT "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(SetTest, Mix) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "(GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " MINUS "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " UNION "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
            " INTERSECT "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), tim.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$^.player.name"}, {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : manu.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                manu.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(SetTest, AssignSet) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$var = "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;"
                    "YIELD $var.*";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.$^.player.name"}, {"$var.serve.start_year"}, {"$var.$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$var = "
                    "(GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION ALL "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name);"
                    "YIELD $var.*";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.$^.player.name"}, {"$var.serve.start_year"}, {"$var.$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tim.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tim.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "$var = (GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " MINUS "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;"
            "YIELD $var.*";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.$^.player.name"}, {"$var.serve.start_year"}, {"$var.$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &like : tim.likes()) {
            auto &player = players_[std::get<0>(like)];
            if (player.name() == tony.name()) {
                continue;
            }
            for (auto &serve : player.serves()) {
                std::tuple<std::string, int64_t, std::string> record(
                    player.name(), std::get<2>(serve), std::get<0>(serve));
                expected.emplace_back(std::move(record));
            }
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt =
            "$var = (GO FROM %ld OVER like YIELD like._dst as id | "
            "GO FROM $-.id OVER serve YIELD $^.player.name, serve.start_year, $$.team.name)"
            " INTERSECT "
            "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name;"
            "YIELD $var.*";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.$^.player.name"}, {"$var.serve.start_year"}, {"$var.$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, int64_t, std::string>> expected;
        for (auto &serve : tony.serves()) {
            std::tuple<std::string, int64_t, std::string> record(
                    tony.name(), std::get<2>(serve), std::get<0>(serve));
            expected.emplace_back(std::move(record));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(SetTest, EmptyInput) {
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
        auto *fmt = "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " MINUS "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " INTERSECT "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name";
        auto query = folly::stringPrintf(
                fmt, nonExistPlayerID, nonExistPlayerID, nonExistPlayerID, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve.start_year"}, {"$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$var = GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " MINUS "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name"
                    " INTERSECT "
                    "GO FROM %ld OVER serve YIELD serve.start_year, $$.team.name;"
                    "YIELD $var.*";
        auto query = folly::stringPrintf(
                fmt, nonExistPlayerID, nonExistPlayerID, nonExistPlayerID, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"$var.serve.start_year"}, {"$var.$$.team.name"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        ASSERT_EQ(nullptr, resp.get_rows());
    }
}

TEST_F(SetTest, SyntaxError) {
    {
        cpp2::ExecutionResponse resp;
        // This is an act of reservation.
        // For now, we treat it as an syntax error.
        auto query = "GO FROM 123 OVER like"
                     " YIELD like._src as src, like._dst as dst"
                     " | (GO FROM $-.src OVER serve"
                     " UNION GO FROM $-.dst OVER serve)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(SetTest, ExecutionError) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name1, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year"
                    " UNION "
                    "GO FROM %ld OVER serve YIELD $^.player.name, serve.start_year, $$.team.name";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}
}  // namespace graph
}  // namespace nebula
