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

class FindPathTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
    }

    void TearDown() override {
        TraverseTestBase::TearDown();
    }
};

TEST_F(FindPathTest, SingleEdgeShortest) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "-8160811731890648949<like,0>5662213458193308137<like,0>-7579316172763586624"
                "<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        // we only find the shortest path to the dest,
        // so -8160811731890648949 to -1782445125509592239 is not in result
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld,%ld TO %ld,%ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tiago = players_["Tiago Splitter"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tiago.vid(),
                tony.vid(), manu.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like yield like._src AS src, like._dst AS dst | "
                    "FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$var = GO FROM %ld OVER like yield like._src AS src, like._dst AS dst;"
                    "FIND SHORTEST PATH FROM $var.src TO $var.dst OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "$var = GO FROM %ld OVER like yield like._src AS src;"
                    "GO FROM %ld OVER like yield like._src AS src, like._dst AS dst | "
                    "FIND SHORTEST PATH FROM $var.src TO $-.dst OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeAll) {
    /*
     * TODO: There might exist loops when find all path,
     * we should provide users with an option on whether or not a loop is required.
     */
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<like,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld,%ld OVER like UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<like,0>3394245602834314645",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<like,0>3394245602834314645"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 3 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "-8160811731890648949<like,0>5662213458193308137<like,0>-7579316172763586624"
                "<like,0>-1782445125509592239"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, MultiEdgesShortest) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like,serve UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER like,serve UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld,%ld,%ld "
                    "OVER like,serve UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt,
                tim.vid(),
                tony.vid(), manu.vid(), al.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239",
            "5662213458193308137<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER like,serve UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "-8160811731890648949<like,0>5662213458193308137<like,0>-7579316172763586624"
                "<like,0>-1782445125509592239",
            "-8160811731890648949<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER * UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<teammate,0>-7579316172763586624"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER * UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<serve,0>7193291116733635180",
            "5662213458193308137<teammate,0>-7579316172763586624",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld,%ld,%ld "
                    "OVER * UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt,
                tim.vid(),
                tony.vid(), manu.vid(), al.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645",
            "5662213458193308137<serve,0>7193291116733635180",
            "5662213458193308137<teammate,0>-1782445125509592239",
            "5662213458193308137<teammate,0>-7579316172763586624",
            "5662213458193308137<teammate,0>3394245602834314645"
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER * UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "-8160811731890648949<like,0>5662213458193308137<teammate,0>-1782445125509592239",
            "-8160811731890648949<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, MultiEdgesAll) {
    /*
     * TODO: There might exist loops when find all path,
     * we should provide users with an option on whether or not a loop is required.
     */
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld,%ld OVER like,serve UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<serve,0>7193291116733635180",
            "5662213458193308137<like,0>3394245602834314645<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>3394245602834314645"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld,%ld OVER like,serve UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), teams_["Spurs"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            "5662213458193308137<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<like,0>-7579316172763586624",
            "5662213458193308137<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<serve,0>7193291116733635180",
            "5662213458193308137<like,0>3394245602834314645<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>3394245602834314645"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>-7579316172763586624<like,0>5662213458193308137"
                "<serve,0>7193291116733635180",
            "5662213458193308137<like,0>3394245602834314645<like,0>5662213458193308137"
                "<serve,0>7193291116733635180",
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, VertexNotExist) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto query = folly::stringPrintf(fmt,
                std::hash<std::string>()("Nobody1"), std::hash<std::string>()("Nobody2"));
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid(), std::hash<std::string>()("Nobody"));
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, std::hash<std::string>()("Nobody"), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto query = folly::stringPrintf(fmt,
                std::hash<std::string>()("Nobody1"), std::hash<std::string>()("Nobody2"));
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid(), std::hash<std::string>()("Nobody"));
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, std::hash<std::string>()("Nobody"), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected;
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}
}  // namespace graph
}  // namespace nebula
