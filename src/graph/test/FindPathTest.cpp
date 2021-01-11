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
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239",
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
            "5662213458193308137<like,0>-7579316172763586624<like,0>-1782445125509592239",
            "-8160811731890648949<like,0>3394245602834314645"
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

TEST_F(FindPathTest, SingleEdgeShortestReversely) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like REVERSELY UPTO 5 STEPS";
        auto &tony = players_["Tony Parker"];
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like REVERSELY UPTO 5 STEPS";
        auto &tony = players_["Tony Parker"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &rudy = players_["Rudy Gay"];
        auto query = folly::stringPrintf(fmt, tony.vid(), rudy.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), al.vid(), rudy.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        // we only find the shortest path to the dest
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER like REVERSELY UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &manu = players_["Manu Ginobili"];
        auto &yao = players_["Yao Ming"];
        auto &oneal = players_["Shaquile O'Neal"];
        auto query = folly::stringPrintf(fmt, tim.vid(), manu.vid(), yao.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tim.vid(), manu.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), oneal.vid(), yao.vid()),
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeShortestBidirect) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 5 STEPS";
        auto &tony = players_["Tony Parker"];
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<like,0>%ld",
                                tony.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 5 STEPS";
        auto &tony = players_["Tony Parker"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &rudy = players_["Rudy Gay"];
        auto query = folly::stringPrintf(fmt, tony.vid(), rudy.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), al.vid(), rudy.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld",
                                tony.vid(), al.vid(), rudy.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        // we only find the shortest path to the dest
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld,%ld OVER like BIDIRECT UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tiago.vid(),
                                         tony.vid(), manu.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld",
                                tiago.vid(), manu.vid(), tony.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld",
                                tiago.vid(), tim.vid(), tony.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld",
                                tiago.vid(), tim.vid(), tony.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld",
                                tiago.vid(), tim.vid(), al.vid()),
            folly::stringPrintf("%ld<like,0>%ld",
                                tiago.vid(), manu.vid())
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

TEST_F(FindPathTest, SingleEdgeAllReversely) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like REVERSELY UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), al.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), tim.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), tim.vid(), manu.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld,%ld TO %ld OVER like REVERSELY UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tony.vid(), manu.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld",
                                manu.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                manu.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), al.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), tim.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                manu.vid(), tim.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tony.vid(), tim.vid(), manu.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                manu.vid(), tim.vid(), manu.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like REVERSELY UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tim.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), tony.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), manu.vid(), tim.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), manu.vid(), tony.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), tony.vid(), tim.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), al.vid(), tony.vid(), al.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeAllBidirect) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 6 STEPS";
        auto &jaylen = players_["Jaylen Adams"];
        auto &steven = players_["Steven Adams"];
        auto &kyle = players_["Kyle Alexander"];
        auto &bam = players_["Bam Adebayo"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, jaylen.vid(), ag.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                jaylen.vid(), steven.vid(), kyle.vid(), ag.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld<like,0>%ld<like,0>%ld",
                                jaylen.vid(), bam.vid(), steven.vid(), kyle.vid(), ag.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<-like,0>%ld<like,0>%ld<like,0>"
                                "%ld<like,0>%ld",
                                jaylen.vid(), steven.vid(), bam.vid(), jaylen.vid(), steven.vid(),
                                kyle.vid(), ag.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 6 STEPS";
        auto &aj = players_["Jarrett Allen"];
        auto &justin = players_["Justin Anderson"];
        auto &kyle = players_["Kyle Alexander"];
        auto &og = players_["OG Anunoby"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, kyle.vid(), og.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<-like,0>%ld",
                                kyle.vid(), ag.vid(), aj.vid(), og.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                kyle.vid(), ag.vid(), aj.vid(), justin.vid(), og.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<-like,0>%ld<-like,0>%ld<-like,0>"
                                "%ld<-like,0>%ld",
                                kyle.vid(), ag.vid(), aj.vid(), og.vid(), justin.vid(),
                                aj.vid(), og.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 4 STEPS";
        auto &aj = players_["Jarrett Allen"];
        auto &justin = players_["Justin Anderson"];
        auto &og = players_["OG Anunoby"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, ag.vid(), aj.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld",
                                ag.vid(), aj.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                ag.vid(), aj.vid(), justin.vid(), og.vid(), aj.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                ag.vid(), aj.vid(), og.vid(), justin.vid(), aj.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeNoLoop) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld",
                                tim.vid(), tony.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld",
                                tim.vid(), tony.vid()),
            folly::stringPrintf("%ld<like,0>%ld",
                                tim.vid(), manu.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld",
                                tim.vid(), tony.vid(), manu.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, al.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld",
                                al.vid(), tim.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld",
                                al.vid(), tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                al.vid(), tony.vid(), manu.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeNoLoopReversely) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like REVERSELY UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tony.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld,%ld TO %ld OVER like REVERSELY UPTO 3 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tony.vid(), manu.vid(), tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tony.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld",
                                manu.vid(), tim.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                manu.vid(), tony.vid(), tim.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like REVERSELY UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<-like,0>%ld",
                                tim.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), tony.vid(), al.vid()),
            folly::stringPrintf("%ld<-like,0>%ld<-like,0>%ld<-like,0>%ld",
                                tim.vid(), manu.vid(), tony.vid(), al.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
}

TEST_F(FindPathTest, SingleEdgeNoLoopBidirect) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 6 STEPS";
        auto &jaylen = players_["Jaylen Adams"];
        auto &steven = players_["Steven Adams"];
        auto &kyle = players_["Kyle Alexander"];
        auto &bam = players_["Bam Adebayo"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, jaylen.vid(), ag.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                jaylen.vid(), steven.vid(), kyle.vid(), ag.vid()),
            folly::stringPrintf("%ld<like,0>%ld<-like,0>%ld<like,0>%ld<like,0>%ld",
                                jaylen.vid(), bam.vid(), steven.vid(), kyle.vid(), ag.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 6 STEPS";
        auto &aj = players_["Jarrett Allen"];
        auto &justin = players_["Justin Anderson"];
        auto &kyle = players_["Kyle Alexander"];
        auto &og = players_["OG Anunoby"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, kyle.vid(), og.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<-like,0>%ld",
                                kyle.vid(), ag.vid(), aj.vid(), og.vid()),
            folly::stringPrintf("%ld<like,0>%ld<like,0>%ld<like,0>%ld<like,0>%ld",
                                kyle.vid(), ag.vid(), aj.vid(), justin.vid(), og.vid())
        };
        ASSERT_TRUE(verifyPath(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND NOLOOP PATH FROM %ld TO %ld OVER like BIDIRECT UPTO 4 STEPS";
        auto &aj = players_["Jarrett Allen"];
        auto &ag = players_["Grayson Allen"];
        auto query = folly::stringPrintf(fmt, ag.vid(), aj.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        std::vector<std::string> expected = {
            folly::stringPrintf("%ld<like,0>%ld",
                                ag.vid(), aj.vid())
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
        auto query = folly::stringPrintf(fmt, std::hash<std::string>()("Nobody1"),
            std::hash<std::string>()("Nobody2"));
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
        auto query = folly::stringPrintf(fmt, std::hash<std::string>()("Nobody1"),
            std::hash<std::string>()("Nobody2"));
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

TEST_F(FindPathTest, DuplicateColumn) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER like yield like._src AS src, like._dst AS src| "
                    "FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto query = folly::stringPrintf(fmt, tim.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << *(resp.get_error_msg());
    }
}

TEST_F(FindPathTest, EmptyInput) {
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
        auto *fmt = "GO FROM %ld OVER like yield like._src AS src, like._dst AS dst| "
                    "FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS";
        auto query = folly::stringPrintf(fmt, nonExistPlayerID);
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << *(resp.get_error_msg());
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "GO FROM %ld OVER serve "
                    "YIELD serve._src as src, serve._dst as dst, serve.start_year as start "
                    "| YIELD $-.src as arc, $-.dst as dst WHERE $-.start > 20000 "
                    "| FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS";
        auto query = folly::stringPrintf(fmt, players_["Marco Belinelli"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << *(resp.get_error_msg());
    }
}

TEST_F(FindPathTest, UseUUID) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "FIND SHORTEST PATH FROM UUID(\"Tim Duncan\") TO UUID(\"Tony Parker\") "
                     "OVER like UPTO 5 STEPS";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        ASSERT_TRUE(resp.get_rows() != nullptr);
        ASSERT_EQ(resp.get_rows()->size(), 1);
    }
    {
        // without use space
        auto client = gEnv->getClient();
        cpp2::ExecutionResponse resp;
        auto query = "FIND SHORTEST PATH FROM UUID(\"Tim Duncan\") TO UUID(\"Tony Parker\") "
                     "OVER like UPTO 5 STEPS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
        ASSERT_EQ("Please choose a graph space with `USE spaceName' firstly",
                  *(resp.get_error_msg()));

        // multi sentences
        query = "USE nba; FIND SHORTEST PATH FROM UUID(\"Tim Duncan\") TO UUID(\"Tony Parker\") "
                "OVER like UPTO 5 STEPS";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << *(resp.get_error_msg());
        ASSERT_TRUE(resp.get_rows() != nullptr);
        ASSERT_EQ(resp.get_rows()->size(), 1);
    }
}
}  // namespace graph
}  // namespace nebula
