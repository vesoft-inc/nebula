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

TEST_F(FindPathTest, shortest) {
    {
        // 5662213458193308137<5,0>-7579316172763586624
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        // 5662213458193308137<5,0>3394245602834314645
        // 5662213458193308137<5,0>-7579316172763586624
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        // 5662213458193308137<5,0>-7579316172763586624<5,0>-1782445125509592239
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        // 5662213458193308137<5,0>3394245602834314645
        // 5662213458193308137<5,0>-7579316172763586624
        // 5662213458193308137<5,0>-7579316172763586624<5,0>-1782445125509592239
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld,%ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        // -8160811731890648949<5,0>5662213458193308137<5,0>-7579316172763586624<5,0>-1782445125509592239
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        // 5662213458193308137<5,0>3394245602834314645
        // 5662213458193308137<5,0>-7579316172763586624
        // 5662213458193308137<5,0>-7579316172763586624<5,0>-1782445125509592239
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
}

TEST_F(FindPathTest, all) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld,%ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto &manu = players_["Manu Ginobili"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid(), manu.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tim.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND ALL PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tiago = players_["Tiago Splitter"];
        auto &al = players_["LaMarcus Aldridge"];
        auto query = folly::stringPrintf(fmt, tiago.vid(), al.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
}
}  // namespace graph
}  // namespace nebula
