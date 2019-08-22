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

TEST_F(FindPathTest, base) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "FIND SHORTEST PATH FROM %ld TO %ld OVER like UPTO 5 STEPS";
        auto &tim = players_["Tim Duncan"];
        auto &tony = players_["Tony Parker"];
        auto query = folly::stringPrintf(fmt, tim.vid(), tony.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
}
}  // namespace graph
}  // namespace nebula
