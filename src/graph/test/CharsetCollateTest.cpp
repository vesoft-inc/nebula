/* Copyright (c) 2020 vesoft inc. All rights reserved.
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

class CharsetCollateTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};

TEST_F(CharsetCollateTest, ShowCharset) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW CHARSET";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string,
                    std::string, int>> expected {
            {"utf8", "UTF-8 Unicode", "utf8_bin", 4},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW CHARSETS";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

TEST_F(CharsetCollateTest, ShowCollate) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATION";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected {
            {"utf8_bin", "utf8"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATE";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SHOW COLLATIONS";
        auto query = fmt;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}

}   // namespace graph
}   // namespace nebula
