/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class DataTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getClient();

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());
    }

    static void TearDownTestCase() {
        ASSERT_TRUE(removeData());
        client_.reset();
    }

protected:
    static AssertionResult prepareSchema();

    static AssertionResult removeData();

    static std::unique_ptr<GraphClient>         client_;
};

std::unique_ptr<GraphClient>         DataTest::client_{nullptr};

AssertionResult DataTest::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string host = folly::stringPrintf("127.0.0.1:%u", gEnv->storageServerPort());
        std::string cmd = "ADD HOSTS " + host;
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
        meta::TestUtils::registerHB(network::NetworkUtils::toHosts(host).value());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG person(name string, age int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG student(grade string, number int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE schoolmate(likeness int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    sleep(FLAGS_load_data_interval_secs + 3);
    return TestOK();
}

AssertionResult DataTest::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = folly::stringPrintf("REMOVE HOSTS 127.0.0.1:%u",
                                              gEnv->storageServerPort());
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }

    return TestOK();
}

TEST_F(DataTest, InsertVertex) {
    // Insert wrong type value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES hash(\"Tom\"):(\"Tom\", \"2\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert wrong num of value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name) VALUES hash(\"Tom\"):(\"Tom\", 2)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert wrong field
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(Name, age) VALUES hash(\"Tom\"):(\"Tom\", 3)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert vertex succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES hash(\"Tom\"):(\"Tom\", 22)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // One vertex multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(grade, number) "
                          "VALUES hash(\"Lucy\"):(\"Lucy\", 8, \"three\", 20190901001)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi vertices multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(grade, number) "
                          "VALUES hash(\"Laura\"):(\"Laura\", 8, \"three\", 20190901008),"
                          "hash(\"Amber\"):(\"Amber\", 9, \"four\", 20180901003)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi vertices one tag
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) "
                          "VALUES hash(\"Kitty\"):(\"Kitty\", 8), hash(\"Peter\"):(\"Peter\", 9)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):(85)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert multi edges
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Tom\")->hash(\"Kitty\"):(81),"
                          "hash(\"Tom\")->hash(\"Peter\"):(83)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Get result
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Tom\") OVER schoolmate YIELD $^.person.name,"
                          "schoolmate.likeness, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"Tom", 85, "Lucy"},
            {"Tom", 81, "Kitty"},
            {"Tom", 83, "Peter"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Get multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Lucy\")->hash(\"Laura\"):(90),"
                          "hash(\"Lucy\")->hash(\"Amber\"):(95)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Lucy\") OVER schoolmate YIELD "
                          "schoolmate.likeness, $$.person.name,"
                          "$$.student.grade, $$.student.number";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<int64_t, std::string, std::string, int64_t>;
        std::vector<valueType> expected = {
            {90, "Laura", "three", 20190901008},
            {95, "Amber", "four", 20180901003},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Multi sentences to insert multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age)"
                          "VALUES hash(\"Aero\"):(\"Aero\", 8);"
                          "INSERT VERTEX student(grade, number) "
                          "VALUES hash(\"Aero\"):(\"four\", 20190901003)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Laura\")->hash(\"Aero\"):(90)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Get result
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Laura\") OVER schoolmate "
                          "YIELD $$.student.number, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<int64_t, std::string>;
        std::vector<valueType> expected{
            {20190901003, "Aero"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Insert wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(\"87\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) "
                          "VALUES hash(\"Laura\")->hash(\"Amber\"):(\"hello\", \"87\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of prop
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(name, likeness) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(87)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong field name
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(like) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(88)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO: Test insert multi tags, and delete one of them then check other existent
}

}   // namespace graph
}   // namespace nebula

