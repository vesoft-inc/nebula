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
            return TestError() << "Do cmd:" << cmd << " failed";
        }
        meta::TestUtils::registerHB(network::NetworkUtils::toHosts(host).value());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG person(name string, age int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG student(name string, id int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG school(name string, addr string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE schoolmate(likeness int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_load_data_interval_secs + 1);
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
        std::string cmd = "INSERT VERTEX person(name, age) VALUES 1001:(\"peiqi\", \"2\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name) VALUES 1001:(\"peiqi\", 2)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong field
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(Name, age) VALUES 1001:(\"peiqi\", 3)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES 1001:(\"zhangsan\", 22)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES 1002:(\"lisi\", 23)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // One vertex multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(name, id) "
                          "VALUES 1003:(\"xiaoming\", 20, \"xiaoming\", 11)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi vertices multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(name, id) "
                          "VALUES 1005:(\"xiaoqiang\", 20, \"xiaoqaing\", 21),"
                          "1006:(\"xiaohong\", 19, \"xiaohong\", 22)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi vertices one tag
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) "
                          "VALUES 1007:(\"xiaogang\", 20), 1008:(\"xiaoxiao\", 21)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX school(name, addr) VALUES "
                          "2001:(\"schoolA\", \"Hangzhou\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX school(name, addr) VALUES "
                          "2002:(\"schoolB\", \"Beijing\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES 1001->1002:(80)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES 1001->1003:(85)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert multi edges
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "1001->1005:(81), 1001->1006:(83)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 1001 OVER schoolmate YIELD $^[person].name,"
                          "schoolmate.likeness, $$[person].name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"zhangsan", 80, "lisi"},
            {"zhangsan", 85, "xiaoming"},
            {"zhangsan", 81, "xiaoqiang"},
            {"zhangsan", 83, "xiaohong"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Insert wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES 1001->2002:(\"87\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) "
                          "VALUES 1001->2002:(\"hello\", \"87\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of prop
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(name, likeness) VALUES 1001->2002:(87)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong field name
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(like) VALUES 1001->2002:(88)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
