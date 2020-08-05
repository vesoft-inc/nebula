/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef GRAPH_TEST_LOOKUPTESTBASE_H
#define GRAPH_TEST_LOOKUPTESTBASE_H


#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class LookupTestBase : public TestBase {
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
        storagePort_ = gEnv->storageServerPort();

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());
    }

    static void TearDownTestCase() {
        ASSERT_TRUE(removeData());
        client_.reset();
    }

    static AssertionResult prepareSchema();

    static AssertionResult removeData();

protected:
    static uint16_t storagePort_;
    static std::unique_ptr<GraphClient> client_;
};

uint16_t LookupTestBase::storagePort_ = 0;
std::unique_ptr<GraphClient> LookupTestBase::client_;

// static
AssertionResult LookupTestBase::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE lookup_space(partition_num=3, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE lookup_space";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG lookup_tag_1(col1 string, col2 string, col3 string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG lookup_tag_2(col1 string, col2 int, col3 double, col4 bool)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE lookup_edge_1(col1 string, col2 string, col3 string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE lookup_edge_2(col1 string,"
                          "col2 int, col3 double, col4 bool)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 3);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX t_index_2 ON lookup_tag_2(col2, col3, col4)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX t_index_4 ON lookup_tag_2(col3, col4)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX t_index_5 ON lookup_tag_2(col4)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE INDEX e_index_2 ON lookup_edge_2(col2, col3, col4)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE INDEX e_index_4 ON lookup_edge_2(col3, col4)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 3);
    return TestOK();
}

AssertionResult LookupTestBase::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE lookup_space";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    return TestOK();
}

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_TEST_LOOKUPTESTBASE_H
