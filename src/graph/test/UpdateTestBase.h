/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_UPDATETESTBASE_H
#define GRAPH_TEST_UPDATETESTBASE_H

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
// https://github.com/vesoft-inc/nebula/blob/master/docs/get-started.md#build-your-own-graph
class UpdateTestBase : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
        client_ = gEnv->getClient();
        storagePort_ = gEnv->storageServerPort();

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());

        ASSERT_TRUE(prepareData());
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();

        ASSERT_TRUE(removeData());
        client_.reset();
    }

    static AssertionResult prepareSchema();

    static AssertionResult prepareData();

    static AssertionResult removeData();

protected:
    static uint16_t                             storagePort_;
    static std::unique_ptr<NebulaClientImpl>         client_;
};

uint16_t UpdateTestBase::storagePort_ = 0;

std::unique_ptr<NebulaClientImpl> UpdateTestBase::client_;

// static
AssertionResult UpdateTestBase::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE myspace_test2(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE myspace_test2";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG course(name string, credits int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG building(name string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG student(name string, age int, gender string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    // create tag with default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG student_default(name string, age int, "
                          "gender string DEFAULT \"one\", birthday int DEFAULT 2010)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE like(likeness double)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE select(grade int, year int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    // create edge with default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE select_default(grade int, "
                          "year TIMESTAMP DEFAULT 1546308000)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 3);
    return TestOK();
}


AssertionResult UpdateTestBase::prepareData() {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE myspace_test2";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "USE myspace_test2 failed"
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert vertices 'student'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT VERTEX student(name, age, gender) VALUES "
                "200:(\"Monica\", 16, \"female\"), "
                "201:(\"Mike\", 18, \"male\"), "
                "202:(\"Jane\", 17, \"female\")";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'student' failed: "
                               << static_cast<int32_t>(code);
        }
        // Insert vertices 'course' and 'building'
        query.clear();
        query = "INSERT VERTEX course(name, credits),building(name) VALUES "
                "101:(\"Math\", 3, \"No5\"), "
                "102:(\"English\", 6, \"No11\")";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' and 'building' failed: "
                               << static_cast<int32_t>(code);
        }
        // Insert vertices only 'course'(for update insertable testing)
        query.clear();
        query = "INSERT VERTEX course(name, credits) VALUES 103:(\"CS\", 5)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert vertices 'student' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT VERTEX student(name, age, gender) VALUES "
                "uuid(\"Monica\"):(\"Monica\", 16, \"female\"), "
                "uuid(\"Mike\"):(\"Mike\", 18, \"male\"), "
                "uuid(\"Jane\"):(\"Jane\", 17, \"female\")";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'student' failed: "
                               << static_cast<int32_t>(code);
        }
        query.clear();
        query = "INSERT VERTEX course(name, credits),building(name) VALUES "
                "uuid(\"Math\"):(\"Math\", 3, \"No5\"), "
                "uuid(\"English\"):(\"English\", 6, \"No11\")";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' and 'building' failed: "
                               << static_cast<int32_t>(code);
        }
        query.clear();
        query = "INSERT VERTEX course(name, credits) VALUES uuid(\"CS\"):(\"CS\", 5)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert edges 'select' and 'like'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT EDGE select(grade, year) VALUES "
                "200 -> 101@0:(5, 2018), "
                "200 -> 102@0:(3, 2018), "
                "201 -> 102@0:(3, 2019), "
                "202 -> 102@0:(3, 2019)";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'select' failed: "
                               << static_cast<int32_t>(code);
        }
        query.clear();
        query = "INSERT EDGE like(likeness) VALUES "
                "200 -> 201@0:(92.5), "
                "201 -> 200@0:(85.6), "
                "201 -> 202@0:(93.2)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'like' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert edges 'select' and 'like' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT EDGE select(grade, year) VALUES "
                "uuid(\"Monica\") -> uuid(\"Math\")@0:(5, 2018), "
                "uuid(\"Monica\") -> uuid(\"English\")@0:(3, 2018), "
                "uuid(\"Mike\") -> uuid(\"English\")@0:(3, 2019), "
                "uuid(\"Jane\") -> uuid(\"English\")@0:(3, 2019)";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'select' failed: "
                               << static_cast<int32_t>(code);
        }
        query.clear();
        query = "INSERT EDGE like(likeness) VALUES "
                "uuid(\"Monica\") -> uuid(\"Mike\")@0:(92.5), "
                "uuid(\"Mike\") -> uuid(\"Monica\")@0:(85.6), "
                "uuid(\"Mike\") -> uuid(\"Jane\")@0:(93.2)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'like' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    return TestOK();
}

AssertionResult UpdateTestBase::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE myspace_test2";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    return TestOK();
}

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_TEST_UPDATETESTBASE_H
