/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

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

    static std::unique_ptr<NebulaClientImpl>         client_;
};

std::unique_ptr<NebulaClientImpl>         DataTest::client_{nullptr};

AssertionResult DataTest::prepareSchema() {
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
    // create tag with default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG personWithDefault(name string DEFAULT \"\", "
                                                       "age int DEFAULT 18, "
                                                       "isMarried bool DEFAULT false, "
                                                       "BMI double DEFAULT 18.5, "
                                                       "department string DEFAULT \"engineering\","
                                                       "birthday timestamp DEFAULT "
                                                       "\"2020-01-10 10:00:00\")";
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
        std::string cmd = "CREATE TAG studentWithDefault"
                          "(grade string DEFAULT \"one\", number int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE schoolmate(likeness int, nickname string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    // create edge with default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE schoolmateWithDefault(likeness int DEFAULT 80)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    // Test same propName diff type in diff tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG employee(name int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    // Test same propName same type in diff tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG interest(name string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    // Insert timestamp
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG school(name string, create_time timestamp)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE study(start_time timestamp, end_time timestamp)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code "<< static_cast<int32_t>(code);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 3);
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
    return TestOK();
}

TEST_F(DataTest, InsertTest) {
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
    // Insert unordered order prop vertex succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(age, name) VALUES hash(\"Conan\"):(10, \"Conan\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        cmd = "FETCH PROP ON person hash(\"Conan\")";
        code = client_->execute(cmd, resp);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
                {std::hash<std::string>()("Conan"), "Conan", 10},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES uuid(\"Tom\"):(\"Tom\", 22)";
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
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(grade, number) "
                          "VALUES uuid(\"Lucy\"):(\"Lucy\", 8, \"three\", 20190901001)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert unordered order prop vertex succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(age, name),student(number, grade) "
                          "VALUES hash(\"Bob\"):(9, \"Bob\", 20191106001, \"four\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        cmd = "FETCH PROP ON person hash(\"Bob\")";
        code = client_->execute(cmd, resp);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
                {std::hash<std::string>()("Bob"), "Bob", 9},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        cmd = "FETCH PROP ON student hash(\"Bob\")";
        code = client_->execute(cmd, resp);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected2 = {
                {std::hash<std::string>()("Bob"), "four", 20191106001},
        };
        ASSERT_TRUE(verifyResult(resp, expected2));
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
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),student(grade, number) "
                          "VALUES uuid(\"Laura\"):(\"Laura\", 8, \"three\", 20190901008),"
                          "uuid(\"Amber\"):(\"Amber\", 9, \"four\", 20180901003)";
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
        std::string cmd = "INSERT VERTEX person(name, age) "
                          "VALUES uuid(\"Kitty\"):(\"Kitty\", 8), uuid(\"Peter\"):(\"Peter\", 9)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):(85, \"Lily\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Tom\")->uuid(\"Lucy\"):(85, \"Lucy\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert unordered order prop edge succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(nickname, likeness) VALUES "
                          "hash(\"Tom\")->hash(\"Bob\"):(\"Superman\", 87)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        cmd = "FETCH PROP ON schoolmate hash(\"Tom\")->hash(\"Bob\")";
        code = client_->execute(cmd, resp);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, std::string>> expected = {
            {std::hash<std::string>()("Tom"), std::hash<std::string>()("Bob"), 0,  87, "Superman"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Insert multi edges
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Tom\")->hash(\"Kitty\"):(81, \"Kitty\"),"
                          "hash(\"Tom\")->hash(\"Peter\"):(83, \"Kitty\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Tom\")->uuid(\"Kitty\"):(81, \"Kitty\"),"
                          "uuid(\"Tom\")->uuid(\"Peter\"):(83, \"Petter\")";
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
            {"Tom", 87, "Bob"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Tom\") OVER schoolmate YIELD $^.person.name,"
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
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Lucy\")->hash(\"Laura\"):(90, \"Laura\"),"
                          "hash(\"Lucy\")->hash(\"Amber\"):(95, \"Amber\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Lucy\")->uuid(\"Laura\"):(90, \"Laura\"),"
                          "uuid(\"Lucy\")->uuid(\"Amber\"):(95, \"Amber\")";
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
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Lucy\") OVER schoolmate YIELD "
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
        std::string cmd = "INSERT VERTEX person(name, age) "
                          "VALUES hash(\"Aero\"):(\"Aero\", 8);"
                          "INSERT VERTEX student(grade, number) "
                          "VALUES hash(\"Aero\"):(\"four\", 20190901003)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age)"
                          "VALUES uuid(\"Aero\"):(\"Aero\", 8);"
                          "INSERT VERTEX student(grade, number) "
                          "VALUES uuid(\"Aero\"):(\"four\", 20190901003)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Laura\")->hash(\"Aero\"):(90, \"Aero\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Laura\")->uuid(\"Aero\"):(90, \"Aero\")";
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
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Laura\") OVER schoolmate "
                          "YIELD $$.student.number, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<int64_t, std::string>;
        std::vector<valueType> expected{
            {20190901003, "Aero"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test same prop name diff type in diff tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),employee(name) "
                          "VALUES hash(\"Joy\"):(\"Joy\", 18, 123),"
                          "hash(\"Petter\"):(\"Petter\", 19, 456)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),employee(name) "
                          "VALUES uuid(\"Joy\"):(\"Joy\", 18, 123),"
                          "uuid(\"Petter\"):(\"Petter\", 19, 456)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Joy\")->hash(\"Petter\"):(90, \"Petter\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Joy\")->uuid(\"Petter\"):(90, \"Petter\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Joy\") OVER schoolmate YIELD $^.person.name,"
                          "schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t, int64_t>> expected = {
            {"Joy", 90, "Petter", 19, 456},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Joy\") OVER schoolmate YIELD $^.person.name,"
                          "schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t, int64_t>> expected = {
            {"Joy", 90, "Petter", 19, 456},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test same prop name same type in diff tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),interest(name) "
                          "VALUES hash(\"Bob\"):(\"Bob\", 19, \"basketball\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age),interest(name) "
                          "VALUES uuid(\"Bob\"):(\"Bob\", 19, \"basketball\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Petter\")->hash(\"Bob\"):(90, \"Bob\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Petter\")->uuid(\"Bob\"):(90, \"Bob\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Petter\") OVER schoolmate "
                          "YIELD $^.person.name, $^.employee.name, "
                          "schoolmate.likeness, $$.person.name,"
                          "$$.interest.name, $$.person.age";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using type = std::tuple<std::string, int64_t, int64_t, std::string, std::string, int64_t>;
        std::vector<type> expected = {
            {"Petter", 456, 90, "Bob", "basketball", 19},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Petter\") OVER schoolmate "
                          "YIELD $^.person.name, $^.employee.name, "
                          "schoolmate.likeness, $$.person.name,"
                          "$$.interest.name, $$.person.age";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using type = std::tuple<std::string, int64_t, int64_t, std::string, std::string, int64_t>;
        std::vector<type> expected = {
            {"Petter", 456, 90, "Bob", "basketball", 19},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Insert wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(\"87\", "")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) "
                          "VALUES hash(\"Laura\")->hash(\"Amber\"):(\"hello\", \"87\", "")";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong num of prop
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(87)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert wrong field name
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(like, HH) VALUES "
                          "hash(\"Laura\")->hash(\"Amber\"):(88)";
        auto code = client_->execute(cmd, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert invalid timestamp
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE study(start_time, end_time) "
                          "VALUES hash(\"Laura\")->hash(\"sun_school\"):"
                          "(\"2300-01-01 10:00:00\", now()+3600*24*365*3)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert timestamp succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX school(name, create_time) VALUES "
                          "hash(\"sun_school\"):(\"sun_school\", \"2010-01-01 10:00:00\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX school(name, create_time) VALUES "
                          "uuid(\"sun_school\"):(\"sun_school\", \"2010-01-01 10:00:00\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE study(start_time, end_time) "
                          "VALUES hash(\"Laura\")->hash(\"sun_school\"):"
                          "(\"2019-01-01 10:00:00\", now()+3600*24*365*3)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE study(start_time, end_time) "
                          "VALUES uuid(\"Laura\")->uuid(\"sun_school\"):"
                          "(\"2019-01-01 10:00:00\", now()+3600*24*365*3)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Laura\") OVER study "
                          "YIELD $$.school.name, study._dst, "
                          "$$.school.create_time, (string)study.start_time";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t, std::string>> expected = {
            {"sun_school", std::hash<std::string>()("sun_school"),  1262311200, "1546308000"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Laura\") OVER study "
                          "YIELD $$.school.name,"
                          "$$.school.create_time, (string)study.start_time";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"sun_school", 1262311200, "1546308000"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON school hash(\"sun_school\") ";;
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
                {std::hash<std::string>()("sun_school"), "sun_school", 1262311200},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON school uuid(\"sun_school\") ";;
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
                {"sun_school", 1262311200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true,  {0}));
    }
    // TODO: Test insert multi tags, and delete one of them then check other existent
}

TEST_F(DataTest, InsertWithDefaultValueTest) {
    // Insert vertex using default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault() "
                          "VALUES hash(\"\"):()";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert lack of the column value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault"
                          "(age, isMarried, BMI)"
                          "VALUES hash(\"Tom\"):(18, false)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert column doesn't match value count
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX studentWithDefault"
                          "(grade, number)"
                          "VALUES hash(\"Tom\"):(\"one\", 111, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault(name) "
                          "VALUES hash(\"Tom\"):(\"Tom\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault(name, age) "
                          "VALUES hash(\"Tom\"):(\"Tom\", 20)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault(name, BMI) "
                          "VALUES hash(\"Tom\"):(\"Tom\", 20.5)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert vertices multi tags
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault(name, BMI),"
                          "studentWithDefault(number) VALUES "
                          "hash(\"Laura\"):(\"Laura\", 21.5, 20190901008),"
                          "hash(\"Amber\"):(\"Amber\", 22.5, 20180901003)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi vertices one tag
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX personWithDefault(name) VALUES "
                          "hash(\"Kitty\"):(\"Kitty\"), "
                          "hash(\"Peter\"):(\"Peter\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert lack of the column value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmateWithDefault(likeness) VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):()";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Column count doesn't match value count
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmateWithDefault(likeness) VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):(60, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmateWithDefault() VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):()";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmateWithDefault(likeness, redundant) VALUES "
                          "hash(\"Tom\")->hash(\"Lucy\"):(90, 0)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Insert multi edges with default value
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmateWithDefault() VALUES "
                          "hash(\"Tom\")->hash(\"Kitty\"):(),"
                          "hash(\"Tom\")->hash(\"Peter\"):(),"
                          "hash(\"Lucy\")->hash(\"Laura\"):(),"
                          "hash(\"Lucy\")->hash(\"Amber\"):()";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Get result
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Tom\") OVER schoolmateWithDefault YIELD $^.person.name,"
                          "schoolmateWithDefault.likeness, $$.person.name";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"Tom", 80, "Lucy"},
            {"Tom", 80, "Kitty"},
            {"Tom", 80, "Peter"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Lucy\") OVER schoolmateWithDefault YIELD "
                          "schoolmateWithDefault.likeness, $$.personWithDefault.name,"
                          "$$.personWithDefault.birthday, $$.personWithDefault.department,"
                          "$$.studentWithDefault.grade, $$.studentWithDefault.number";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<int64_t, std::string, int64_t,
                                     std::string, std::string, int64_t>;
        std::vector<valueType> expected = {
            {80, "Laura", 1578621600, "engineering", "one", 20190901008},
            {80, "Amber", 1578621600, "engineering", "one", 20180901003},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DataTest, InsertMultiVersionTest) {
    // Insert multi version vertex
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "hash(\"Tony\"):(\"Tony\", 18), "
                          "hash(\"Mack\"):(\"Mack\", 19)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "hash(\"Mack\"):(\"Mack\", 20)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "hash(\"Mack\"):(\"Mack\", 21)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert multi version edge
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Tony\")->hash(\"Mack\")@1:(1, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Tony\")->hash(\"Mack\")@1:(2, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "hash(\"Tony\")->hash(\"Mack\")@1:(3, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Get result
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM hash(\"Tony\") OVER schoolmate "
                          "YIELD $$.person.name, $$.person.age, schoolmate.likeness";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, int64_t>;
        // Get the latest result
        std::vector<valueType> expected{
            {"Mack", 21, 3},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DataTest, InsertMultiVersionWithUUIDTest) {
    // Insert multi version vertex
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "uuid(\"Tony\"):(\"Tony\", 18), "
                          "uuid(\"Mack\"):(\"Mack\", 19)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "uuid(\"Mack\"):(\"Mack\", 20)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(name, age) VALUES "
                          "uuid(\"Mack\"):(\"Mack\", 21)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Insert multi version edge
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Tony\")->uuid(\"Mack\")@1:(1, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Tony\")->uuid(\"Mack\")@1:(2, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE schoolmate(likeness, nickname) VALUES "
                          "uuid(\"Tony\")->uuid(\"Mack\")@1:(3, \"\")";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Get result
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM uuid(\"Tony\") OVER schoolmate "
                          "YIELD $$.person.name, $$.person.age, schoolmate.likeness";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        using valueType = std::tuple<std::string, int64_t, int64_t>;
        // Get the latest result
        std::vector<valueType> expected{
            {"Mack", 21, 3},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(DataTest, LookupTest) {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "LOOKUP ON person where person.name == \"Tony\" "
                          "YIELD person.name, person.age";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(DataTest, MatchTest) {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "MATCH";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}


static inline void execute(NebulaClientImpl* client, const std::string& nGQL) {
    cpp2::ExecutionResponse resp;
    auto code = client->execute(nGQL, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << "Do cmd:" << nGQL << " failed";
}

class FetchEmptyPropsTest : public TestBase {
protected:
    void SetUp() override {
        // Access the Nebula Environment
        client_ = gEnv->getClient();
        ASSERT_NE(client_, nullptr);

        prepareSchema();
        prepareData();
    }

    void TearDown() override {
        execute(client_.get(), "DROP SPACE empty");
    }

    std::unique_ptr<NebulaClientImpl> client_ = nullptr;

private:
    void prepareSchema() {
        execute(client_.get(), "CREATE SPACE empty(partition_num=1, replica_factor=1)");

        const std::vector<std::string> queries = {
            "USE empty",
            "CREATE TAG empty_tag_0()",
            "CREATE TAG empty_tag_1()",
            "CREATE EDGE empty_edge()"
        };

        for (auto& q : queries) {
            execute(client_.get(), q);
        }

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }

    void prepareData() {
        const std::vector<std::string> queries = {
            "INSERT VERTEX empty_tag_0() values 1:(), 2:()",
            "INSERT VERTEX empty_tag_1() values 1:(), 2:()",
            "INSERT EDGE empty_edge() values 1->2:()",
        };

        for (auto& q : queries) {
            execute(client_.get(), q);
        }
    }
};

// #1478
// Fetch property from empty tag
TEST_F(FetchEmptyPropsTest, EmptyProps) {
    {
        cpp2::ExecutionResponse resp;
        const std::string stmt = "FETCH PROP ON empty_tag_0 1";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        const std::string stmt = "FETCH PROP ON * 1";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        const std::string stmt = "FETCH PROP ON empty_edge 1->2";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {1, 2, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(FetchEmptyPropsTest, WithInput) {
    {
        cpp2::ExecutionResponse resp;
        const std::string stmt = "GO FROM 1 OVER empty_edge YIELD empty_edge._dst as id"
                                 " | FETCH PROP ON empty_tag_0 $-.id";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t>> expected = {
            {2}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        const std::string stmt =
            "GO FROM 1 OVER empty_edge YIELD empty_edge._src as src, empty_edge._dst as dst"
            " | FETCH PROP ON empty_edge $-.src->$-.dst";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {1, 2, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
