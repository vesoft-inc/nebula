/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/UpdateTestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class UpdateTest : public UpdateTestBase {
protected:
    void SetUp() override {
        UpdateTestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        UpdateTestBase::TearDown();
    }
};


TEST_F(UpdateTest, InvalidError) {
    // INSERT VERTEX course(name, credits),building(name) VALUES 102:("English", 6, "No11");
    {   // BaseLine: VALID Set, Where and Yield syntax
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 102 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No12\" "
                     "WHERE $^.course.name == \"English\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // Vertex INVALID YIELD(Yield)
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE OR INSERT VERTEX 102 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No12\" "
                     "WHERE $^.course.name == \"English\" && $^.course.credits > 2 "
                     "YIELD $$.course.name AS Name, $$.course.credits AS Credits, $$.building.name";
        auto code = client_->execute(query, resp);
        EXPECT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {   // Vertex E_INVALID_UPDATER(Set)
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 102 "
                     "SET course.credits = $$.course.credits + 1, $^.building.name = \"No12\" "
                     "WHERE $^.course.name == \"English\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        EXPECT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {   // Vertex E_INVALID_FILTER(Where)
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE OR INSERT VERTEX 102 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No12\" "
                     "WHERE $$.course.name == \"English\" && select.grade >= 3 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        EXPECT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
}


TEST_F(UpdateTest, UpdateVertex) {
    // INSERT VERTEX course(name, credits), building(name) VALUES 101:("Math", 3, "No5");
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No6\" ";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No7\" "
                     "WHERE $^.course.name == \"Math\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No8\" "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"Math", 6, "No8"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHERE $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"Math", 7, "No9"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Insertable: vertex 103 ("CS", 5) --> ("CS", 6, "No10")
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE OR INSERT VERTEX 103 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No10\" "
                     "WHERE $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"CS", 6, "No10"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(UpdateTest, UpdateEdge) {
    // OnlySet, SetFilter, SetYield, SetFilterYield, Insertable
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + 1, year = 2000";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + 1, year = 2000 "
                     "WHERE select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + 1, year = 2018 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 8, 2018},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHERE select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(UpdateTest, AdvancedUpdateEdge) {
    {  // Insertable
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE OR INSERT EDGE 201 -> 101@0 OF select "
                     "SET grade = 3, year = 2019 "
                     "WHERE $^.student.age > 15 && $^.student.gender == \"male\" "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {3, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // DestPropertyExpression
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + $$.course.credits, year = 2020 "
                     "WHERE select.grade > 4 && $^.student.age > 15 "
                           "&& $$.building.name == \"No9\" "
                     "YIELD $^.student.name AS Name, $$.course.credits AS Credits, "
                           "select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t, int64_t>> expected = {
            {"Monica", 7, 9 + 7, 2020},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // DestPropertyExpression + SamePropName
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + $$.course.credits, year = 2019 "
                     "WHERE select.grade > 4 && $^.student.age > 15 "
                           "&& $$.building.name == \"No9\" "
                     "YIELD $^.student.name AS Name, $$.course.name AS Course, "
                           "select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string, int64_t, int64_t>> expected = {
            {"Monica", "Math", 23, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {  // Insertable + DestPropertyExpression
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE OR INSERT EDGE 202 -> 101@0 OF select "
                     "SET grade = $$.course.credits + 1, year = 2020 "
                     "WHERE $^.student.gender == \"female\" && $$.course.name == \"Math\" "
                     "YIELD $^.student.name AS Name, $$.course.credits AS Credits, "
                           "select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t, int64_t>> expected = {
            {"Jane", 7, 7 + 1, 2020},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
