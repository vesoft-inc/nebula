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


TEST_F(UpdateTest, UpdateVertex) {
    // INSERT VERTEX course(name, credits), building(name) VALUES 101:("Math", 3, "No5");
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No6\" ";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX uuid(\"Math\") "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No6\" ";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No7\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX uuid(\"Math\") "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No7\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2";
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
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX uuid(\"Math\") "
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
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"Math", 7, "No9"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX uuid(\"Math\") "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
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
        auto query = "UPSERT VERTEX 103 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No10\" "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
            {"CS", 6, "No10"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX uuid(\"CS\") "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No10\" "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
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
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                     "SET grade = select.grade + 1, year = 2000";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET grade = select.grade + 1, year = 2000 "
                     "WHEN select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                     "SET grade = select.grade + 1, year = 2000 "
                     "WHEN select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
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
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
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
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {  // Insertable
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 201 -> 101@0 OF select "
                     "SET grade = 3, year = 2019 "
                     "WHEN $^.student.age > 15 && $^.student.gender == \"male\" "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {3, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE uuid(\"Mike\") -> uuid(\"Math\")@0 OF select "
                     "SET grade = 3, year = 2019 "
                     "WHEN $^.student.age > 15 && $^.student.gender == \"male\" "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t>> expected = {
            {3, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_F(UpdateTest, InvalidTest) {
    {   // update vertex: the item is TagName.PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET credits = $^.course.credits + 1, name = \"No9\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {   // update edge: the item is PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET select.grade = select.grade + 1, select.year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // the $$.TagName.PropName expressions are not allowed in any update sentence
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET course.credits = $$.course.credits + 1 "
                     "WHEN $$.course.name == \"Math\" && $^.course.credits > $$.course.credits + 1 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $$.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure the vertex must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101000000000000 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure TagName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101 "
                     "SET nonexistentTag.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.nonexistentProperty > 2 "
                     "YIELD $^.course.name AS Name, $^.nonexistentTag.nonexistentProperty";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101000000000000@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, edge_tyep, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF nonexistentEdgeTypeName "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, edge_tyep, ranking<default is 0>, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101 OF select "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, edge_tyep, ranking, dst) must exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@1000000000000 OF select "
                     "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure EdgeName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@0 OF select "
                     "SET nonexistentProperty = select.grade + 1, year = 2019 "
                     "WHEN nonexistentEdgeName.grade > 4 && $^.student.nonexistentProperty > 15 "
                     "YIELD $^.nonexistentTag.name AS Name, select.nonexistentProperty AS Grade";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
}


}   // namespace graph
}   // namespace nebula
