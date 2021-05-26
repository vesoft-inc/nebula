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

namespace nebula {
namespace graph {

// Update/Upsert
class UpdateUpsertTest : public UpdateTestBase, public ::testing::WithParamInterface<std::string> {
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

// Update specific fixture
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

// Upsert specific fixture
class UpsertTest : public UpdateTestBase {
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

// Update/Upsert behavior
// Update the not exist vertexId/edgeId will return error
// Update the not exist vertex(tag)/edge_type will return error
// Update the exist vertex/edge but filter out will return ok but not affect

// The upsert behavior as insert when the vertexId/edgeId not exist (ignore the WHEN check)
// the other behavior same as update


TEST_P(UpdateUpsertTest, UpdateUpsertVertex) {
    // INSERT VERTEX course(name, credits), building(name) VALUES 101:("Math", 3, "No5");
    {   // OnlySet
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                    + "SET course.credits = $^.course.credits + 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX uuid(\"Math\") "
                    + "SET course.credits = $^.course.credits + 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Filter out
        // https://github.com/vesoft-inc/nebula/issues/1888
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                    + "SET course.credits = $^.course.credits + 1 "
                    + "WHEN $^.course.name == \"English\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                    + "SET course.credits = $^.course.credits + 1 "
                    + "WHEN $^.course.name == \"Math\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX uuid(\"Math\") "
                    + "SET course.credits = $^.course.credits + 1 "
                    + "WHEN $^.course.name == \"Math\" && $^.course.credits > 2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                + "SET course.credits = $^.course.credits + 1 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 6},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX uuid(\"Math\") "
                + "SET course.credits = $^.course.credits + 1 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 6},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                + "SET course.credits = $^.course.credits + 1 "
                + "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 7},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX uuid(\"Math\") "
                + "SET course.credits = $^.course.credits + 1 "
                + "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 7},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Filter out with Yield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                + "SET course.credits = $^.course.credits + 1 "
                + "WHEN $^.course.name == \"notexist\" && $^.course.credits > 2 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 7},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Filter out with Yield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX uuid(\"Math\") "
                + "SET course.credits = $^.course.credits + 1 "
                + "WHEN $^.course.name == \"notexist\" && $^.course.credits > 2 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"Math", 7},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(UpdateUpsertTest, UpdateUpsertEdge) {
    // OnlySet, SetFilter, SetYield, SetFilterYield, Insertable
    {   // OnlySet
        {
            cpp2::ExecutionResponse resp;
            auto query = "FETCH PROP ON select 200->101@0 "
                        "YIELD select.grade, select.year";
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
                {200, 101, 0, 5, 2018},
            };
            ASSERT_TRUE(verifyResult(resp, expected));
        }

        {
            cpp2::ExecutionResponse resp;
            auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                        + "SET grade = select.grade + 1, year = 2000";
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
        {
            cpp2::ExecutionResponse resp;
            auto query = "FETCH PROP ON select 200->101@0 "
                         "YIELD select.grade, select.year";
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
                {200, 101, 0, 6, 2000},
            };
            ASSERT_TRUE(verifyResult(resp, expected));
        }
        {
            cpp2::ExecutionResponse resp;
            auto query = "GO FROM 101 OVER select REVERSELY "
                         "YIELD select.grade, select.year";
            auto code = client_->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
            std::vector<std::tuple<int64_t, int64_t>> expected = {
                {6, 2000},
            };
            ASSERT_TRUE(verifyResult(resp, expected));
        }
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                    + "SET grade = select.grade + 1, year = 2000";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // Filter out
        // https://github.com/vesoft-inc/nebula/issues/1888
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET grade = select.grade + 1, year = 2000 "
                    + "WHEN select.grade > 1024 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetFilter
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET grade = select.grade + 1, year = 2000 "
                    + "WHEN select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                    + "SET grade = select.grade + 1, year = 2000 "
                    + "WHEN select.grade > 4 && $^.student.age > 15";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // SetYield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET grade = select.grade + 1, year = 2018 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 8, 2018},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                    + "SET grade = select.grade + 1, year = 2018 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 8, 2018},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // SetFilterYield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET grade = select.grade + 1, year = 2019 "
                    + "WHEN select.grade > 4 && $^.student.age > 15 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                    + "SET grade = select.grade + 1, year = 2019 "
                    + "WHEN select.grade > 4 && $^.student.age > 15 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Filter out with Yield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET grade = select.grade + 1, year = 2019 "
                    + "WHEN select.grade > 233333333333 && $^.student.age > 15 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Filter out with Yield
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE uuid(\"Monica\") -> uuid(\"Math\")@0 OF select "
                    + "SET grade = select.grade + 1, year = 2019 "
                    + "WHEN select.grade > 233333333333 && $^.student.age > 15 "
                    + "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, int64_t>> expected = {
            {"Monica", 9, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


TEST_P(UpdateUpsertTest, InvalidTest) {
    {   // update vertex: the item is TagName.PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                    + "SET credits = $^.course.credits + 1, name = \"No9\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {   // update edge: the item is PropName = Expression in SET clause
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET select.grade = select.grade + 1, select.year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // the $$.TagName.PropName expressions are not allowed in any update sentence
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                + "SET course.credits = $$.course.credits + 1 "
                + "WHEN $$.course.name == \"Math\" && $^.course.credits > $$.course.credits + 1 "
                + "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $$.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // make sure TagName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " VERTEX 101 "
                    + "SET nonexistentTag.credits = $^.course.credits + 1, building.name = \"No9\" "
                    + "WHEN $^.course.name == \"Math\" && $^.course.nonexistentProperty > 2 "
                    + "YIELD $^.course.name AS Name, $^.nonexistentTag.nonexistentProperty";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure EdgeName and PropertyName must exist in all clauses
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF select "
                    + "SET nonexistentProperty = select.grade + 1, year = 2019 "
                    + "WHEN nonexistentEdgeName.grade > 4 && $^.student.nonexistentProperty > 15 "
                    + "YIELD $^.nonexistentTag.name AS Name, select.nonexistentProperty AS Grade";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge_type must not exist
        cpp2::ExecutionResponse resp;
        auto query = GetParam() + " EDGE 200 -> 101@0 OF nonexistentEdgeTypeName "
                    + "SET grade = select.grade + 1, year = 2019";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
}

INSTANTIATE_TEST_CASE_P(updateUpsert, UpdateUpsertTest, ::testing::Values("UPDATE", "UPSERT"));

// Above cases keep same with UPDATE for the UPSERT behavior as UPDATE when vertex/edge exists


TEST_F(UpsertTest, VertexNotExists) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON course 103 YIELD course.name, course.credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, int64_t>> expected = {
            {103, "CS", 5},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // note: not allow to handle multi tagid when update
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 103 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No10\" "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // Insertable: vertex 103 ("CS", 5) --> ("CS", 6)
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 103 "
                     "SET course.credits = $^.course.credits + 1 "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t>> expected = {
            {"CS", 6},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // when tag on vertex not exists, update failed
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 104 "
                     "SET course.credits = $^.course.credits + 1 "
                     "WHEN $^.course.name == \"CS\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // has default value test
    {   // Insertable: vertex 110 ("Ann") --> ("Ann", "one"),
        // 110 is nonexistent, gender with default value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 110 "
                     "SET student_default.name = \"Ann\", student_default.age = 10 "
                     "YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected = {
                {"Ann", "one"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Insertable success, 111 is nonexistent, name and age without default value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 111 "
                     "SET student_default.name = \"Tom\", "
                     "student_default.age = $^.student_default.age + 8 "
                     "YIELD $^.student_default.name AS Name, $^.student_default.age AS Age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // Insertable error, 111 is nonexistent, name without default value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 111 "
                     "SET student_default.gender = \"two\", student_default.age = 10 "
                     "YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // Insertable: vertex 112 ("Lily") --> ("Lily", "one"),
        // 112 is nonexistent, gender with default value,
        // update student_default.age with string value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 112 "
                     "SET student_default.name = \"Lily\", student_default.age = \"10\" "
                     "YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // Insertable: vertex 113 ("Jack") --> ("Jack", "Three"),
        // 113 is nonexistent, gender with default value,
        // update student_default.age with string value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 112 "
                     "SET student_default.name = \"Ann\", student_default.age = \"10\" "
                     "YIELD $^.student_default.name AS Name, $^.student_default.gender AS Gender";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {   // Insertable success, 115 is nonexistent, name and age without default value,
        // the filter is always true.
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 115 "
                     "SET student_default.name = \"Kate\", "
                     "student_default.age = 12 "
                     "WHEN $^.student_default.gender == \"two\""
                     "YIELD $^.student_default.name AS Name, $^.student_default.age AS Age, "
                     "$^.student_default.gender AS gender";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string>> expected = {
                {"Kate", 12, "one"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Order problem
    {   // Insertable success, 116 is nonexistent, name and age without default value,
        // the filter is always true.
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 116 "
                     "SET student_default.name = \"Kate\", "
                     "student_default.age = $^.student_default.birthday + 1, "
                     "student_default.birthday = $^.student_default.birthday + 1 "
                     "YIELD $^.student_default.name AS Name, $^.student_default.age AS Age, "
                     "$^.student_default.gender AS gender, $^.student_default.birthday AS birthday";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t>> expected = {
                {"Kate", 2011, "one", 2011},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Order problem
    {   // Insertable success, 117 is nonexistent, name and age without default value,
        // the filter is always true.
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 117 "
                     "SET student_default.birthday = $^.student_default.birthday + 1, "
                     "student_default.name = \"Kate\", "
                     "student_default.age = $^.student_default.birthday + 1 "
                     "YIELD $^.student_default.name AS Name, $^.student_default.age AS Age, "
                     "$^.student_default.gender AS gender, $^.student_default.birthday AS birthday";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int64_t, std::string, int64_t>> expected = {
                {"Kate", 2012, "one", 2011},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(UpsertTest, EdgeNotExists) {
    // handle edges
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP ON select 200->101@0 "
                     "YIELD select.grade, select.year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {200, 101, 0, 5, 2018},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {   // Insertable, upsert when edge exists
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
    {   // update when edge not exists, failed
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 601 -> 101@0 OF select "
                     "SET year = 2019 "
                     "WHEN select.grade >10 "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {   // upsert when edge not exists,success
        // filter condition is always true, insert default value or update value
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 601 -> 101@0 OF select "
                     "SET year = 2019, grade = 3 "
                     "WHEN select.grade >10 "
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
        auto query = "FETCH PROP ON select 601->101@0 "
                     "YIELD select.grade, select.year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t, int64_t>> expected = {
            {601, 101, 0, 3, 2019},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // select_default's year with default value
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 111 -> 222@0 OF select_default "
                     "SET grade = 3 "
                     "YIELD select_default.grade AS Grade, select_default.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector <std::tuple<int64_t, int64_t>> expected = {
            {3, 1546308000},
        };
        if (isShangHaiTimezone()) {
            ASSERT_TRUE(verifyResult(resp, expected));
        }
    }
    // select_default's year is timestamp type, set str type
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 222 -> 333@0 OF select_default "
                     "SET grade = 3, year = \"2020-01-10 10:00:00\" "
                     "YIELD select_default.grade AS Grade, select_default.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector <std::tuple<int64_t, int64_t>> expected = {
            {3, 1578621600},
        };
        if (isShangHaiTimezone()) {
            ASSERT_TRUE(verifyResult(resp, expected));
        }
    }
    // select_default's grade without default value
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 444 -> 555@0 OF select "
                     "SET year = 1546279201 "
                     "YIELD select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // update select_default's grade with string value
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 222 -> 333@0 OF select_default "
                     "SET grade = \"3\" "
                     "YIELD select_default.grade AS Grade, select_default.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // update select_default's grade with edge prop value
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 333 -> 444@0 OF select_default "
                     "SET grade = 3 + select_default.grade "
                     "YIELD select_default.grade AS Grade, select_default.year AS Year";
        auto code = client_->execute(query, resp);
        LOG(INFO) << "code is " << static_cast<int32_t>(code);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // update select_default's year with edge prop value
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 222 -> 444@0 OF select_default "
                     "SET grade = 3, year = select_default.year + 10 "
                     "YIELD select_default.grade AS Grade, select_default.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector <std::tuple<int64_t, int64_t>> expected = {
            {3, 1546308010},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(UpdateTest, NotExists) {
    {   // make sure the vertex must not exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE VERTEX 101000000000000 "
                     "SET course.credits = $^.course.credits + 1, building.name = \"No9\" "
                     "WHEN $^.course.name == \"Math\" && $^.course.credits > 2 "
                     "YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, dst) must not exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101000000000000@0 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
    {   // make sure the edge(src, ranking, dst) must not exist
        cpp2::ExecutionResponse resp;
        auto query = "UPDATE EDGE 200 -> 101@123456789 OF select "
                     "SET grade = select.grade + 1, year = 2019 "
                     "WHEN select.grade > 4 && $^.student.age > 15 "
                     "YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code) << resp.get_error_msg();
    }
}

TEST_F(UpdateTest, UpsertThenInsert) {
    FLAGS_enable_multi_versions = true;
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 100 SET building.name = \"No1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string>> expected = {
            {100, "No1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX building(name) VALUES 100: (\"No2\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string>> expected = {
            {100, "No2"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    FLAGS_enable_multi_versions = false;
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 101 SET building.name = \"No1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 101";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string>> expected = {
            {101, "No1"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX building(name) VALUES 101: (\"No2\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 101";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string>> expected = {
            {101, "No2"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(UpdateTest, UpsertAfterAlterSchema) {
    // Test upsert tag after alter schema
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX building(name) VALUES 100: (\"No1\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "ALTER TAG building ADD (new_field string default \"123\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 100 SET building.name = \"No2\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {100, "No2", "123"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 100 SET building.name = \"No3\", building.new_field = \"321\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {100, "No3", "321"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT VERTEX 101 SET building.name = \"No1\", building.new_field = \"No2\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on building 101";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, std::string, std::string>> expected = {
            {101, "No1", "No2"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test upsert edge after alter schema
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE like(likeness) VALUES 1 -> 100:(1.0)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "ALTER EDGE like ADD (new_field string default \"123\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 1->100 of like SET likeness = 2.0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on like 1->100@0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, double, std::string>> expected = {
            {1, 100, 0, 2.0, "123"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 1->100 of like SET likeness = 3.0, new_field = \"321\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on like 1->100@0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, double, std::string>> expected = {
            {1, 100, 0, 3.0, "321"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "UPSERT EDGE 1->101 of like SET likeness = 1.0, new_field = \"111\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "FETCH PROP on like 1->101@0";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int64_t, int64_t, int64_t, double, std::string>> expected = {
            {1, 101, 0, 1.0, "111"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

// Above cases behavior different with UPDATE for UPSERT insert data when not exists

}   // namespace graph
}   // namespace nebula
