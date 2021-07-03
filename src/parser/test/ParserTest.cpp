/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "parser/GQLParser.h"
#include "util/AstUtils.h"

namespace nebula {

using graph::QueryContext;
class ParserTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<graph::QueryContext>();
    }
    void TearDown() override {
        qctx_.reset();
    }

protected:
    StatusOr<std::unique_ptr<Sentence>> parse(const std::string& query) {
        auto* qctx = qctx_.get();
        GQLParser parser(qctx);
        auto result = parser.parse(query);
        NG_RETURN_IF_ERROR(result);
        NG_RETURN_IF_ERROR(graph::AstUtils::reprAstCheck(*result.value(), qctx));
        return result;
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(ParserTest, TestSchemaCreation) {
    // All type
    {
        std::string query = "CREATE TAG person(name STRING, age INT8, count INT16, "
                            "friends INT32, books INT64, citys INT, property DOUBLE, "
                            "liabilities FLOAT, profession FIXED_STRING(20), "
                            "start TIMESTAMP, study DATE, birthday DATETIME)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }

    {
        std::string query = "CREATE TAG person(name STRING NULL, age INT8 NOT NULL, "
                            "count INT16 DEFAULT 10, friends INT32 NULL DEFAULT 10, "
                            "profession FIXED_STRING(20) NOT NULL DEFAULT \"student\");";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Error FIXED_STRING
    {
        std::string query = "CREATE TAG person(profession FIXED_STRING)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG person(profession FIXED_STRING(400000))";
        auto result = parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    // Default value is NULL
    {
        std::string query = "CREATE TAG person(profession string DEFAULT NULL)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Default value AND NOT NULL
    {
        std::string query = "CREATE TAG person(profession string DEFAULT \"HELLO\" NOT NULL)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG person(profession string NOT NULL DEFAULT \"HELLO\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Default value AND NULL
    {
        std::string query = "CREATE TAG person(profession string DEFAULT \"HELLO\" NULL)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG person(profession string NULL DEFAULT \"HELLO\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Go) {
    {
        std::string query = "GO FROM \"1\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend;";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO 2 STEPS FROM \"1\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend REVERSELY";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend YIELD person.name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $^.manager.name,$^.manager.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $$.manager.name,$$.manager.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $^[manager].name,$^[manager].age";
        auto result = parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM $-.id OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM $-.col1 OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM $-.id OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER friend WHERE person.name == \"Tom\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, SpaceOperation) {
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "USE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3) "
                            "ON group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space ON group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "charset=utf8, collate=utf8_bin)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "charset=utf8)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "collate=utf8_bin)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "atomic_edge=true)";
        auto result = parse(query);
        EXPECT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "atomic_edge=FALSE)";
        auto result = parse(query);
        EXPECT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "USE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, TagOperation) {
    {
        std::string query = "CREATE TAG person(name string, age int, "
                            "married bool, salary double)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test empty prop
    {
        std::string query = "CREATE TAG person()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG man(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG woman(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100, ttl_col = \"create_time\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG woman(name string, age int default 22, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG woman(name string default \"\", age int default 22, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER TAG man ttl_duration = 200";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER TAG man ttl_col = \"\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER TAG woman ttl_duration = 50, ttl_col = \"age\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER TAG woman ADD (col6 int) ttl_duration = 200";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE TAG person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC TAG person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE TAG person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP TAG person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, EdgeOperation) {
    {
        std::string query = "CREATE EDGE e1(name string, age int, "
                            "married bool, salary double)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test empty prop
    {
        std::string query = "CREATE EDGE e1()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE EDGE man(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE EDGE man(name string default \"\", age int default 18, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE EDGE woman(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100, ttl_col = \"create_time\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER EDGE e1 ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER EDGE man ttl_duration = 200";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER EDGE man ttl_col = \"\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER EDGE woman ttl_duration = 50, ttl_col = \"age\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ALTER EDGE woman ADD (col6 int)  ttl_duration = 200";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE EDGE e1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC EDGE e1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE EDGE e1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP EDGE e1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}
// Column space format test, expected SyntaxError
TEST_F(ParserTest, ColumnSpacesTest) {
    {
        std::string query = "CREATE TAG person(name, age, married bool)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CREATE TAG person(name, age, married)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CREATE TAG man(name string, age)"
                            "ttl_duration = 100";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age int)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER TAG person ADD (col1, col2), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married, salary), "
                            "DROP (age, create_time)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CREATE EDGE man(name, age, married bool) "
                            "ttl_duration = 100";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER EDGE woman ADD (col6)  ttl_duration = 200";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER EDGE woman CHANGE (col6)  ttl_duration = 200";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "ALTER EDGE woman DROP (col6 int)  ttl_duration = 200";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, IndexOperation) {
    {
        std::string query = "CREATE TAG INDEX empty_field_index ON person()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX name_index ON person(name)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX IF NOT EXISTS name_index ON person(name)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX IF NOT EXISTS name_index ON person(name, age)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS empty_field_index ON service()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS like_index ON service(like)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS like_index ON service(like, score)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1(10), c2(20))";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1, c2(20))";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1(10), c2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1(10), c2(20))";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1, c2(20))";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1(10), c2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DROP TAG INDEX name_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DROP EDGE INDEX like_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DESCRIBE TAG INDEX name_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DESCRIBE EDGE INDEX like_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "REBUILD TAG INDEX name_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "REBUILD EDGE INDEX like_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
}

TEST_F(ParserTest, Set) {
    {
        std::string query = "GO FROM \"1\" OVER friend INTERSECT "
                            "GO FROM \"2\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend UNION "
                            "GO FROM \"2\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend UNION "
                            "GO FROM \"2\" OVER friend INTERSECT "
                            "GO FROM \"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "(GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend) UNION "
                            "GO FROM \"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        // pipe have priority over set
        std::string query = "GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend UNION "
                            "GO FROM \"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "(GO FROM \"1\" OVER friend YIELD friend._dst AS id UNION "
                            "GO FROM \"2\" OVER friend YIELD friend._dst AS id) | "
                            "GO FROM $-.id OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Pipe) {
    {
        std::string query = "GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend | "
                            "GO FROM \"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend | "
                            "GO FROM \"3\" OVER friend";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, InsertVertex) {
    {
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test one vertex multi tags
    {
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111, "
                            "\"zhangsan\", 20190527, 1111)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test multi vertex multi tags
    {
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111, "
                            "\"zhangsan\", 20190527, 1111),"
                            "\"12346\":(\"lisi\", 20, 1112, \"lisi\", 20190413, 1112)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111), "
                            "( \"zhangsan\", 20190527),"
                            "\"12346\":(\"lisi\", 20, 1112), (\"lisi\", 20190413)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        std::string query = "INSERT VERTEX person() "
                            "VALUES \"12345\":()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert prop unterminated ""
    {
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\"Tom, 30)";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    // Test insert prop unterminated ''
    {
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
}

TEST_F(ParserTest, UpdateVertex) {
    {
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tome\", person.age=30, "
                            "job.salary=10000, person.create_time=1551331999";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tome\", age=30";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=$^.person.age + 1, "
                            "person.married=true "
                            "WHEN $^.job.salary > 10000 AND $^.person.age > 30";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET name=\"Tom\", age=age + 1, "
                            "married=true "
                            "WHEN salary > 10000 AND age > 30";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=31, person.married=true, "
                            "job.salary=1.1 * $^.person.create_time / 31536000 "
                            "YIELD $^.person.name AS Name, job.name AS Title, "
                            "$^.job.salary AS Salary";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age=31, married=true "
                            "YIELD name AS Name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=30, person.married=true "
                            "WHEN $^.job.salary > 10000 AND $^.job.name == \"CTO\" OR "
                            "$^.person.age < 30"
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                            "$^.person.create_time AS Time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age=30, married=true "
                            "WHEN salary > 10000 AND name == \"CTO\" OR age < 30"
                            "YIELD name AS Name, salary AS Salary, create_time AS Time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPSERT VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age = 30, job.name =\"CTO\" "
                            "WHEN $^.job.salary > 10000 "
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                            "$^.person.create_time AS Time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPSERT VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age = 30, name =\"CTO\" "
                            "WHEN salary > 10000 "
                            "YIELD name AS Name, salary AS Salary, create_time AS Time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, InsertEdge) {
    {
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321\":(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"from\"->\"to\":(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"from\"->\"to\":(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // multi edge
    {
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321@1537408527\":(3.75, 1537408527),"
                            "\"56789\"->\"98765@1537408527\":(3.5, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        std::string query = "INSERT EDGE transfer() "
                            "VALUES \"12345\"->\"54321@1537408527\":()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT EDGE IF NOT EXISTS transfer(amount, time_) "
                            "VALUES \"-12345\"->\"54321\":(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321\"@1537408527:(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT EDGE IF NOT EXISTS transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321@1537408527\":(3.75, 1537408527)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        std::string query = "INSERT EDGE IF NOT EXISTS transfer() "
                            "VALUES \"12345\"->\"54321@1537408527\":()";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, UpdateEdge) {
    {
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\" OF transfer "
                            "SET amount=3.14, time_=1537408527";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\" "
                            "SET amount=3.14, time_=1537408527";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\"@789 OF transfer "
                            "SET amount=3.14,time_=1537408527 "
                            "WHEN transfer.amount > 3.14 AND $^.person.name == \"Tom\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\"@789 "
                            "SET amount=3.14,time_=1537408527 "
                            "WHEN amount > 3.14";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\" OF transfer "
                            "SET amount = 3.14 + $^.job.salary, time_ = 1537408527 "
                            "WHEN transfer.amount > 3.14 OR $^.job.salary >= 10000 "
                            "YIELD transfer.amount, transfer.time_ AS Time_, "
                            "$^.person.name AS PayFrom";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\" "
                            "SET amount = 3.14 + amount, time_ = 1537408527 "
                            "WHEN amount > 3.14 "
                            "YIELD amount, time_ AS Time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPSERT EDGE \"12345\" -> \"54321\" @789 OF transfer "
                            "SET amount=$^.job.salary + 3.14, time_=1537408527 "
                            "WHEN transfer.amount > 3.14 AND $^.job.salary >= 10000 "
                            "YIELD transfer.amount,transfer.time_, $^.person.name AS Name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPSERT EDGE ON transfer \"12345\" -> \"54321\" @789 "
                            "SET amount=$^.job.salary + 3.14, time_=1537408527 "
                            "WHEN amount > 3.14 AND salary >= 10000 "
                            "YIELD amount, time_, name AS Name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, DeleteVertex) {
    {
        std::string query = "DELETE VERTEX \"12345\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE VERTEX \"12345\",\"23456\",\"34567\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE VERTEX \"Tom\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE VERTEX \"Tom\", \"Jerry\", \"Mickey\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE VERTEX \"Tom\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE VERTEX \"Tom\", \"Jerry\", \"Mickey\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"Ann\" OVER schoolmate YIELD $$.person.name as name"
                            "| DELETE VERTEX $-.id";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, DeleteEdge) {
    {
        std::string query = "DELETE EDGE transfer \"12345\" -> \"5432\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE EDGE transfer \"123\" -> \"321\", "
                            "\"456\" -> \"654\"@11, \"789\" -> \"987\"@12";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DELETE EDGE transfer \"jack\" -> \"rose\","
                            "\"mr\" -> \"miss\"@13";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"Ann\" OVER schoolmate "
                            "YIELD schoolmate._src as src, schoolmate._dst as dst, "
                            "schoolmate._rank as rank"
                            "| DELETE EDGE transfer $-.src -> $-.dst @ $-.rank";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, FetchVertex) {
    {
        std::string query = "FETCH PROP ON person \"1\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"1\", \"2\", \"3\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\" "
                            "YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\" "
                            "YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" over edu YIELD edu._dst AS id | "
                            "FETCH PROP ON person $-.id YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "$var = GO FROM \"1\" over e1; "
                            "FETCH PROP ON person $var.id YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"1\", \"2\", \"3\" "
                            "YIELD DISTINCT person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person uuid(\"Tom\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\" "
                            "YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\" "
                            "YIELD person.name, person.age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON * \"1\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON * \"1\", \"2\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON * $-.id";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "yield \"1\" as id | FETCH PROP ON * $-.id";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "yield \"1\" as id | FETCH PROP ON * $-.id yield friend.id, person.id";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, FetchEdge) {
    {
        std::string query = "FETCH PROP ON transfer \"12345\" -> \"54321\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FETCH PROP ON transfer \"12345\" -> \"54321\" "
                            "YIELD transfer.time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"12345\" OVER transfer "
                            "YIELD transfer.time_";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"12345\" OVER transfer "
                            "YIELD transfer._src AS s, serve._dst AS d | "
                            "FETCH PROP ON transfer $-.s -> $-.d YIELD transfer.amount";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "$var = GO FROM \"12345\" OVER transfer "
                            "YIELD transfer._src AS s, edu._dst AS d; "
                            "FETCH PROP ON service $var.s -> $var.d YIELD service.amount";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // this will be denied in Semantic Analysis
    {
        std::string query = "FETCH PROP ON transfer, another \"12345\" -> \"-54321\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Lookup) {
    {
        std::string query = "LOOKUP ON person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON salary, age";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON person WHERE person.gender == \"man\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON transfer WHERE transfer.amount > 1000 YIELD transfer.amount";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON transfer WHERE transfer.amount > 1000 YIELD transfer.amount,"
                            " transfer.test";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, subgraph) {
    {
        std::string query = "GET SUBGRAPH FROM \"TOM\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH FROM \"TOM\" BOTH like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH FROM \"TOM\" IN like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH FROM \"TOM\" OUT like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH FROM \"TOM\" IN like OUT serve";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH 1 STEPS FROM \"TOM\" IN like OUT serve";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH 2 STEPS FROM \"TOM\" BOTH like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH 2 STEPS FROM \"TOM\" IN like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH 4 STEPS FROM \"TOM\" IN like OUT serve BOTH owner";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET SUBGRAPH 4 STEPS FROM \"TOM\" IN like OUT serve";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, AdminOperation) {
    {
        std::string query = "SHOW HOSTS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW HOSTS graph";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW HOSTS meta";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW HOSTS storage";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW SPACES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW PARTS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW PARTS 666";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW TAGS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW EDGES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW TAG INDEXES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW TAG INDEXES BY tag1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW EDGE INDEXES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW EDGE INDEXES BY edge1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE SPACE default_space";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE TAG person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE EDGE like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE TAG INDEX person_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CREATE EDGE INDEX like_index";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW TAG INDEX STATUS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW EDGE INDEX STATUS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CHARSET";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW COLLATION";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW STATS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, UserOperation) {
    {
        std::string query = "CREATE USER user1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE USER user1 WITH PASSWORD aaa";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CREATE USER user1 WITH PASSWORD \"aaa\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"aaa\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "ALTER USER user1 WITH PASSWORD \"a\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "ALTER USER user1 WITH PASSWORD \"a\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DROP USER user1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "DROP USER IF EXISTS user1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "CHANGE PASSWORD \"new password\"";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CHANGE PASSWORD account TO \"new password\"";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "CHANGE PASSWORD account FROM \"old password\" TO \"new password\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "GRANT ROLE ADMIN ON spacename TO account";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "GRANT ROLE DBA ON spacename TO account";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "GRANT ROLE SYSTEM ON spacename TO account";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "REVOKE ROLE ADMIN ON spacename FROM account";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "SHOW ROLES IN spacename";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        std::string query = "SHOW USERS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
}

TEST_F(ParserTest, UnreservedKeywords) {
    {
        std::string query = "CREATE TAG tag1(space string, spaces string, "
                            "email string, password string, roles string, uuid int, "
                            "path string, variables string, leader string, data string)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE EDGE edge1(space string, spaces string, "
                            "email string, password string, roles string)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"123\" OVER guest WHERE $-.EMAIL";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM UUID(\"tom\") OVER guest WHERE $-.EMAIL";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"123\" OVER like YIELD $$.tag1.EMAIL, like.users,"
                            "like._src, like._dst, like.type, $^.tag2.SPACE "
                            "| ORDER BY $-.SPACE";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM UUID(\"tom\") OVER like YIELD $$.tag1.EMAIL, like.users,"
                            "like._src, like._dst, like.type, $^.tag2.SPACE "
                            "| ORDER BY $-.SPACE";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "$var = GO FROM \"123\" OVER like;GO FROM $var.SPACE OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "$var = GO FROM UUID(\"tom\") OVER like;GO FROM $var.SPACE OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG status(part int, parts int, job string, jobs string,"
                            " rebuild bool, submit bool, compact bool, "
                            " bidirect bool, force bool, configs string)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, Annotation) {
    {
        std::string query = "show spaces /* test comment....";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        std::string query = "// test comment....";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        std::string query = "# test comment....";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        std::string query = "/* test comment....*/";
        auto result = parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        std::string query = "CREATE TAG TAG1(space string) // test....";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG TAG1(space string) # test....";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG TAG1/* tag name */(space string)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE TAG TAG1/* tag name */(space string) // test....";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, DownloadAndIngest) {
    {
        std::string query = "DOWNLOAD HDFS \"hdfs://127.0.0.1:9090/data\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INGEST";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Agg) {
    {
        std::string query = "ORDER BY $-.id";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name ASC";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name DESC";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, friend.age as age | "
                            "ORDER BY $-.name ASC, $-.age DESC";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, friend.age as age | "
                            "ORDER BY name ASC, age DESC";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, ReentrantRecoveryFromFailure) {
    {
        std::string query = "USE dumy tag_name";
        ASSERT_FALSE(parse(query).ok());
    }
    {
        std::string query = "USE space_name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, IllegalCharacter) {
    {
        std::string query = "USE space；";
        ASSERT_FALSE(parse(query).ok());
    }
    {
        std::string query = "USE space_name；USE space";
        ASSERT_FALSE(parse(query).ok());
    }
}

TEST_F(ParserTest, Distinct) {
    {
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD DISTINCT friend.name as name, friend.age as age";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        // syntax error
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, DISTINCT friend.age as age";
        auto result = parse(query);
        ASSERT_TRUE(!result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "| GO FROM $-.id OVER like | GO FROM $-.id OVER serve "
                            "YIELD DISTINCT serve._dst, $$.team.name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, ConfigOperation) {
    {
        std::string query = "SHOW CONFIGS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW CONFIGS GRAPH";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS storage:name=\"value\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET CONFIGS Meta:name";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS load_data_interval_secs=120";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GET CONFIGS load_data_interval_secs";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = "
                            "{ stats_dump_period_sec = 200 }";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS rocksdb_db_options={disable_auto_compaction=false}";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = "
                            "{stats_dump_period_sec = 200, disable_auto_compaction = false}";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS rocksdb_column_family_options={"
                            "write_buffer_size = 1 * 1024 * 1024}";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = {}";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, BalanceOperation) {
    {
        std::string query = "BALANCE LEADER";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA 1234567890";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA STOP";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA REMOVE 192.168.0.1:50000,192.168.0.1:50001";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA REMOVE 192.168.0.1:50000,\"localhost\":50001";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "BALANCE DATA RESET PLAN";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, CrashByFuzzer) {
    {
        std::string query = ";YIELD\nI41( ,1)GEGE.INGEST";
        auto result = parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, FindPath) {
    {
        std::string query = "FIND SHORTEST PATH FROM \"1\" TO \"2\" OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FIND ALL PATH FROM \"1\" TO \"2\" OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FIND SHORTEST PATH WITH PROP FROM \"1\" TO \"2\" OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "FIND ALL PATH WITH PROP FROM \"1\" TO \"2\" OVER like";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Limit) {
    {
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1,2";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1 OFFSET 2";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // ERROR
    {
        std::string query = "GO FROM \"1\" OVER work | LIMIT \"1\"";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, GroupBy) {
    // All fun succeed
    {
        std::string query = "GO FROM \"1\" OVER work "
                            "YIELD $$.company.name, $^.person.name AS name "
                            "| GROUP BY $$.company.name , abs($$.company.age+1)"
                            "YIELD $$.company.name AS name, "
                            "COUNT($^.person.name ), "
                            "SUM($^.person.name ), "
                            "AVG($^.person.name ), "
                            "(INT)abs($$.company.age+1), "
                            "(INT)COUNT(DISTINCT $^.person.name ), "
                            "COUNT(DISTINCT $-.name )+1, "
                            "abs(SUM(DISTINCT $$.person.name )), "
                            "AVG(DISTINCT $^.person.name ), "
                            "AVG(DISTINCT $-.name ), "
                            "COUNT(*), "
                            "COUNT(DISTINCT *), "
                            "MAX($^.person.name ), "
                            "MIN($$.person.name ), "
                            "STD($^.person.name ), "
                            "BIT_AND($^.person.name ), "
                            "BIT_OR($$.person.name ), "
                            "BIT_XOR($^.person.name ),"
                            "STD(DISTINCT $^.person.name ), "
                            "BIT_AND(DISTINCT $$.person.name ), "
                            "BIT_OR(DISTINCT $^.person.name ), "
                            "BIT_XOR(DISTINCT $$.person.name ),"
                            "BIT_XOR(DISTINCT $-.name ),"
                            "STD($^.person.name ), "
                            "BIT_AND($^.person.name ), "
                            "BIT_OR($^.person.name ), "
                            "BIT_XOR($^.person.name )";

        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER work "
                            "YIELD $$.company.name, $^.person.name AS name "
                            "| YIELD $$.company.name AS name, "
                            " abs($$.company.age+1), "
                            "COUNT($^.person.name ), "
                            "SUM($^.person.name ), "
                            "AVG($^.person.name ), "
                            "(INT)abs($$.company.age+1), "
                            "(INT)COUNT(DISTINCT $^.person.name ), "
                            "COUNT(DISTINCT $-.name )+1, "
                            "abs(SUM(DISTINCT $$.person.name )), "
                            "AVG(DISTINCT $^.person.name ), "
                            "AVG(DISTINCT $-.name ), "
                            "COUNT(*), "
                            "COUNT(DISTINCT *), "
                            "MAX($^.person.name ), "
                            "MIN($$.person.name ), "
                            "STD($^.person.name ), "
                            "BIT_AND($^.person.name ), "
                            "BIT_OR($$.person.name ), "
                            "BIT_XOR($^.person.name ),"
                            "STD(DISTINCT $^.person.name ), "
                            "BIT_AND(DISTINCT $$.person.name ), "
                            "BIT_OR(DISTINCT $^.person.name ), "
                            "BIT_XOR(DISTINCT $$.person.name ),"
                            "BIT_XOR(DISTINCT $-.name ),"
                            "STD($^.person.name ), "
                            "BIT_AND($^.person.name ), "
                            "BIT_OR($^.person.name ), "
                            "BIT_XOR($^.person.name )";

        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Error syntax
    {
        std::string query = "YIELD rand32() as id, sum(1) as sum, avg(2) as avg GROUP BY id";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    // All fun error, empty group name
    {
        std::string query = "GO FROM \"1\" OVER work "
                            "YIELD $$.company.name, $^.person.name "
                            "| GROUP BY "
                            "YIELD $$.company.name as name, "
                            "COUNT($^.person.name )";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, ErrorMsg) {
    {
        std::string query = "CREATE SPACE " + std::string(4097, 'A');
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range of the LABEL length, "
                     "the  max length of LABEL is 4096: near `" + std::string(80, 'A') + "'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775809) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775809'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min integer bound checking
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-9223372036854775808) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-(1+9223372036854775808)) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-9223372036854775809) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775809'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max integer bound checking
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775807) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+9223372036854775807) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775808) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775808'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+9223372036854775808) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775808'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0xFFFFFFFFFFFFFFFFF) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0xFFFFFFFFFFFFFFFFF'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min hex integer bound
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-0x8000000000000000) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-0x8000000000000001) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000001'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max hex integer bound
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0x7FFFFFFFFFFFFFFF) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0x7FFFFFFFFFFFFFFF) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0x8000000000000000) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0x8000000000000000) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(002777777777777777777777) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `002777777777777777777777'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min oct integer bound
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-01000000000000000000000)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-01000000000000000000001) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000001'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max oct integer bound
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0777777777777777777777) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0777777777777777777777) ";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(01000000000000000000000) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+01000000000000000000000) ";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
}

TEST_F(ParserTest, UseReservedKeyword) {
    {
        std::string query = "CREATE TAG tag()";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());

        query = "CREATE TAG `tag`()";
        result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "CREATE EDGE edge()";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());

        query = "CREATE EDGE `edge`()";
        result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "CREATE TAG `person`(`tag` string)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "CREATE TAG `time`(`time` string)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
}

TEST_F(ParserTest, TypeCast) {
    {
        std::string query = "YIELD (INT)\"123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT)  \"123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT)\".123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT8)\".123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT16)\".123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT32)\".123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT64)\"1.23\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT65)\"1.23\"";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "YIELD (DOUBLE)\"123abc\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (INT)\"abc123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (FLOAT)\"123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (FLOAT)\"1.23\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (FLOAT)\".123\"";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (STRING)123";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (STRING)1.23";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        std::string query = "YIELD (MAP)123";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "YIELD (INT)true";
        auto result = parse(query);
        ASSERT_TRUE(result.ok());
    }
}

TEST_F(ParserTest, Match) {
    {
        std::string query = "MATCH () -- () RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH () <-- () RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH () --> () RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH () <--> () RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) --> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a:person) --> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (:person {name: 'Tom'}) --> (:person {name: 'Jerry'}) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[]- (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[]-> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) <-[]- (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) <-[m:like]-> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) <-[m:like{likeness: 99}]-> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) WHERE a.name == 'Tom' RETURN a";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like|serve]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like|:serve]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*2]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*2..]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*..2]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*2..4]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m:like*..2]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH p = (a) -[m:like*..2]- (b) RETURN p as Path";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "OPTIONAL MATCH (a) -[m]- (b) RETURN a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN DISTINCT a as Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person SKIP 1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person SKIP 1 LIMIT 2";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person SKIP 1 LIMIT 2";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person SKIP 1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person LIMIT 1";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a AS b RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH DISTINCT a as b RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b ORDER BY b RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b ORDER BY b SKIP 1 LIMIT 1 RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b SKIP 1 LIMIT 1 RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b LIMIT 1 RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b SKIP 1 RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "WITH a as b SKIP 1 WHERE b > 0 RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UNWIND a as b RETURN b";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH (a) MATCH (b) -[m]- (c) RETURN a,b,c";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UNWIND a AS b MATCH (c) -[m]- (d) RETURN b,c,d";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "UNWIND a AS b MATCH (c) MATCH (d) WITH e MATCH (f) RETURN b,c,d";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, MatchErrorCheck) {
    {
        std::string query = "MATCH (v:player) WHERE count(v)>1 RETURN v";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Invalid use of aggregating "
            "function in this context. near `WHERE count(v)>1'";
        ASSERT_EQ(error, result.status().toString());
    }
}

TEST_F(ParserTest, MatchMultipleTags) {
    {
        std::string query = "MATCH (a:person:player) --> (b) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query =
            "MATCH (:person {name: 'Tom'}:player {id: 233}) --> (:person {name: 'Jerry'}) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query =
            "MATCH (:person:player {id: 233}) --> (:person {name: 'Jerry'}) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH () --> (:person {name: 'Jerry'}:player {id: 233}) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "MATCH () --> (:person {name: 'Jerry'}:player) RETURN *";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, MatchListSubscriptRange) {
    {
        std::string query = "WITH [0, 1, 2] AS list RETURN list[0..] AS l";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "RETURN [0, 1, 2][0..] AS l";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "RETURN [0, 1, 2][0..1] AS l";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, Zone) {
    {
        std::string query = "SHOW GROUPS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW ZONES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ADD ZONE zone_0 127.0.0.1:8989,127.0.0.1:8988,127.0.0.1:8987";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ADD HOST 127.0.0.1:8989 INTO ZONE zone_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP HOST 127.0.0.1:8989 FROM ZONE zone_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC ZONE zone_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE ZONE zone_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP ZONE zone_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ADD GROUP group_0 zone_0,zone_1,zone_2";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ADD ZONE zone_3 INTO GROUP group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP ZONE zone_3 FROM GROUP group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESC GROUP group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DESCRIBE GROUP group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "DROP GROUP group_0";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST_F(ParserTest, FullText) {
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1, 2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", 1, 2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", 1, 2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 1, 2)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", AUTO, AND)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", AUTO, OR)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, AND)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1, 1)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, -1, 1)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1, -1)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", -1, OR, 1, 1)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 4, OR, 1, 1)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", -1, 2)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1, -2)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", AUTO, AND)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", AUTO, AND)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", AUTO, AND)";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(ParserTest, FullTextServiceTest) {
    {
        std::string query = "ADD LISTENER ELASTICSEARCH 127.0.0.1:12000";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "ADD LISTENER ELASTICSEARCH 127.0.0.1:12000, 127.0.0.1:12001";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "REMOVE LISTENER ELASTICSEARCH";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SHOW LISTENER";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200), (127.0.0.1:9300)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\", \"password\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\", \"password\"), "
                            "(127.0.0.1:9200, \"user\", \"password\")";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN OUT TEXT SERVICE";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\")";
        auto result = parse(query);
        ASSERT_FALSE(result.ok());
    }
}


TEST_F(ParserTest, SessionTest) {
    {
        std::string query = "SHOW SESSIONS";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "SHOW SESSIONS");
    }
    {
        std::string query = "SHOW SESSION 123";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "SHOW SESSION 123");
    }
}

TEST_F(ParserTest, JobTest) {
    auto checkTest = [&, this] (const std::string& query, const std::string expectedStr) {
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << query << ":" << result.status();
        ASSERT_EQ(result.value()->toString(), expectedStr);
    };
    checkTest("SUBMIT JOB COMPACT", "SUBMIT JOB COMPACT");
    checkTest("SUBMIT JOB COMPACT 111", "SUBMIT JOB COMPACT 111");
    checkTest("SUBMIT JOB FLUSH", "SUBMIT JOB FLUSH");
    checkTest("SUBMIT JOB FLUSH 111", "SUBMIT JOB FLUSH 111");
    checkTest("SUBMIT JOB STATS", "SUBMIT JOB STATS");
    checkTest("SUBMIT JOB STATS 111", "SUBMIT JOB STATS 111");
    checkTest("SHOW JOBS", "SHOW JOBS");
    checkTest("SHOW JOB 111", "SHOW JOB 111");
    checkTest("STOP JOB 111", "STOP JOB 111");
    checkTest("RECOVER JOB", "RECOVER JOB");
    checkTest("REBUILD TAG INDEX name_index", "REBUILD TAG INDEX name_index");
    checkTest("REBUILD EDGE INDEX name_index", "REBUILD EDGE INDEX name_index");
    checkTest("REBUILD TAG INDEX", "REBUILD TAG INDEX ");
    checkTest("REBUILD EDGE INDEX", "REBUILD EDGE INDEX ");
    checkTest("REBUILD TAG INDEX name_index, age_index",
            "REBUILD TAG INDEX name_index,age_index");
    checkTest("REBUILD EDGE INDEX name_index, age_index",
            "REBUILD EDGE INDEX name_index,age_index");
}

TEST_F(ParserTest, ShowAndKillQueryTest) {
    {
        std::string query = "SHOW QUERIES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "SHOW QUERIES");
    }
    {
        std::string query = "SHOW ALL QUERIES";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "SHOW ALL QUERIES");
    }
    {
        std::string query = "KILL QUERY (plan=123)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "KILL QUERY (plan=123)");
    }
    {
        std::string query = "KILL QUERY (session=123, plan=123)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "KILL QUERY (session=123, plan=123)");
    }
    {
        std::string query = "KILL QUERY (plan=123, session=123)";
        auto result = parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(result.value()->toString(), "KILL QUERY (session=123, plan=123)");
    }
}
}   // namespace nebula
