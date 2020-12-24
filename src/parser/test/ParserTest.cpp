/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "parser/GQLParser.h"

namespace nebula {

TEST(Parser, TestSchemaCreation) {
    // All type
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name STRING, age INT8, count INT16, "
                            "friends INT32, books INT64, citys INT, property DOUBLE, "
                            "liabilities FLOAT, profession FIXED_STRING(20), "
                            "start TIMESTAMP, study DATE, birthday DATETIME)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }

    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name STRING NULL, age INT8 NOT NULL, "
                            "count INT16 DEFAULT 10, friends INT32 NULL DEFAULT 10, "
                            "profession FIXED_STRING(20) NOT NULL DEFAULT \"student\");";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Error FIXED_STRING
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession FIXED_STRING)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession FIXED_STRING(400000))";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    // Default value is NULL
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession string DEFAULT NULL)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Default value AND NOT NULL
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession string DEFAULT \"HELLO\" NOT NULL)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession string NOT NULL DEFAULT \"HELLO\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Default value AND NULL
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession string DEFAULT \"HELLO\" NULL)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(profession string NULL DEFAULT \"HELLO\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Go) {
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend;";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO 2 STEPS FROM \"1\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend REVERSELY";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend YIELD person.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $^.manager.name,$^.manager.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $$.manager.name,$$.manager.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD $^[manager].name,$^[manager].age";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $-.id OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $-.col1 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $-.id OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER friend WHERE person.name == \"Tom\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, SpaceOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "USE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESC SPACE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE SPACE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE SPACE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP SPACE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "charset=utf8, collate=utf8_bin)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "charset=utf8)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=3,"
                            "collate=utf8_bin)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, TagOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name string, age int, "
                            "married bool, salary double)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test empty prop
    {
        GQLParser parser;
        std::string query = "CREATE TAG person()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG man(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100, ttl_col = \"create_time\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string, age int default 22, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string default \"\", age int default 22, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG man ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG man ttl_col = \"\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG woman ttl_duration = 50, ttl_col = \"age\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG woman ADD (col6 int) ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE TAG person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESC TAG person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE TAG person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP TAG person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, EdgeOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE EDGE e1(name string, age int, "
                            "married bool, salary double)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test empty prop
    {
        GQLParser parser;
        std::string query = "CREATE EDGE e1()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE man(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE man(name string default \"\", age int default 18, "
                            "married bool default false, salary double default 1000.0)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE woman(name string, age int, "
                            "married bool, salary double)"
                            "ttl_duration = 100, ttl_col = \"create_time\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE e1 ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE man ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE man ttl_col = \"\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE woman ttl_duration = 50, ttl_col = \"age\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE woman ADD (col6 int)  ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE EDGE e1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESC EDGE e1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE EDGE e1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP EDGE e1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}
// Column space format test, expected SyntaxError
TEST(Parser, ColumnSpacesTest) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name, age, married bool)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name, age, married)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG man(name string, age)"
                            "ttl_duration = 100";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married int, salary int), "
                            "DROP (age int)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG person ADD (col1, col2), "
                            "CHANGE (married int, salary int), "
                            "DROP (age, create_time)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER TAG person ADD (col1 int, col2 string), "
                            "CHANGE (married, salary), "
                            "DROP (age, create_time)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE man(name, age, married bool) "
                            "ttl_duration = 100";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE woman ADD (col6)  ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE woman CHANGE (col6)  ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "ALTER EDGE woman DROP (col6 int)  ttl_duration = 200";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, IndexOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX empty_field_index ON person()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX name_index ON person(name)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX IF NOT EXISTS name_index ON person(name)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX IF NOT EXISTS name_index ON person(name, age)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS empty_field_index ON service()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS like_index ON service(like)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS like_index ON service(like, score)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1(10), c2(20))";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1, c2(20))";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE INDEX IF NOT EXISTS std_index ON t1(c1(10), c2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1(10), c2(20))";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1, c2(20))";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG INDEX IF NOT EXISTS std_index ON t1(c1(10), c2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP TAG INDEX name_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP EDGE INDEX like_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE TAG INDEX name_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE EDGE INDEX like_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "REBUILD TAG INDEX name_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "REBUILD EDGE INDEX like_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Set) {
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend INTERSECT "
                            "GO FROM \"2\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend UNION "
                            "GO FROM \"2\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend UNION "
                            "GO FROM \"2\" OVER friend INTERSECT "
                            "GO FROM \"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "(GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend) UNION "
                            "GO FROM \"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        // pipe have priority over set
        std::string query = "GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend UNION "
                            "GO FROM \"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "(GO FROM \"1\" OVER friend YIELD friend._dst AS id UNION "
                            "GO FROM \"2\" OVER friend YIELD friend._dst AS id) | "
                            "GO FROM $-.id OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Pipe) {
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend | "
                            "GO FROM \"2\" OVER friend | "
                            "GO FROM \"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend MINUS "
                            "GO FROM \"2\" OVER friend | "
                            "GO FROM \"3\" OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertVertex) {
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test one vertex multi tags
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111, "
                            "\"zhangsan\", 20190527, 1111)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test multi vertex multi tags
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111, "
                            "\"zhangsan\", 20190527, 1111),"
                            "\"12346\":(\"lisi\", 20, 1112, \"lisi\", 20190413, 1112)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number) "
                            "VALUES \"zhangsan\":(\"zhangsan\", 18, 1111), "
                            "( \"zhangsan\", 20190527),"
                            "\"12346\":(\"lisi\", 20, 1112), (\"lisi\", 20190413)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES \"Tom\":(\"Tom\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person() "
                            "VALUES \"12345\":()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert prop unterminated ""
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\"Tom, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    // Test insert prop unterminated ''
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES \"Tom\":(\'Tom, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
}

TEST(Parser, UpdateVertex) {
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tome\", person.age=30, "
                            "job.salary=10000, person.create_time=1551331999";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tome\", age=30";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=$^.person.age + 1, "
                            "person.married=true "
                            "WHEN $^.job.salary > 10000 AND $^.person.age > 30";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET name=\"Tom\", age=age + 1, "
                            "married=true "
                            "WHEN salary > 10000 AND age > 30";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=31, person.married=true, "
                            "job.salary=1.1 * $^.person.create_time / 31536000 "
                            "YIELD $^.person.name AS Name, job.name AS Title, "
                            "$^.job.salary AS Salary";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age=31, married=true "
                            "YIELD name AS Name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age=30, person.married=true "
                            "WHEN $^.job.salary > 10000 AND $^.job.name == \"CTO\" OR "
                            "$^.person.age < 30"
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                            "$^.person.create_time AS Time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age=30, married=true "
                            "WHEN salary > 10000 AND name == \"CTO\" OR age < 30"
                            "YIELD name AS Name, salary AS Salary, create_time AS Time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT VERTEX \"12345\" "
                            "SET person.name=\"Tom\", person.age = 30, job.name =\"CTO\" "
                            "WHEN $^.job.salary > 10000 "
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                            "$^.person.create_time AS Time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT VERTEX ON person \"12345\" "
                            "SET name=\"Tom\", age = 30, name =\"CTO\" "
                            "WHEN salary > 10000 "
                            "YIELD name AS Name, salary AS Salary, create_time AS Time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertEdge) {
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321\":(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"from\"->\"to\":(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"from\"->\"to\":(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // multi edge
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321@1537408527\":(3.75, 1537408527),"
                            "\"56789\"->\"98765@1537408527\":(3.5, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer() "
                            "VALUES \"12345\"->\"54321@1537408527\":()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time_) "
                            "VALUES \"-12345\"->\"54321\":(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321\"@1537408527:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time_) "
                            "VALUES \"12345\"->\"54321@1537408527\":(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer() "
                            "VALUES \"12345\"->\"54321@1537408527\":()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UpdateEdge) {
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\" OF transfer "
                            "SET amount=3.14, time_=1537408527";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\" "
                            "SET amount=3.14, time_=1537408527";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\"@789 OF transfer "
                            "SET amount=3.14,time_=1537408527 "
                            "WHEN transfer.amount > 3.14 AND $^.person.name == \"Tom\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\"@789 "
                            "SET amount=3.14,time_=1537408527 "
                            "WHEN amount > 3.14";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE \"12345\" -> \"54321\" OF transfer "
                            "SET amount = 3.14 + $^.job.salary, time_ = 1537408527 "
                            "WHEN transfer.amount > 3.14 OR $^.job.salary >= 10000 "
                            "YIELD transfer.amount, transfer.time_ AS Time_, "
                            "$^.person.name AS PayFrom";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE ON transfer \"12345\" -> \"54321\" "
                            "SET amount = 3.14 + amount, time_ = 1537408527 "
                            "WHEN amount > 3.14 "
                            "YIELD amount, time_ AS Time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT EDGE \"12345\" -> \"54321\" @789 OF transfer "
                            "SET amount=$^.job.salary + 3.14, time_=1537408527 "
                            "WHEN transfer.amount > 3.14 AND $^.job.salary >= 10000 "
                            "YIELD transfer.amount,transfer.time_, $^.person.name AS Name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT EDGE ON transfer \"12345\" -> \"54321\" @789 "
                            "SET amount=$^.job.salary + 3.14, time_=1537408527 "
                            "WHEN amount > 3.14 AND salary >= 10000 "
                            "YIELD amount, time_, name AS Name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DeleteVertex) {
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"12345\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"12345\",\"23456\",\"34567\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"Tom\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"Tom\", \"Jerry\", \"Mickey\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"Tom\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX \"Tom\", \"Jerry\", \"Mickey\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"Ann\" OVER schoolmate YIELD $$.person.name as name"
                            "| DELETE VERTEX $-.id";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DeleteEdge) {
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer \"12345\" -> \"5432\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer \"123\" -> \"321\", "
                            "\"456\" -> \"654\"@11, \"789\" -> \"987\"@12";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer \"jack\" -> \"rose\","
                            "\"mr\" -> \"miss\"@13";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"Ann\" OVER schoolmate "
                            "YIELD schoolmate._src as src, schoolmate._dst as dst, "
                            "schoolmate._rank as rank"
                            "| DELETE EDGE transfer $-.src -> $-.dst @ $-.rank";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, FetchVertex) {
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"1\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"1\", \"2\", \"3\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\" "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\" "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" over edu YIELD edu._dst AS id | "
                            "FETCH PROP ON person $-.id YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM \"1\" over e1; "
                            "FETCH PROP ON person $var.id YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"1\", \"2\", \"3\" "
                            "YIELD DISTINCT person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person uuid(\"Tom\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\" "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person \"Tom\", \"darion\" "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON * \"1\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON * \"1\", \"2\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON * $-.id";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "yield \"1\" as id | FETCH PROP ON * $-.id";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "yield \"1\" as id | FETCH PROP ON * $-.id yield friend.id, person.id";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, FetchEdge) {
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON transfer \"12345\" -> \"54321\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON transfer \"12345\" -> \"54321\" "
                            "YIELD transfer.time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"12345\" OVER transfer "
                            "YIELD transfer.time_";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"12345\" OVER transfer "
                            "YIELD transfer._src AS s, serve._dst AS d | "
                            "FETCH PROP ON transfer $-.s -> $-.d YIELD transfer.amount";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM \"12345\" OVER transfer "
                            "YIELD transfer._src AS s, edu._dst AS d; "
                            "FETCH PROP ON service $var.s -> $var.d YIELD service.amount";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // this will be denied in Semantic Analysis
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON transfer, another \"12345\" -> \"-54321\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Lookup) {
    {
        GQLParser parser;
        std::string query = "LOOKUP ON person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON salary, age";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON person WHERE person.gender == \"man\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON transfer WHERE transfer.amount > 1000 YIELD transfer.amount";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON transfer WHERE transfer.amount > 1000 YIELD transfer.amount,"
                            " transfer.test";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, subgraph) {
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH FROM \"TOM\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH FROM \"TOM\" BOTH like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH FROM \"TOM\" IN like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH FROM \"TOM\" OUT like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH FROM \"TOM\" IN like OUT serve";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH 1 STEPS FROM \"TOM\" IN like OUT serve";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH 2 STEPS FROM \"TOM\" BOTH like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH 2 STEPS FROM \"TOM\" IN like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH 4 STEPS FROM \"TOM\" IN like OUT serve BOTH owner";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET SUBGRAPH 4 STEPS FROM \"TOM\" IN like OUT serve";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, AdminOperation) {
    {
        GQLParser parser;
        std::string query = "SHOW HOSTS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW HOSTS graph";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW HOSTS meta";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW HOSTS storage";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW SPACES";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW PARTS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW PARTS 666";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW TAGS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW EDGES";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW TAG INDEXES";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW EDGE INDEXES";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE SPACE default_space";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE TAG person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE EDGE like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE TAG INDEX person_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CREATE EDGE INDEX like_index";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW TAG INDEX STATUS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW EDGE INDEX STATUS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CHARSET";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW COLLATION";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW STATS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UserOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE USER user1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE USER user1 WITH PASSWORD aaa";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE USER user1 WITH PASSWORD \"aaa\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"aaa\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "ALTER USER user1 WITH PASSWORD \"a\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "ALTER USER user1 WITH PASSWORD \"a\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "DROP USER user1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "DROP USER IF EXISTS user1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "CHANGE PASSWORD \"new password\"";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CHANGE PASSWORD account TO \"new password\"";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CHANGE PASSWORD account FROM \"old password\" TO \"new password\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "GRANT ROLE ADMIN ON spacename TO account";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "GRANT ROLE DBA ON spacename TO account";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "GRANT ROLE SYSTEM ON spacename TO account";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "REVOKE ROLE ADMIN ON spacename FROM account";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "SHOW ROLES IN spacename";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "SHOW USERS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
}

TEST(Parser, UnreservedKeywords) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG tag1(space string, spaces string, "
                            "email string, password string, roles string, uuid int, "
                            "path string, variables string, leader string, data string)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE edge1(space string, spaces string, "
                            "email string, password string, roles string)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"123\" OVER guest WHERE $-.EMAIL";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM UUID(\"tom\") OVER guest WHERE $-.EMAIL";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"123\" OVER like YIELD $$.tag1.EMAIL, like.users,"
                            "like._src, like._dst, like.type, $^.tag2.SPACE "
                            "| ORDER BY $-.SPACE";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM UUID(\"tom\") OVER like YIELD $$.tag1.EMAIL, like.users,"
                            "like._src, like._dst, like.type, $^.tag2.SPACE "
                            "| ORDER BY $-.SPACE";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM \"123\" OVER like;GO FROM $var.SPACE OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM UUID(\"tom\") OVER like;GO FROM $var.SPACE OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG status(part int, parts int, job string, jobs string,"
                            " rebuild bool, submit bool, compact bool, "
                            " bidirect bool, force bool, configs string)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, Annotation) {
    {
        GQLParser parser;
        std::string query = "show spaces /* test comment....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        GQLParser parser;
        std::string query = "// test comment....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        GQLParser parser;
        std::string query = "# test comment....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        GQLParser parser;
        std::string query = "/* test comment....*/";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isStatementEmpty());
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG TAG1(space string) // test....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG TAG1(space string) # test....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG TAG1/* tag name */(space string)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG TAG1/* tag name */(space string) // test....";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DownloadAndIngest) {
    {
        GQLParser parser;
        std::string query = "DOWNLOAD HDFS \"hdfs://127.0.0.1:9090/data\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INGEST";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Agg) {
    {
        GQLParser parser;
        std::string query = "ORDER BY $-.id";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name ASC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name DESC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, friend.age as age | "
                            "ORDER BY $-.name ASC, $-.age DESC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, friend.age as age | "
                            "ORDER BY name ASC, age DESC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, ReentrantRecoveryFromFailure) {
    GQLParser parser;
    {
        std::string query = "USE dumy tag_name";
        ASSERT_FALSE(parser.parse(query).ok());
    }
    {
        std::string query = "USE space_name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, IllegalCharacter) {
    GQLParser parser;
    {
        std::string query = "USE space；";
        ASSERT_FALSE(parser.parse(query).ok());
    }
    {
        std::string query = "USE space_name；USE space";
        ASSERT_FALSE(parser.parse(query).ok());
    }
}

TEST(Parser, Distinct) {
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD DISTINCT friend.name as name, friend.age as age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        // syntax error
        std::string query = "GO FROM \"1\" OVER friend "
                            "YIELD friend.name as name, DISTINCT friend.age as age";
        auto result = parser.parse(query);
        ASSERT_TRUE(!result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER like "
                            "| GO FROM $-.id OVER like | GO FROM $-.id OVER serve "
                            "YIELD DISTINCT serve._dst, $$.team.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, ConfigOperation) {
    {
        GQLParser parser;
        std::string query = "SHOW CONFIGS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW CONFIGS GRAPH";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS storage:name=\"value\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET CONFIGS Meta:name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS load_data_interval_secs=120";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GET CONFIGS load_data_interval_secs";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = "
                            "{ stats_dump_period_sec = 200 }";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS rocksdb_db_options={disable_auto_compaction=false}";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = "
                            "{stats_dump_period_sec = 200, disable_auto_compaction = false}";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS rocksdb_column_family_options={"
                            "write_buffer_size = 1 * 1024 * 1024}";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE CONFIGS storage:rocksdb_db_options = {}";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, BalanceOperation) {
    {
        GQLParser parser;
        std::string query = "BALANCE LEADER";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA 1234567890";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA STOP";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA REMOVE 192.168.0.1:50000,192.168.0.1:50001";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA REMOVE 192.168.0.1:50000,\"localhost\":50001";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "BALANCE DATA RESET PLAN";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, CrashByFuzzer) {
    {
        GQLParser parser;
        std::string query = ";YIELD\nI41( ,1)GEGE.INGEST";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
}

TEST(Parser, FindPath) {
    {
        GQLParser parser;
        std::string query = "FIND SHORTEST PATH FROM \"1\" TO \"2\" OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND ALL PATH FROM \"1\" TO \"2\" OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND SHORTEST PATH WITH PROP FROM \"1\" TO \"2\" OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND ALL PATH WITH PROP FROM \"1\" TO \"2\" OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Limit) {
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1,2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work | LIMIT 1 OFFSET 2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // ERROR
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work | LIMIT \"1\"";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, GroupBy) {
    // All fun succeed
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work "
                            "YIELD $$.company.name, $^.person.name "
                            "| GROUP BY $$.company.name "
                            "YIELD $$.company.name as name, "
                            "COUNT($^.person.name ), "
                            "COUNT_DISTINCT($^.person.name ), "
                            "SUM($^.person.name ), "
                            "AVG($^.person.name ), "
                            "MAX($^.person.name ), "
                            "MIN($^.person.name ), "
                            "F_STD($^.person.name ), "
                            "F_BIT_AND($^.person.name ), "
                            "F_BIT_OR($^.person.name ), "
                            "F_BIT_XOR($^.person.name )";

        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Error syntax
    {
        GQLParser parser;
        std::string query = "YIELD rand32() as id, sum(1) as sum, avg(2) as avg GROUP BY id";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    // All fun error, empty group name
    {
        GQLParser parser;
        std::string query = "GO FROM \"1\" OVER work "
                            "YIELD $$.company.name, $^.person.name "
                            "| GROUP BY "
                            "YIELD $$.company.name as name, "
                            "COUNT($^.person.name )";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, Return) {
    {
        GQLParser parser;
        std::string query = "RETURN $A IF $A IS NOT NULL";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, ErrorMsg) {
    {
        GQLParser parser;
        std::string query = "CREATE SPACE " + std::string(4097, 'A');
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range of the LABEL length, "
                     "the  max length of LABEL is 4096: near `" + std::string(80, 'A') + "'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775809) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775809'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min integer bound checking
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-9223372036854775808) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-(1+9223372036854775808)) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-9223372036854775809) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775809'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max integer bound checking
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775807) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+9223372036854775807) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(9223372036854775808) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775808'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+9223372036854775808) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `9223372036854775808'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0xFFFFFFFFFFFFFFFFF) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0xFFFFFFFFFFFFFFFFF'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min hex integer bound
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-0x8000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-0x8000000000000001) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000001'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max hex integer bound
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0x7FFFFFFFFFFFFFFF) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0x7FFFFFFFFFFFFFFF) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0x8000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0x8000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `0x8000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(002777777777777777777777) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `002777777777777777777777'";
        ASSERT_EQ(error, result.status().toString());
    }
    // min oct integer bound
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-01000000000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(-01000000000000000000001) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000001'";
        ASSERT_EQ(error, result.status().toString());
    }
    // max oct integer bound
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(0777777777777777777777) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+0777777777777777777777) ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status().toString();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(01000000000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(id) VALUES \"100\":(+01000000000000000000000) ";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
        auto error = "SyntaxError: Out of range: near `01000000000000000000000'";
        ASSERT_EQ(error, result.status().toString());
    }
}

TEST(Parser, UseReservedKeyword) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG tag()";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());

        query = "CREATE TAG `tag`()";
        result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE EDGE edge()";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());

        query = "CREATE EDGE `edge`()";
        result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG `person`(`tag` string)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG `time`(`time` string)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
}

TEST(Parser, TypeCast) {
    {
        GQLParser parser;
        std::string query = "YIELD (INT)\"123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT)  \"123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT)\".123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT8)\".123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT16)\".123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT32)\".123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT64)\"1.23\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT65)\"1.23\"";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (DOUBLE)\"123abc\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT)\"abc123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (FLOAT)\"123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (FLOAT)\"1.23\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (FLOAT)\".123\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (STRING)123";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (STRING)1.23";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (MAP)123";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "YIELD (INT)true";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok());
    }
}

TEST(Parser, Match) {
    {
        GQLParser parser;
        std::string query = "MATCH () -- () RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH () <-- () RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH () --> () RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH () <--> () RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) --> (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a:person) --> (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (:person {name: 'Tom'}) --> (:person {name: 'Jerry'}) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[]- (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[]-> (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) <-[]- (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) <-[m:like]-> (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) <-[m:like{likeness: 99}]-> (b) RETURN *";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) WHERE a.name == 'Tom' RETURN a";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like|serve]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like|:serve]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*2]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*2..]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*..2]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*2..4]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m:like*..2]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH p = (a) -[m:like*..2]- (b) RETURN p as Path";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "OPTIONAL MATCH (a) -[m]- (b) RETURN a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN DISTINCT a as Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person SKIP 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person ORDER BY Person SKIP 1 LIMIT 2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person SKIP 1 LIMIT 2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person SKIP 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) -[m]- (b) RETURN a as Person LIMIT 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a AS b RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH DISTINCT a as b RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b ORDER BY b RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b ORDER BY b SKIP 1 LIMIT 1 RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b SKIP 1 LIMIT 1 RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b LIMIT 1 RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b SKIP 1 RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "WITH a as b SKIP 1 WHERE b > 0 RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UNWIND a as b RETURN b";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "MATCH (a) MATCH (b) -[m]- (c) RETURN a,b,c";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UNWIND a AS b MATCH (c) -[m]- (d) RETURN b,c,d";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UNWIND a AS b MATCH (c) MATCH (d) WITH e MATCH (f) RETURN b,c,d";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Zone) {
    {
        GQLParser parser;
        std::string query = "SHOW GROUPS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW ZONES";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ADD ZONE zone_0 127.0.0.1:8989,127.0.0.1:8988,127.0.0.1:8987";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ADD HOST 127.0.0.1:8989 INTO ZONE zone_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP HOST 127.0.0.1:8989 FROM ZONE zone_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESC ZONE zone_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE ZONE zone_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP ZONE zone_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ADD GROUP group_0 zone_0,zone_1,zone_2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ADD ZONE zone_3 INTO GROUP group_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP ZONE zone_3 FROM GROUP group_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESC GROUP group_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DESCRIBE GROUP group_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DROP GROUP group_0";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, FullText) {
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1, 2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", 1, 2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", 1, 2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 1, 2)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", AUTO, AND)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", AUTO, OR)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, AND)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1, 1)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, -1, 1)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 0, OR, 1, -1)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", -1, OR, 1, 1)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE FUZZY(t1.c1, \"a\", 4, OR, 1, 1)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", -1, 2)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", 1, -2)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE PREFIX(t1.c1, \"a\", AUTO, AND)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE WILDCARD(t1.c1, \"a\", AUTO, AND)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
    {
        GQLParser parser;
        std::string query = "LOOKUP ON t1 WHERE REGEXP(t1.c1, \"a\", AUTO, AND)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, FullTextServiceTest) {
    {
        GQLParser parser;
        std::string query = "ADD LISTENER ELASTICSEARCH 127.0.0.1:12000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "ADD LISTENER ELASTICSEARCH 127.0.0.1:12000, 127.0.0.1:12001";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "REMOVE LISTENER ELASTICSEARCH";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW LISTENER";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200), (127.0.0.1:9300)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\", \"password\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\", \"password\"), "
                            "(127.0.0.1:9200, \"user\", \"password\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN OUT TEXT SERVICE";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SIGN IN TEXT SERVICE (127.0.0.1:9200, \"user\")";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}
}   // namespace nebula
