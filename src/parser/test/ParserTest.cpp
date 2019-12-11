/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "base/Base.h"
#include "parser/GQLParser.h"

// TODO(dutor) Inspect the internal structures to check on the syntax and semantics

namespace nebula {

TEST(Parser, Go) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend;";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO 2 STEPS FROM 1 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO UPTO 2 STEPS FROM 1 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend REVERSELY";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend YIELD person.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend "
                            "YIELD $^.manager.name,$^.manager.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend "
                            "YIELD $$.manager.name,$$.manager.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend "
                            "YIELD $^[manager].name,$^[manager].age";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1,2,3 OVER friend";
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
        std::string query = "GO FROM 1,2,3 OVER friend WHERE person.name == \"dutor\"";
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
}

TEST(Parser, TagOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE TAG person(name string, age int, "
                            "married bool, salary double, create_time timestamp)";
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
                            "married bool, salary double, create_time timestamp)"
                            "ttl_duration = 100";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string, age int, "
                            "married bool, salary double, create_time timestamp)"
                            "ttl_duration = 100, ttl_col = create_time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string, age int default 22, "
                            "married bool default false, salary double default 1000.0, "
                            "create_time timestamp)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "CREATE TAG woman(name string default \"\", age int default 22, "
                            "married bool default false, salary double default 1000.0, "
                            "create_time timestamp default 1566541858)";
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
        std::string query = "ALTER TAG woman ttl_duration = 50, ttl_col = age";
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
                            "married bool, salary double, create_time timestamp)";
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
                            "married bool, salary double, create_time timestamp)"
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
                            "married bool, salary double, create_time timestamp)"
                            "ttl_duration = 100, ttl_col = create_time";
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
        std::string query = "ALTER EDGE woman ttl_duration = 50, ttl_col = age";
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
                            "DROP (age int, create_time timestamp)";
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

TEST(Parser, Set) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend INTERSECT "
                            "GO FROM 2 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend UNION "
                            "GO FROM 2 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend MINUS "
                            "GO FROM 2 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend MINUS "
                            "GO FROM 2 OVER friend UNION "
                            "GO FROM 2 OVER friend INTERSECT "
                            "GO FROM 3 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "(GO FROM 1 OVER friend | "
                            "GO FROM 2 OVER friend) UNION "
                            "GO FROM 3 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        // pipe have priority over set
        std::string query = "GO FROM 1 OVER friend | "
                            "GO FROM 2 OVER friend UNION "
                            "GO FROM 3 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "(GO FROM 1 OVER friend UNION "
                            "GO FROM 2 OVER friend) | "
                            "GO FROM $- OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Pipe) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend | "
                            "GO FROM 2 OVER friend | "
                            "GO FROM 3 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER friend MINUS "
                            "GO FROM 2 OVER friend | "
                            "GO FROM 3 OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertVertex) {
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES 12345:(\"dutor\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test one vertex multi tags
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES 12345:(\"zhangsan\", 18, 1111, \"zhangsan\", 20190527, 1111)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test multi vertex multi tags
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number, id) "
                            "VALUES 12345:(\"zhangsan\", 18, 1111, \"zhangsan\", 20190527, 1111),"
                            "12346:(\"lisi\", 20, 1112, \"lisi\", 20190413, 1112)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test wrong syntax
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age, id), student(name, number) "
                            "VALUES 12345:(\"zhangsan\", 18, 1111), ( \"zhangsan\", 20190527),"
                            "12346:(\"lisi\", 20, 1112), (\"lisi\", 20190413)";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES -12345:(\"dutor\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES hash(\"dutor\"):(\"dutor\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary,create_time) "
                            "VALUES uuid(\"dutor\"):(\"dutor\", 30, true, 3.14, 1551331900)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person() "
                            "VALUES 12345:()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert prop unterminated ""
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES 12345:(\"dutor, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    // Test insert prop unterminated ''
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES 12345:(\'dutor, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES hash(\"dutor\"):(\'dutor, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name, age) "
                            "VALUES uuid(\"dutor\"):(\'dutor, 30)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.status().isSyntaxError());
    }
}

TEST(Parser, UpdateVertex) {
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 "
                            "SET person.name=\"dutor\", person.age=30, "
                                "job.salary=10000, person.create_time=1551331999";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 "
                            "SET person.name=\"dutor\", person.age=$^.person.age + 1, "
                                "person.married=true "
                            "WHEN $^.job.salary > 10000 && $^.person.age > 30";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 "
                            "SET person.name=\"dutor\", person.age=31, person.married=true, "
                                "job.salary=1.1 * $^.person.create_time / 31536000 "
                            "YIELD $^.person.name AS Name, job.name AS Title, "
                                  "$^.job.salary AS Salary";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 "
                            "SET person.name=\"dutor\", person.age=30, person.married=true "
                            "WHEN $^.job.salary > 10000 && $^.job.name == \"CTO\" || "
                                  "$^.person.age < 30"
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                                  "$^.person.create_time AS Time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT VERTEX 12345 "
                            "SET person.name=\"dutor\", person.age = 30, job.name =\"CTO\" "
                            "WHEN $^.job.salary > 10000 "
                            "YIELD $^.person.name AS Name, $^.job.salary AS Salary, "
                                  "$^.person.create_time AS Time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertEdge) {
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES 12345->-54321:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES hash(\"from\")->hash(\"to\"):(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES uuid(\"from\")->uuid(\"to\"):(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // multi edge
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES 12345->54321@1537408527:(3.75, 1537408527),"
                            "56789->98765@1537408527:(3.5, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer() "
                            "VALUES 12345->54321@1537408527:()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES -12345->54321:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES 12345->54321@1537408527:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES 12345->-54321@1537408527:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // Test insert empty value
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer() "
                            "VALUES 12345->54321@1537408527:()";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UpdateEdge) {
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 OF transfer "
                            "SET amount=3.14, time=1537408527";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321@789 OF transfer "
                            "SET amount=3.14,time=1537408527 "
                            "WHEN transfer.amount > 3.14 && $^.person.name == \"dutor\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 OF transfer "
                            "SET amount = 3.14 + $^.job.salary, time = 1537408527 "
                            "WHEN transfer.amount > 3.14 || $^.job.salary >= 10000 "
                            "YIELD transfer.amount, transfer.time AS Time, "
                                "$^.person.name AS PayFrom";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPSERT EDGE 12345 -> 54321 @789 OF transfer "
                            "SET amount=$^.job.salary + 3.14, time=1537408527 "
                            "WHEN transfer.amount > 3.14 && $^.job.salary >= 10000 "
                            "YIELD transfer.amount,transfer.time, $^.person.name AS Name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DeleteVertex) {
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX 12345";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX hash(\"zhangsan\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DeleteEdge) {
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer 12345 -> 54321";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer 123 -> 321,456 -> 654@11,789 -> 987@12";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE transfer uuid(\"jack\") -> uuid(\"rose\"),"
                            "uuid(\"mr\") -> uuid(\"miss\")@13";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, FetchVertex) {
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person 1, 2, 3";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person hash(\"dutor\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person hash(\"dutor\"), hash(\"darion\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person hash(\"dutor\") "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person hash(\"dutor\"), hash(\"darion\") "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over edu | "
                            "FETCH PROP ON person $- YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM 1 over e1; "
                            "FETCH PROP ON person $var.id YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person 1,2,3 "
                            "YIELD DISTINCT person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person uuid(\"dutor\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person uuid(\"dutor\"), uuid(\"darion\")";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person uuid(\"dutor\") "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON person uuid(\"dutor\"), uuid(\"darion\") "
                            "YIELD person.name, person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, FetchEdge) {
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON transfer 12345 -> -54321";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FETCH PROP ON transfer 12345 -> -54321 "
                            "YIELD transfer.time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 12345 OVER transfer "
                            "YIELD transfer.time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 12345 OVER transfer "
                            "YIELD transfer._src AS s, serve._dst AS d | "
                            "FETCH PROP ON transfer $-.s -> $-.d YIELD transfer.amount";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM 12345 OVER transfer "
                            "YIELD transfer._src AS s, edu._dst AS d; "
                            "FETCH PROP ON service $var.s -> $var.d YIELD service.amount";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Find) {
    {
        GQLParser parser;
        std::string query = "FIND name FROM person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND name, salary, age FROM person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND name, salary, age FROM person WHERE gender == \"man\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "FIND amount, time FROM transfer WHERE amount > 1000";
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
        std::string query = "SHOW CREATE EDGE e1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UserOperation) {
    {
        GQLParser parser;
        std::string query = "CREATE USER user1 WITH PASSWORD \"aaa\" ";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"aaa\" , "
                            "FIRSTNAME \"a\", LASTNAME \"a\", EMAIL \"a\", PHONE \"111\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto& sentence = result.value();
        EXPECT_EQ(query, sentence->toString());
    }
    {
        GQLParser parser;
        std::string query = "ALTER USER user1 WITH FIRSTNAME \"a\","
                            " LASTNAME \"a\", EMAIL \"a\", PHONE \"111\"";
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
        std::string query = "SHOW USER account";
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
                            "email string, password string, roles string)";
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
        std::string query = "GO FROM 123 OVER guest WHERE $-.EMAIL";
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
        std::string query = "GO FROM 123 OVER like YIELD $$.tag1.EMAIL, like.users,"
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
        std::string query = "$var = GO FROM 123 OVER like;GO FROM $var.SPACE OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "$var = GO FROM UUID(\"tom\") OVER like;GO FROM $var.SPACE OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
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
        std::string query = "-- test comment....";
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
        std::string query = "CREATE TAG TAG1(space string) -- test....";
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
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name | "
                            "ORDER BY name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name ASC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name | "
                            "ORDER BY $-.name DESC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name, friend.age as age | "
                            "ORDER BY $-.name ASC, $-.age DESC";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 over friend "
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
        std::string query = "GO FROM 1 over friend "
                            "YIELD DISTINCT friend.name as name, friend.age as age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        // syntax error
        std::string query = "GO FROM 1 over friend "
                            "YIELD friend.name as name, DISTINCT friend.age as age";
        auto result = parser.parse(query);
        ASSERT_TRUE(!result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER like "
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
        std::string query = "UPDATE CONFIGS storage:name=value";
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
}

TEST(Parser, CrashByFuzzer) {
    {
        GQLParser parser;
        std::string query = ";MATCH";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }

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
        std::string query = "FIND SHORTEST PATH FROM 1 TO 2 OVER like";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Limit) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER work | LIMIT 1";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER work | LIMIT 1,2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER work | LIMIT 1 OFFSET 2";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    // ERROR
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER work | LIMIT \"1\"";
        auto result = parser.parse(query);
        ASSERT_FALSE(result.ok());
    }
}

TEST(Parser, GroupBy) {
    // All fun succeed
    {
        GQLParser parser;
        std::string query = "GO FROM 1 OVER work "
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
        std::string query = "GO FROM 1 OVER work "
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
}   // namespace nebula
