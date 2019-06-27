/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
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
                            "YIELD $^[manager].name,$^[manager].age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
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
        std::string query = "DESCRIBE TAG person";
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
        std::string query = "DESCRIBE EDGE e1";
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
                            "GO FROM 3 OVER friend";
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
}

TEST(Parser, UpdateVertex) {
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\", age=30, "
                            "married=true, create_time=1551331999";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\", age=31, "
                            "married=true, create_time=1551332019 WHERE salary > 10000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\", age=31, married=true, "
                            "create_time=1551332019 WHERE create_time > 1551332018";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\", age=30, married=true "
                            "YIELD name, salary, create_time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE OR INSERT VERTEX 12345 SET name=\"dutor\", age=30, "
                            "married=true YIELD name, salary, create_time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertEdge) {
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES 12345->54321:(3.75, 1537408527)";
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
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES 12345->54321:(3.75, 1537408527)";
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
                            "VALUES 12345->54321@1537408527:(3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UpdateEdge) {
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14 YIELD amount,time";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE OR INSERT EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14 YIELD amount,time";
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
        std::string query = "DELETE VERTEX 123,456,789";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX 12345 WHERE salary > 10000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE VERTEX 123,456,789 WHERE salary > 10000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DeleteEdge) {
    {
        GQLParser parser;
        std::string query = "DELETE EDGE 12345 -> 54321";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE 123 -> 321,456 -> 654,789 -> 987";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE 12345 -> 54321 WHERE amount > 3.14";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "DELETE EDGE 123 -> 321,456 -> 654,789 -> 987 WHERE amount > 3.14";
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
        std::string query = "ADD HOSTS 127.0.0.1:1000, 127.0.0.1:9000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "SHOW HOSTS";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "REMOVE HOSTS 127.0.0.1:1000, 127.0.0.1:9000";
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
}


TEST(Parser, Annotation) {
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

}   // namespace nebula
