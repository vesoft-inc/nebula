/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "parser/GQLParser.h"

// TODO(dutor) Inspect the internal structures to check on the syntax and semantics

namespace vesoft {

TEST(Parser, Go) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person;";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO 2 STEPS FROM 1 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO UPTO 2 STEPS FROM 1 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person OVER friend";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person OVER friend REVERSELY";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person YIELD person.name";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person YIELD person[manager].name,person.age";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1,2,3 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $_._id AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $_.col1 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM $_[person].id AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1,2,3 AS person WHERE person.name == \"dutor\"";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UseNamespace) {
    {
        GQLParser parser;
        std::string query = "USE SPACE ns";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, DefineTag) {
    {
        GQLParser parser;
        std::string query = "DEFINE TAG person(name string, age uint8 TTL = 100, "
                            "married bool, salary double)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, AlterTag) {
    {
        GQLParser parser;
        std::string query = "ALTER TAG person(age uint8 TTL = 200)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Set) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person INTERSECT GO FROM 2 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person UNION GO FROM 2 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person "
                            "UNION GO FROM 3 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, Pipe) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person | GO FROM 2 AS person | GO FROM 3 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person | GO FROM 3 AS person";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertVertex) {
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary) "
                            "VALUES(12345: \"dutor\", 30, true, 3.14)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT TAG person(name,age,married,salary) "
                            "VALUES(12345: \"dutor\", 30, true, 3.14)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, UpdateVertex) {
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=30,married=true";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=31,married=true "
                            "WHERE salary > 10000";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=30,married=true "
                            "YIELD name,salary";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE OR INSERT VERTEX 12345 SET name=\"dutor\",age=30,married=true "
                            "YIELD name,salary";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
}

TEST(Parser, InsertEdge) {
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES(12345 -> 54321: 3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES(12345 -> 54321: 3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES(12345 -> 54321 @1537408527: 3.75, 1537408527)";
        auto result = parser.parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES(12345 -> 54321 @1537408527: 3.75, 1537408527)";
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

}   // namespace vesoft
