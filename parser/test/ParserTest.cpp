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
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person;";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO 2 STEPS FROM 1 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO UPTO 2 STEPS FROM 1 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person OVER friend";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person OVER friend REVERSELY";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person RETURN person.name";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person RETURN person[manager].name,person.age";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1,2,3 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM [$1,$2,$5] AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1,2,3 AS person WHERE person.name == \"dutor\"";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, UseNamespace) {
    {
        GQLParser parser;
        std::string query = "USE NAMESPACE ns";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, DefineTag) {
    {
        GQLParser parser;
        std::string query = "DEFINE TAG person(name string, age uint8 TTL = 100, "
                            "married bool, salary double)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, AlterTag) {
    {
        GQLParser parser;
        std::string query = "ALTER TAG person(age uint8 TTL = 200)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, Set) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person INTERSECT GO FROM 2 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person UNION GO FROM 2 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person "
                            "UNION GO FROM 3 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, Pipe) {
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person | GO FROM 2 AS person | GO FROM 3 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "GO FROM 1 AS person MINUS GO FROM 2 AS person | GO FROM 3 AS person";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, InsertVertex) {
    {
        GQLParser parser;
        std::string query = "INSERT VERTEX person(name,age,married,salary) "
                            "VALUES(12345: \"dutor\", 30, true, 3.14)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "INSERT TAG person(name,age,married,salary) "
                            "VALUES(12345: \"dutor\", 30, true, 3.14)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, UpdateVertex) {
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=30,married=true";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=31,married=true "
                            "WHERE salary > 10000";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE VERTEX 12345 SET name=\"dutor\",age=30,married=true "
                            "RETURN name,salary";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE OR INSERT VERTEX 12345 SET name=\"dutor\",age=30,married=true "
                            "RETURN name,salary";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, InsertEdge) {
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES(12345 -> 54321: 3.75, 1537408527)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES(12345 -> 54321: 3.75, 1537408527)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE transfer(amount, time) "
                            "VALUES(12345 -> 54321 @1537408527: 3.75, 1537408527)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "INSERT EDGE NO OVERWRITE transfer(amount, time) "
                            "VALUES(12345 -> 54321 @1537408527: 3.75, 1537408527)";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

TEST(Parser, UpdateEdge) {
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14 RETURN amount,time";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
    {
        GQLParser parser;
        std::string query = "UPDATE OR INSERT EDGE 12345 -> 54321 SET amount=3.14,time=1537408527 "
                            "WHERE amount > 3.14 RETURN amount,time";
        ASSERT_TRUE(parser.parse(query)) << parser.error();
    }
}

}   // namespace vesoft
