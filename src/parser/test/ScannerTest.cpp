/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <gtest/gtest.h>
#include <sstream>
#include <vector>
#include <utility>
#include "parser/GraphParser.hpp"
#include "parser/GraphScanner.h"

using testing::AssertionFailure;
using testing::AssertionSuccess;
using testing::AssertionResult;

namespace nebula {

using semantic_type = nebula::GraphParser::semantic_type;
static auto checkSemanticValue(const char *expected, semantic_type *sv) {
    auto actual = *sv->strval;
    delete sv->strval;
    if (expected != actual) {
        return AssertionFailure() << "Semantic value not match, "
                                  << "expected: " << expected
                                  << ", actual: " << actual;
    }
    return AssertionSuccess();
}


static auto checkSemanticValue(bool expected, semantic_type *sv) {
    auto actual = sv->boolval;
    if (expected != actual) {
        return AssertionFailure() << "Semantic value not match, "
                                  << "expected: " << expected
                                  << ", actual: " << actual;
    }
    return AssertionSuccess();
}


template <typename T>
static std::enable_if_t<std::is_integral<T>::value, AssertionResult>
checkSemanticValue(T expected, semantic_type *sv) {
    auto actual = static_cast<T>(sv->intval);
    if (expected != actual) {
        return AssertionFailure() << "Semantic value not match, "
                                  << "expected: " << expected
                                  << ", actual: " << actual;
    }
    return AssertionSuccess();
}


template <typename T>
static std::enable_if_t<std::is_floating_point<T>::value, AssertionResult>
checkSemanticValue(T expected, semantic_type *sv) {
    auto actual = static_cast<T>(sv->doubleval);
    if (expected != actual) {
        return AssertionFailure() << "Semantic value not match, "
                                  << "expected: " << expected
                                  << ", actual: " << actual;
    }
    return AssertionSuccess();
}


TEST(Scanner, Basic) {
    using TokenType = nebula::GraphParser::token_type;
    using Validator = std::function<::testing::AssertionResult()>;
    nebula::GraphParser::semantic_type yylval;
    nebula::GraphParser::location_type yyloc;
    GraphScanner scanner;
    std::string stream;

#define CHECK_SEMANTIC_TYPE(STR, TYPE)                                      \
    (stream += " ", stream += STR, [&] () {                                 \
        auto actual = scanner.yylex(&yylval, &yyloc);                       \
        if (actual != TYPE) {                                               \
            return AssertionFailure() << "Token type not match for `"       \
                                      << STR << "', expected: "             \
                                      << static_cast<int>(TYPE)             \
                                      << ", actual: "                       \
                                      << static_cast<int>(actual);          \
        } else {                                                            \
            return AssertionSuccess();                                      \
        }                                                                   \
    })

#define CHECK_SEMANTIC_VALUE(STR, TYPE, value)                              \
    (stream += " ", stream += STR, [&] () {                                 \
        auto actual = scanner.yylex(&yylval, &yyloc);                       \
        if (actual != TYPE) {                                               \
            return AssertionFailure() << "Token type not match for `"       \
                                      << STR << "', expected: "             \
                                      << static_cast<int>(TYPE)             \
                                      << ", actual: "                       \
                                      << static_cast<int>(actual);          \
        } else {                                                            \
            return checkSemanticValue(value, &yylval);                      \
        }                                                                   \
    })

    std::vector<Validator> validators = {
        CHECK_SEMANTIC_TYPE(".", TokenType::DOT),
        CHECK_SEMANTIC_TYPE(",", TokenType::COMMA),
        CHECK_SEMANTIC_TYPE(":", TokenType::COLON),
        CHECK_SEMANTIC_TYPE(";", TokenType::SEMICOLON),
        CHECK_SEMANTIC_TYPE("+", TokenType::ADD),
        CHECK_SEMANTIC_TYPE("-", TokenType::SUB),
        CHECK_SEMANTIC_TYPE("*", TokenType::MUL),
        CHECK_SEMANTIC_TYPE("/", TokenType::DIV),
        CHECK_SEMANTIC_TYPE("%", TokenType::MOD),
        CHECK_SEMANTIC_TYPE("!", TokenType::NOT),
        CHECK_SEMANTIC_TYPE("@", TokenType::AT),

        CHECK_SEMANTIC_TYPE("<", TokenType::LT),
        CHECK_SEMANTIC_TYPE("<=", TokenType::LE),
        CHECK_SEMANTIC_TYPE(">", TokenType::GT),
        CHECK_SEMANTIC_TYPE(">=", TokenType::GE),
        CHECK_SEMANTIC_TYPE("==", TokenType::EQ),
        CHECK_SEMANTIC_TYPE("!=", TokenType::NE),

        CHECK_SEMANTIC_TYPE("||", TokenType::OR),
        CHECK_SEMANTIC_TYPE("&&", TokenType::AND),
        CHECK_SEMANTIC_TYPE("|", TokenType::PIPE),
        CHECK_SEMANTIC_TYPE("=", TokenType::ASSIGN),
        CHECK_SEMANTIC_TYPE("(", TokenType::L_PAREN),
        CHECK_SEMANTIC_TYPE(")", TokenType::R_PAREN),
        CHECK_SEMANTIC_TYPE("[", TokenType::L_BRACKET),
        CHECK_SEMANTIC_TYPE("]", TokenType::R_BRACKET),
        CHECK_SEMANTIC_TYPE("{", TokenType::L_BRACE),
        CHECK_SEMANTIC_TYPE("}", TokenType::R_BRACE),

        CHECK_SEMANTIC_TYPE("<-", TokenType::L_ARROW),
        CHECK_SEMANTIC_TYPE("->", TokenType::R_ARROW),

        CHECK_SEMANTIC_TYPE("$-", TokenType::INPUT_REF),
        CHECK_SEMANTIC_TYPE("$^", TokenType::SRC_REF),
        CHECK_SEMANTIC_TYPE("$$", TokenType::DST_REF),

        CHECK_SEMANTIC_TYPE("GO", TokenType::KW_GO),
        CHECK_SEMANTIC_TYPE("go", TokenType::KW_GO),
        CHECK_SEMANTIC_TYPE("AS", TokenType::KW_AS),
        CHECK_SEMANTIC_TYPE("as", TokenType::KW_AS),
        CHECK_SEMANTIC_TYPE("TO", TokenType::KW_TO),
        CHECK_SEMANTIC_TYPE("to", TokenType::KW_TO),
        CHECK_SEMANTIC_TYPE("USE", TokenType::KW_USE),
        CHECK_SEMANTIC_TYPE("use", TokenType::KW_USE),
        CHECK_SEMANTIC_TYPE("SET", TokenType::KW_SET),
        CHECK_SEMANTIC_TYPE("set", TokenType::KW_SET),
        CHECK_SEMANTIC_TYPE("FROM", TokenType::KW_FROM),
        CHECK_SEMANTIC_TYPE("from", TokenType::KW_FROM),
        CHECK_SEMANTIC_TYPE("WHERE", TokenType::KW_WHERE),
        CHECK_SEMANTIC_TYPE("where", TokenType::KW_WHERE),
        CHECK_SEMANTIC_TYPE("MATCH", TokenType::KW_MATCH),
        CHECK_SEMANTIC_TYPE("match", TokenType::KW_MATCH),
        CHECK_SEMANTIC_TYPE("INSERT", TokenType::KW_INSERT),
        CHECK_SEMANTIC_TYPE("insert", TokenType::KW_INSERT),
        CHECK_SEMANTIC_TYPE("VALUES", TokenType::KW_VALUES),
        CHECK_SEMANTIC_TYPE("values", TokenType::KW_VALUES),
        CHECK_SEMANTIC_TYPE("YIELD", TokenType::KW_YIELD),
        CHECK_SEMANTIC_TYPE("yield", TokenType::KW_YIELD),
        CHECK_SEMANTIC_TYPE("RETURN", TokenType::KW_RETURN),
        CHECK_SEMANTIC_TYPE("return", TokenType::KW_RETURN),
        CHECK_SEMANTIC_TYPE("VERTEX", TokenType::KW_VERTEX),
        CHECK_SEMANTIC_TYPE("vertex", TokenType::KW_VERTEX),
        CHECK_SEMANTIC_TYPE("EDGE", TokenType::KW_EDGE),
        CHECK_SEMANTIC_TYPE("edge", TokenType::KW_EDGE),
        CHECK_SEMANTIC_TYPE("UPDATE", TokenType::KW_UPDATE),
        CHECK_SEMANTIC_TYPE("update", TokenType::KW_UPDATE),
        CHECK_SEMANTIC_TYPE("ALTER", TokenType::KW_ALTER),
        CHECK_SEMANTIC_TYPE("alter", TokenType::KW_ALTER),
        CHECK_SEMANTIC_TYPE("STEPS", TokenType::KW_STEPS),
        CHECK_SEMANTIC_TYPE("steps", TokenType::KW_STEPS),
        CHECK_SEMANTIC_TYPE("OVER", TokenType::KW_OVER),
        CHECK_SEMANTIC_TYPE("over", TokenType::KW_OVER),
        CHECK_SEMANTIC_TYPE("UPTO", TokenType::KW_UPTO),
        CHECK_SEMANTIC_TYPE("upto", TokenType::KW_UPTO),
        CHECK_SEMANTIC_TYPE("REVERSELY", TokenType::KW_REVERSELY),
        CHECK_SEMANTIC_TYPE("reversely", TokenType::KW_REVERSELY),
        CHECK_SEMANTIC_TYPE("SPACE", TokenType::KW_SPACE),
        CHECK_SEMANTIC_TYPE("space", TokenType::KW_SPACE),
        CHECK_SEMANTIC_TYPE("SPACES", TokenType::KW_SPACES),
        CHECK_SEMANTIC_TYPE("spaces", TokenType::KW_SPACES),
        CHECK_SEMANTIC_TYPE("TTL", TokenType::KW_TTL),
        CHECK_SEMANTIC_TYPE("ttl", TokenType::KW_TTL),
        CHECK_SEMANTIC_TYPE("BIGINT", TokenType::KW_BIGINT),
        CHECK_SEMANTIC_TYPE("bigint", TokenType::KW_BIGINT),
        CHECK_SEMANTIC_TYPE("DOUBLE", TokenType::KW_DOUBLE),
        CHECK_SEMANTIC_TYPE("double", TokenType::KW_DOUBLE),
        CHECK_SEMANTIC_TYPE("STRING", TokenType::KW_STRING),
        CHECK_SEMANTIC_TYPE("string", TokenType::KW_STRING),
        CHECK_SEMANTIC_TYPE("BOOL", TokenType::KW_BOOL),
        CHECK_SEMANTIC_TYPE("bool", TokenType::KW_BOOL),
        CHECK_SEMANTIC_TYPE("TAG", TokenType::KW_TAG),
        CHECK_SEMANTIC_TYPE("tag", TokenType::KW_TAG),
        CHECK_SEMANTIC_TYPE("UNION", TokenType::KW_UNION),
        CHECK_SEMANTIC_TYPE("union", TokenType::KW_UNION),
        CHECK_SEMANTIC_TYPE("INTERSECT", TokenType::KW_INTERSECT),
        CHECK_SEMANTIC_TYPE("intersect", TokenType::KW_INTERSECT),
        CHECK_SEMANTIC_TYPE("MINUS", TokenType::KW_MINUS),
        CHECK_SEMANTIC_TYPE("minus", TokenType::KW_MINUS),
        CHECK_SEMANTIC_TYPE("SHOW", TokenType::KW_SHOW),
        CHECK_SEMANTIC_TYPE("show", TokenType::KW_SHOW),
        CHECK_SEMANTIC_TYPE("Show", TokenType::KW_SHOW),
        CHECK_SEMANTIC_TYPE("ADD", TokenType::KW_ADD),
        CHECK_SEMANTIC_TYPE("add", TokenType::KW_ADD),
        CHECK_SEMANTIC_TYPE("Add", TokenType::KW_ADD),
        CHECK_SEMANTIC_TYPE("HOSTS", TokenType::KW_HOSTS),
        CHECK_SEMANTIC_TYPE("hosts", TokenType::KW_HOSTS),
        CHECK_SEMANTIC_TYPE("Hosts", TokenType::KW_HOSTS),
        CHECK_SEMANTIC_TYPE("TIMESTAMP", TokenType::KW_TIMESTAMP),
        CHECK_SEMANTIC_TYPE("timestamp", TokenType::KW_TIMESTAMP),
        CHECK_SEMANTIC_TYPE("Timestamp", TokenType::KW_TIMESTAMP),
        CHECK_SEMANTIC_TYPE("DELETE", TokenType::KW_DELETE),
        CHECK_SEMANTIC_TYPE("delete", TokenType::KW_DELETE),
        CHECK_SEMANTIC_TYPE("Delete", TokenType::KW_DELETE),
        CHECK_SEMANTIC_TYPE("FIND", TokenType::KW_FIND),
        CHECK_SEMANTIC_TYPE("find", TokenType::KW_FIND),
        CHECK_SEMANTIC_TYPE("Find", TokenType::KW_FIND),
        CHECK_SEMANTIC_TYPE("CREATE", TokenType::KW_CREATE),
        CHECK_SEMANTIC_TYPE("create", TokenType::KW_CREATE),
        CHECK_SEMANTIC_TYPE("Create", TokenType::KW_CREATE),
        CHECK_SEMANTIC_TYPE("PARTITION_NUM", TokenType::KW_PARTITION_NUM),
        CHECK_SEMANTIC_TYPE("partition_num", TokenType::KW_PARTITION_NUM),
        CHECK_SEMANTIC_TYPE("Partition_num", TokenType::KW_PARTITION_NUM),
        CHECK_SEMANTIC_TYPE("REPLICA_FACTOR", TokenType::KW_REPLICA_FACTOR),
        CHECK_SEMANTIC_TYPE("replica_factor", TokenType::KW_REPLICA_FACTOR),
        CHECK_SEMANTIC_TYPE("Replica_factor", TokenType::KW_REPLICA_FACTOR),
        CHECK_SEMANTIC_TYPE("DROP", TokenType::KW_DROP),
        CHECK_SEMANTIC_TYPE("drop", TokenType::KW_DROP),
        CHECK_SEMANTIC_TYPE("Drop", TokenType::KW_DROP),
        CHECK_SEMANTIC_TYPE("REMOVE", TokenType::KW_REMOVE),
        CHECK_SEMANTIC_TYPE("remove", TokenType::KW_REMOVE),
        CHECK_SEMANTIC_TYPE("Remove", TokenType::KW_REMOVE),

        CHECK_SEMANTIC_TYPE("_type", TokenType::TYPE_PROP),
        CHECK_SEMANTIC_TYPE("_id", TokenType::ID_PROP),
        CHECK_SEMANTIC_TYPE("_src", TokenType::SRC_ID_PROP),
        CHECK_SEMANTIC_TYPE("_dst", TokenType::DST_ID_PROP),
        CHECK_SEMANTIC_TYPE("_rank", TokenType::RANK_PROP),

        CHECK_SEMANTIC_VALUE("TRUE", TokenType::BOOL, true),
        CHECK_SEMANTIC_VALUE("true", TokenType::BOOL, true),
        CHECK_SEMANTIC_VALUE("FALSE", TokenType::BOOL, false),
        CHECK_SEMANTIC_VALUE("false", TokenType::BOOL, false),

        CHECK_SEMANTIC_VALUE("$var", TokenType::VARIABLE, "var"),
        CHECK_SEMANTIC_VALUE("$var123", TokenType::VARIABLE, "var123"),

        CHECK_SEMANTIC_VALUE("label", TokenType::LABEL, "label"),
        CHECK_SEMANTIC_VALUE("label123", TokenType::LABEL, "label123"),

        CHECK_SEMANTIC_VALUE("123", TokenType::INTEGER, 123),
        CHECK_SEMANTIC_VALUE("-123", TokenType::INTEGER, -123),
        CHECK_SEMANTIC_VALUE("0x123", TokenType::INTEGER, 0x123),
        CHECK_SEMANTIC_VALUE("0xdeadbeef", TokenType::INTEGER, 0xdeadbeef),
        CHECK_SEMANTIC_VALUE("0123", TokenType::INTEGER, 0123),
        CHECK_SEMANTIC_VALUE("123.", TokenType::DOUBLE, 123.),
        CHECK_SEMANTIC_VALUE(".123", TokenType::DOUBLE, 0.123),
        CHECK_SEMANTIC_VALUE("123.456", TokenType::DOUBLE, 123.456),
        CHECK_SEMANTIC_VALUE("+123.456", TokenType::DOUBLE, 123.456),
        CHECK_SEMANTIC_VALUE("-123.456", TokenType::DOUBLE, -123.456),

        CHECK_SEMANTIC_VALUE("\"Hello\"", TokenType::STRING, "Hello"),
        CHECK_SEMANTIC_VALUE("\"Hello\\\\\"", TokenType::STRING, "Hello\\"),
        CHECK_SEMANTIC_VALUE("\"He\\nllo\"", TokenType::STRING, "He\nllo"),
        CHECK_SEMANTIC_VALUE("\"He\\\nllo\"", TokenType::STRING, "He\nllo"),
        CHECK_SEMANTIC_VALUE("\"\\\"Hello\\\"\"", TokenType::STRING, "\"Hello\""),

        // escape Normal character
        CHECK_SEMANTIC_VALUE("\"Hell\\o\"", TokenType::STRING, "Hello"),
        CHECK_SEMANTIC_VALUE("\"Hell\\\\o\"", TokenType::STRING, "Hell\\o"),
        CHECK_SEMANTIC_VALUE("\"Hell\\\\\\o\"", TokenType::STRING, "Hell\\o"),
        CHECK_SEMANTIC_VALUE("\"\\110ello\"", TokenType::STRING, "Hello"),
        CHECK_SEMANTIC_VALUE("\"\110ello\"", TokenType::STRING, "Hello"),

        CHECK_SEMANTIC_VALUE("\"\110 \"", TokenType::STRING, "H "),
        CHECK_SEMANTIC_VALUE("\"\\110 \"", TokenType::STRING, "H "),
        CHECK_SEMANTIC_VALUE("\"\\\110 \"", TokenType::STRING, "H "),
        CHECK_SEMANTIC_VALUE("\"\\\\110 \"", TokenType::STRING, "\\110 "),
        CHECK_SEMANTIC_VALUE("\"\\\\\110 \"", TokenType::STRING, "\\H "),
        CHECK_SEMANTIC_VALUE("\"\\\\\\110 \"", TokenType::STRING, "\\H "),
        CHECK_SEMANTIC_VALUE("\"\\\\\\\110 \"", TokenType::STRING, "\\H "),
        CHECK_SEMANTIC_VALUE("\"\\\\\\\\110 \"", TokenType::STRING, "\\\\110 "),
        CHECK_SEMANTIC_VALUE("\"\\\\\\\\\110 \"", TokenType::STRING, "\\\\H "),
    };
#undef CHECK_SEMANTIC_TYPE
#undef CHECK_SEMANTIC_VALUE

    std::istringstream is(stream);
    scanner.switch_streams(&is, nullptr);

    for (auto &item : validators) {
        ASSERT_TRUE(item());
    }
}

}   // namespace nebula
