/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
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

#define CHECK_LEXICAL_ERROR(STR)                                            \
    ([] () {                                                                \
        auto input = [] (char *buf, int) -> int {                           \
            static bool first = true;                                       \
            if (!first) { return 0; }                                       \
            first = false;                                                  \
            auto size = ::strlen(STR);                                      \
            ::memcpy(buf, STR, size);                                       \
            return size;                                                    \
        };                                                                  \
        GraphScanner lexer;                                                 \
        lexer.setReadBuffer(input);                                         \
        nebula::GraphParser::semantic_type dumyyylval;                      \
        nebula::GraphParser::location_type dumyyyloc;                       \
        try {                                                               \
            auto token = lexer.yylex(&dumyyylval, &dumyyyloc);              \
            if (token != 0) {                                               \
                return AssertionFailure() << "Lexical error should've "     \
                                          << "happened for `" << STR << "'";\
            } else {                                                        \
                return AssertionSuccess();                                  \
            }                                                               \
        } catch (const std::exception &e) {                                 \
            LOG(INFO) << e.what() << STR;                                   \
            return AssertionSuccess();                                      \
        }                                                                   \
    })

    std::vector<Validator> validators = {
        CHECK_SEMANTIC_TYPE(".", TokenType::DOT),
        CHECK_SEMANTIC_TYPE("..", TokenType::DOT_DOT),
        CHECK_SEMANTIC_TYPE(",", TokenType::COMMA),
        CHECK_SEMANTIC_TYPE(":", TokenType::COLON),
        CHECK_SEMANTIC_TYPE(";", TokenType::SEMICOLON),
        CHECK_SEMANTIC_TYPE("+", TokenType::PLUS),
        CHECK_SEMANTIC_TYPE("-", TokenType::MINUS),
        CHECK_SEMANTIC_TYPE("*", TokenType::STAR),
        CHECK_SEMANTIC_TYPE("/", TokenType::DIV),
        CHECK_SEMANTIC_TYPE("%", TokenType::MOD),
        CHECK_SEMANTIC_TYPE("!", TokenType::NOT),
        CHECK_SEMANTIC_TYPE("@", TokenType::AT),
        CHECK_SEMANTIC_TYPE("?", TokenType::QM),

        CHECK_SEMANTIC_TYPE("<", TokenType::LT),
        CHECK_SEMANTIC_TYPE("<=", TokenType::LE),
        CHECK_SEMANTIC_TYPE(">", TokenType::GT),
        CHECK_SEMANTIC_TYPE(">=", TokenType::GE),
        CHECK_SEMANTIC_TYPE("==", TokenType::EQ),
        CHECK_SEMANTIC_TYPE("!=", TokenType::NE),
        CHECK_SEMANTIC_TYPE("<>", TokenType::NE),
        CHECK_SEMANTIC_TYPE("=~", TokenType::REG),

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
        CHECK_SEMANTIC_TYPE("VALUE", TokenType::KW_VALUES),
        CHECK_SEMANTIC_TYPE("value", TokenType::KW_VALUES),
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
        CHECK_SEMANTIC_TYPE("EDGES", TokenType::KW_EDGES),
        CHECK_SEMANTIC_TYPE("edges", TokenType::KW_EDGES),
        CHECK_SEMANTIC_TYPE("UPDATE", TokenType::KW_UPDATE),
        CHECK_SEMANTIC_TYPE("update", TokenType::KW_UPDATE),
        CHECK_SEMANTIC_TYPE("ALTER", TokenType::KW_ALTER),
        CHECK_SEMANTIC_TYPE("alter", TokenType::KW_ALTER),
        CHECK_SEMANTIC_TYPE("STEP", TokenType::KW_STEPS),
        CHECK_SEMANTIC_TYPE("step", TokenType::KW_STEPS),
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
        CHECK_SEMANTIC_TYPE("PARTS", TokenType::KW_PARTS),
        CHECK_SEMANTIC_TYPE("Parts", TokenType::KW_PARTS),
        CHECK_SEMANTIC_TYPE("parts", TokenType::KW_PARTS),
        CHECK_SEMANTIC_TYPE("DOUBLE", TokenType::KW_DOUBLE),
        CHECK_SEMANTIC_TYPE("double", TokenType::KW_DOUBLE),
        CHECK_SEMANTIC_TYPE("STRING", TokenType::KW_STRING),
        CHECK_SEMANTIC_TYPE("string", TokenType::KW_STRING),
        CHECK_SEMANTIC_TYPE("BOOL", TokenType::KW_BOOL),
        CHECK_SEMANTIC_TYPE("bool", TokenType::KW_BOOL),
        CHECK_SEMANTIC_TYPE("TAG", TokenType::KW_TAG),
        CHECK_SEMANTIC_TYPE("tag", TokenType::KW_TAG),
        CHECK_SEMANTIC_TYPE("TAGS", TokenType::KW_TAGS),
        CHECK_SEMANTIC_TYPE("tags", TokenType::KW_TAGS),
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
        CHECK_SEMANTIC_TYPE("HOST", TokenType::KW_HOST),
        CHECK_SEMANTIC_TYPE("host", TokenType::KW_HOST),
        CHECK_SEMANTIC_TYPE("Host", TokenType::KW_HOST),
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
        CHECK_SEMANTIC_TYPE("DESC", TokenType::KW_DESC),
        CHECK_SEMANTIC_TYPE("desc", TokenType::KW_DESC),
        CHECK_SEMANTIC_TYPE("Desc", TokenType::KW_DESC),
        CHECK_SEMANTIC_TYPE("DESCRIBE", TokenType::KW_DESCRIBE),
        CHECK_SEMANTIC_TYPE("describe", TokenType::KW_DESCRIBE),
        CHECK_SEMANTIC_TYPE("Describe", TokenType::KW_DESCRIBE),
        CHECK_SEMANTIC_TYPE("REMOVE", TokenType::KW_REMOVE),
        CHECK_SEMANTIC_TYPE("remove", TokenType::KW_REMOVE),
        CHECK_SEMANTIC_TYPE("Remove", TokenType::KW_REMOVE),
        CHECK_SEMANTIC_TYPE("IF", TokenType::KW_IF),
        CHECK_SEMANTIC_TYPE("If", TokenType::KW_IF),
        CHECK_SEMANTIC_TYPE("if", TokenType::KW_IF),
        CHECK_SEMANTIC_TYPE("NOT", TokenType::KW_NOT),
        CHECK_SEMANTIC_TYPE("Not", TokenType::KW_NOT),
        CHECK_SEMANTIC_TYPE("not", TokenType::KW_NOT),
        CHECK_SEMANTIC_TYPE("OR", TokenType::KW_OR),
        CHECK_SEMANTIC_TYPE("Or", TokenType::KW_OR),
        CHECK_SEMANTIC_TYPE("or", TokenType::KW_OR),
        CHECK_SEMANTIC_TYPE("AND", TokenType::KW_AND),
        CHECK_SEMANTIC_TYPE("And", TokenType::KW_AND),
        CHECK_SEMANTIC_TYPE("and", TokenType::KW_AND),
        CHECK_SEMANTIC_TYPE("XOR", TokenType::KW_XOR),
        CHECK_SEMANTIC_TYPE("Xor", TokenType::KW_XOR),
        CHECK_SEMANTIC_TYPE("xor", TokenType::KW_XOR),
        CHECK_SEMANTIC_TYPE("EXISTS", TokenType::KW_EXISTS),
        CHECK_SEMANTIC_TYPE("Exists", TokenType::KW_EXISTS),
        CHECK_SEMANTIC_TYPE("exists", TokenType::KW_EXISTS),
        CHECK_SEMANTIC_TYPE("WITH", TokenType::KW_WITH),
        CHECK_SEMANTIC_TYPE("With", TokenType::KW_WITH),
        CHECK_SEMANTIC_TYPE("with", TokenType::KW_WITH),
        CHECK_SEMANTIC_TYPE("USER", TokenType::KW_USER),
        CHECK_SEMANTIC_TYPE("User", TokenType::KW_USER),
        CHECK_SEMANTIC_TYPE("user", TokenType::KW_USER),
        CHECK_SEMANTIC_TYPE("USERS", TokenType::KW_USERS),
        CHECK_SEMANTIC_TYPE("Users", TokenType::KW_USERS),
        CHECK_SEMANTIC_TYPE("users", TokenType::KW_USERS),
        CHECK_SEMANTIC_TYPE("PASSWORD", TokenType::KW_PASSWORD),
        CHECK_SEMANTIC_TYPE("Password", TokenType::KW_PASSWORD),
        CHECK_SEMANTIC_TYPE("password", TokenType::KW_PASSWORD),
        CHECK_SEMANTIC_TYPE("CHANGE", TokenType::KW_CHANGE),
        CHECK_SEMANTIC_TYPE("Change", TokenType::KW_CHANGE),
        CHECK_SEMANTIC_TYPE("change", TokenType::KW_CHANGE),
        CHECK_SEMANTIC_TYPE("ROLE", TokenType::KW_ROLE),
        CHECK_SEMANTIC_TYPE("Role", TokenType::KW_ROLE),
        CHECK_SEMANTIC_TYPE("role", TokenType::KW_ROLE),
        CHECK_SEMANTIC_TYPE("GOD", TokenType::KW_GOD),
        CHECK_SEMANTIC_TYPE("God", TokenType::KW_GOD),
        CHECK_SEMANTIC_TYPE("god", TokenType::KW_GOD),
        CHECK_SEMANTIC_TYPE("ADMIN", TokenType::KW_ADMIN),
        CHECK_SEMANTIC_TYPE("Admin", TokenType::KW_ADMIN),
        CHECK_SEMANTIC_TYPE("admin", TokenType::KW_ADMIN),
        CHECK_SEMANTIC_TYPE("GUEST", TokenType::KW_GUEST),
        CHECK_SEMANTIC_TYPE("Guest", TokenType::KW_GUEST),
        CHECK_SEMANTIC_TYPE("guest", TokenType::KW_GUEST),
        CHECK_SEMANTIC_TYPE("GRANT", TokenType::KW_GRANT),
        CHECK_SEMANTIC_TYPE("Grant", TokenType::KW_GRANT),
        CHECK_SEMANTIC_TYPE("grant", TokenType::KW_GRANT),
        CHECK_SEMANTIC_TYPE("REVOKE", TokenType::KW_REVOKE),
        CHECK_SEMANTIC_TYPE("Revoke", TokenType::KW_REVOKE),
        CHECK_SEMANTIC_TYPE("revoke", TokenType::KW_REVOKE),
        CHECK_SEMANTIC_TYPE("ON", TokenType::KW_ON),
        CHECK_SEMANTIC_TYPE("On", TokenType::KW_ON),
        CHECK_SEMANTIC_TYPE("on", TokenType::KW_ON),
        CHECK_SEMANTIC_TYPE("ROLES", TokenType::KW_ROLES),
        CHECK_SEMANTIC_TYPE("Roles", TokenType::KW_ROLES),
        CHECK_SEMANTIC_TYPE("BY", TokenType::KW_BY),
        CHECK_SEMANTIC_TYPE("By", TokenType::KW_BY),
        CHECK_SEMANTIC_TYPE("by", TokenType::KW_BY),
        CHECK_SEMANTIC_TYPE("IN", TokenType::KW_IN),
        CHECK_SEMANTIC_TYPE("In", TokenType::KW_IN),
        CHECK_SEMANTIC_TYPE("TTL_DURATION", TokenType::KW_TTL_DURATION),
        CHECK_SEMANTIC_TYPE("ttl_duration", TokenType::KW_TTL_DURATION),
        CHECK_SEMANTIC_TYPE("Ttl_duration", TokenType::KW_TTL_DURATION),
        CHECK_SEMANTIC_TYPE("TTL_COL", TokenType::KW_TTL_COL),
        CHECK_SEMANTIC_TYPE("ttl_col", TokenType::KW_TTL_COL),
        CHECK_SEMANTIC_TYPE("Ttl_col", TokenType::KW_TTL_COL),
        CHECK_SEMANTIC_TYPE("DOWNLOAD", TokenType::KW_DOWNLOAD),
        CHECK_SEMANTIC_TYPE("download", TokenType::KW_DOWNLOAD),
        CHECK_SEMANTIC_TYPE("Download", TokenType::KW_DOWNLOAD),
        CHECK_SEMANTIC_TYPE("HDFS", TokenType::KW_HDFS),
        CHECK_SEMANTIC_TYPE("Hdfs", TokenType::KW_HDFS),
        CHECK_SEMANTIC_TYPE("hdfs", TokenType::KW_HDFS),
        CHECK_SEMANTIC_TYPE("ORDER", TokenType::KW_ORDER),
        CHECK_SEMANTIC_TYPE("Order", TokenType::KW_ORDER),
        CHECK_SEMANTIC_TYPE("order", TokenType::KW_ORDER),
        CHECK_SEMANTIC_TYPE("ASC", TokenType::KW_ASC),
        CHECK_SEMANTIC_TYPE("Asc", TokenType::KW_ASC),
        CHECK_SEMANTIC_TYPE("asc", TokenType::KW_ASC),
        CHECK_SEMANTIC_TYPE("DISTINCT", TokenType::KW_DISTINCT),
        CHECK_SEMANTIC_TYPE("Distinct", TokenType::KW_DISTINCT),
        CHECK_SEMANTIC_TYPE("distinct", TokenType::KW_DISTINCT),
        CHECK_SEMANTIC_TYPE("DEFAULT", TokenType::KW_DEFAULT),
        CHECK_SEMANTIC_TYPE("Default", TokenType::KW_DEFAULT),
        CHECK_SEMANTIC_TYPE("default", TokenType::KW_DEFAULT),
        CHECK_SEMANTIC_TYPE("INGEST", TokenType::KW_INGEST),
        CHECK_SEMANTIC_TYPE("Ingest", TokenType::KW_INGEST),
        CHECK_SEMANTIC_TYPE("ingest", TokenType::KW_INGEST),
        CHECK_SEMANTIC_TYPE("CONFIGS", TokenType::KW_CONFIGS),
        CHECK_SEMANTIC_TYPE("configs", TokenType::KW_CONFIGS),
        CHECK_SEMANTIC_TYPE("Configs", TokenType::KW_CONFIGS),
        CHECK_SEMANTIC_TYPE("ALL", TokenType::KW_ALL),
        CHECK_SEMANTIC_TYPE("all", TokenType::KW_ALL),
        CHECK_SEMANTIC_TYPE("BALANCE", TokenType::KW_BALANCE),
        CHECK_SEMANTIC_TYPE("Balance", TokenType::KW_BALANCE),
        CHECK_SEMANTIC_TYPE("balance", TokenType::KW_BALANCE),
        CHECK_SEMANTIC_TYPE("LEADER", TokenType::KW_LEADER),
        CHECK_SEMANTIC_TYPE("Leader", TokenType::KW_LEADER),
        CHECK_SEMANTIC_TYPE("leader", TokenType::KW_LEADER),
        CHECK_SEMANTIC_TYPE("RESET", TokenType::KW_RESET),
        CHECK_SEMANTIC_TYPE("reset", TokenType::KW_RESET),
        CHECK_SEMANTIC_TYPE("Reset", TokenType::KW_RESET),
        CHECK_SEMANTIC_TYPE("PLAN", TokenType::KW_PLAN),
        CHECK_SEMANTIC_TYPE("plan", TokenType::KW_PLAN),
        CHECK_SEMANTIC_TYPE("Plan", TokenType::KW_PLAN),
        CHECK_SEMANTIC_TYPE("FETCH", TokenType::KW_FETCH),
        CHECK_SEMANTIC_TYPE("Fetch", TokenType::KW_FETCH),
        CHECK_SEMANTIC_TYPE("fetch", TokenType::KW_FETCH),
        CHECK_SEMANTIC_TYPE("UUID", TokenType::KW_UUID),
        CHECK_SEMANTIC_TYPE("Uuid", TokenType::KW_UUID),
        CHECK_SEMANTIC_TYPE("uuid", TokenType::KW_UUID),
        CHECK_SEMANTIC_TYPE("OF", TokenType::KW_OF),
        CHECK_SEMANTIC_TYPE("Of", TokenType::KW_OF),
        CHECK_SEMANTIC_TYPE("of", TokenType::KW_OF),
        CHECK_SEMANTIC_TYPE("LIMIT", TokenType::KW_LIMIT),
        CHECK_SEMANTIC_TYPE("limit", TokenType::KW_LIMIT),
        CHECK_SEMANTIC_TYPE("OFFSET", TokenType::KW_OFFSET),
        CHECK_SEMANTIC_TYPE("offset", TokenType::KW_OFFSET),
        CHECK_SEMANTIC_TYPE("SNAPSHOT", TokenType::KW_SNAPSHOT),
        CHECK_SEMANTIC_TYPE("Snapshot", TokenType::KW_SNAPSHOT),
        CHECK_SEMANTIC_TYPE("snapshot", TokenType::KW_SNAPSHOT),
        CHECK_SEMANTIC_TYPE("SNAPSHOTS", TokenType::KW_SNAPSHOTS),
        CHECK_SEMANTIC_TYPE("Snapshots", TokenType::KW_SNAPSHOTS),
        CHECK_SEMANTIC_TYPE("snapshots", TokenType::KW_SNAPSHOTS),
        CHECK_SEMANTIC_TYPE("SHORTEST", TokenType::KW_SHORTEST),
        CHECK_SEMANTIC_TYPE("Shortest", TokenType::KW_SHORTEST),
        CHECK_SEMANTIC_TYPE("shortest", TokenType::KW_SHORTEST),
        CHECK_SEMANTIC_TYPE("SUBGRAPH", TokenType::KW_SUBGRAPH),
        CHECK_SEMANTIC_TYPE("Subgraph", TokenType::KW_SUBGRAPH),
        CHECK_SEMANTIC_TYPE("subgraph", TokenType::KW_SUBGRAPH),
        CHECK_SEMANTIC_TYPE("CONTAINS", TokenType::KW_CONTAINS),
        CHECK_SEMANTIC_TYPE("Contains", TokenType::KW_CONTAINS),
        CHECK_SEMANTIC_TYPE("contains", TokenType::KW_CONTAINS),
        CHECK_SEMANTIC_TYPE("STARTS", TokenType::KW_STARTS),
        CHECK_SEMANTIC_TYPE("Starts", TokenType::KW_STARTS),
        CHECK_SEMANTIC_TYPE("starts", TokenType::KW_STARTS),
        CHECK_SEMANTIC_TYPE("ENDS", TokenType::KW_ENDS),
        CHECK_SEMANTIC_TYPE("Ends", TokenType::KW_ENDS),
        CHECK_SEMANTIC_TYPE("ends", TokenType::KW_ENDS),
        CHECK_SEMANTIC_TYPE("STARTS WITH", TokenType::KW_STARTS_WITH),
        CHECK_SEMANTIC_TYPE("Starts with", TokenType::KW_STARTS_WITH),
        CHECK_SEMANTIC_TYPE("starts with", TokenType::KW_STARTS_WITH),
        CHECK_SEMANTIC_TYPE("ENDS WITH", TokenType::KW_ENDS_WITH),
        CHECK_SEMANTIC_TYPE("Ends with", TokenType::KW_ENDS_WITH),
        CHECK_SEMANTIC_TYPE("ends with", TokenType::KW_ENDS_WITH),
        CHECK_SEMANTIC_TYPE("OUT", TokenType::KW_OUT),
        CHECK_SEMANTIC_TYPE("Out", TokenType::KW_OUT),
        CHECK_SEMANTIC_TYPE("out", TokenType::KW_OUT),
        CHECK_SEMANTIC_TYPE("BOTH", TokenType::KW_BOTH),
        CHECK_SEMANTIC_TYPE("Both", TokenType::KW_BOTH),
        CHECK_SEMANTIC_TYPE("both", TokenType::KW_BOTH),
        CHECK_SEMANTIC_TYPE("GRAPH", TokenType::KW_GRAPH),
        CHECK_SEMANTIC_TYPE("Graph", TokenType::KW_GRAPH),
        CHECK_SEMANTIC_TYPE("graph", TokenType::KW_GRAPH),
        CHECK_SEMANTIC_TYPE("META", TokenType::KW_META),
        CHECK_SEMANTIC_TYPE("Meta", TokenType::KW_META),
        CHECK_SEMANTIC_TYPE("meta", TokenType::KW_META),
        CHECK_SEMANTIC_TYPE("STORAGE", TokenType::KW_STORAGE),
        CHECK_SEMANTIC_TYPE("Storage", TokenType::KW_STORAGE),
        CHECK_SEMANTIC_TYPE("storage", TokenType::KW_STORAGE),
        CHECK_SEMANTIC_TYPE("UNWIND", TokenType::KW_UNWIND),
        CHECK_SEMANTIC_TYPE("unwind", TokenType::KW_UNWIND),
        CHECK_SEMANTIC_TYPE("SKIP", TokenType::KW_SKIP),
        CHECK_SEMANTIC_TYPE("skip", TokenType::KW_SKIP),
        CHECK_SEMANTIC_TYPE("OPTIONAL", TokenType::KW_OPTIONAL),
        CHECK_SEMANTIC_TYPE("optional", TokenType::KW_OPTIONAL),
        CHECK_SEMANTIC_TYPE("DATE", TokenType::KW_DATE),
        CHECK_SEMANTIC_TYPE("date", TokenType::KW_DATE),
        CHECK_SEMANTIC_TYPE("TIME", TokenType::KW_TIME),
        CHECK_SEMANTIC_TYPE("time", TokenType::KW_TIME),
        CHECK_SEMANTIC_TYPE("DATETIME", TokenType::KW_DATETIME),
        CHECK_SEMANTIC_TYPE("datetime", TokenType::KW_DATETIME),
        CHECK_SEMANTIC_TYPE("GROUP", TokenType::KW_GROUP),
        CHECK_SEMANTIC_TYPE("Group", TokenType::KW_GROUP),
        CHECK_SEMANTIC_TYPE("group", TokenType::KW_GROUP),
        CHECK_SEMANTIC_TYPE("GROUPS", TokenType::KW_GROUPS),
        CHECK_SEMANTIC_TYPE("Groups", TokenType::KW_GROUPS),
        CHECK_SEMANTIC_TYPE("groups", TokenType::KW_GROUPS),
        CHECK_SEMANTIC_TYPE("ZONE", TokenType::KW_ZONE),
        CHECK_SEMANTIC_TYPE("Zone", TokenType::KW_ZONE),
        CHECK_SEMANTIC_TYPE("zone", TokenType::KW_ZONE),
        CHECK_SEMANTIC_TYPE("ZONES", TokenType::KW_ZONES),
        CHECK_SEMANTIC_TYPE("Zones", TokenType::KW_ZONES),
        CHECK_SEMANTIC_TYPE("zones", TokenType::KW_ZONES),
        CHECK_SEMANTIC_TYPE("INTO", TokenType::KW_INTO),
        CHECK_SEMANTIC_TYPE("Into", TokenType::KW_INTO),
        CHECK_SEMANTIC_TYPE("into", TokenType::KW_INTO),
        CHECK_SEMANTIC_TYPE("STATS", TokenType::KW_STATS),
        CHECK_SEMANTIC_TYPE("Stats", TokenType::KW_STATS),
        CHECK_SEMANTIC_TYPE("stats", TokenType::KW_STATS),
        CHECK_SEMANTIC_TYPE("ANY", TokenType::KW_ANY),
        CHECK_SEMANTIC_TYPE("any", TokenType::KW_ANY),
        CHECK_SEMANTIC_TYPE("SINGLE", TokenType::KW_SINGLE),
        CHECK_SEMANTIC_TYPE("single", TokenType::KW_SINGLE),
        CHECK_SEMANTIC_TYPE("NONE", TokenType::KW_NONE),
        CHECK_SEMANTIC_TYPE("none", TokenType::KW_NONE),
        CHECK_SEMANTIC_TYPE("CASE", TokenType::KW_CASE),
        CHECK_SEMANTIC_TYPE("case", TokenType::KW_CASE),
        CHECK_SEMANTIC_TYPE("WHEN", TokenType::KW_WHEN),
        CHECK_SEMANTIC_TYPE("when", TokenType::KW_WHEN),
        CHECK_SEMANTIC_TYPE("THEN", TokenType::KW_THEN),
        CHECK_SEMANTIC_TYPE("then", TokenType::KW_THEN),
        CHECK_SEMANTIC_TYPE("ELSE", TokenType::KW_ELSE),
        CHECK_SEMANTIC_TYPE("else", TokenType::KW_ELSE),
        CHECK_SEMANTIC_TYPE("END", TokenType::KW_END),
        CHECK_SEMANTIC_TYPE("end", TokenType::KW_END),
        CHECK_SEMANTIC_TYPE("REDUCE", TokenType::KW_REDUCE),
        CHECK_SEMANTIC_TYPE("reduce", TokenType::KW_REDUCE),
        CHECK_SEMANTIC_TYPE("SESSIONS", TokenType::KW_SESSIONS),
        CHECK_SEMANTIC_TYPE("Sessions", TokenType::KW_SESSIONS),
        CHECK_SEMANTIC_TYPE("sessions", TokenType::KW_SESSIONS),
        CHECK_SEMANTIC_TYPE("SESSION", TokenType::KW_SESSION),
        CHECK_SEMANTIC_TYPE("Session", TokenType::KW_SESSION),
        CHECK_SEMANTIC_TYPE("session", TokenType::KW_SESSION),
        CHECK_SEMANTIC_TYPE("QUERY", TokenType::KW_QUERY),
        CHECK_SEMANTIC_TYPE("Query", TokenType::KW_QUERY),
        CHECK_SEMANTIC_TYPE("query", TokenType::KW_QUERY),
        CHECK_SEMANTIC_TYPE("QUERIES", TokenType::KW_QUERIES),
        CHECK_SEMANTIC_TYPE("Queries", TokenType::KW_QUERIES),
        CHECK_SEMANTIC_TYPE("queries", TokenType::KW_QUERIES),
        CHECK_SEMANTIC_TYPE("KILL", TokenType::KW_KILL),
        CHECK_SEMANTIC_TYPE("Kill", TokenType::KW_KILL),
        CHECK_SEMANTIC_TYPE("kill", TokenType::KW_KILL),
        CHECK_SEMANTIC_TYPE("TOP", TokenType::KW_TOP),
        CHECK_SEMANTIC_TYPE("Top", TokenType::KW_TOP),
        CHECK_SEMANTIC_TYPE("top", TokenType::KW_TOP),

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
        CHECK_SEMANTIC_VALUE("0x123", TokenType::INTEGER, 0x123),
        CHECK_SEMANTIC_VALUE("0xdeadbeef", TokenType::INTEGER, 0xdeadbeef),
        CHECK_SEMANTIC_VALUE("0123", TokenType::INTEGER, 0123),
        CHECK_SEMANTIC_VALUE("123.", TokenType::DOUBLE, 123.),
        CHECK_SEMANTIC_VALUE(".123", TokenType::DOUBLE, 0.123),
        CHECK_SEMANTIC_VALUE("123.456", TokenType::DOUBLE, 123.456),

        CHECK_SEMANTIC_VALUE("0x7FFFFFFFFFFFFFFF", TokenType::INTEGER, 0x7FFFFFFFFFFFFFFFL),
        CHECK_SEMANTIC_VALUE("0x007FFFFFFFFFFFFFFF", TokenType::INTEGER, 0x007FFFFFFFFFFFFFFFL),
        CHECK_SEMANTIC_VALUE("9223372036854775807", TokenType::INTEGER, 9223372036854775807L),
        CHECK_SEMANTIC_VALUE(
            "00777777777777777777777", TokenType::INTEGER, 00777777777777777777777),
        CHECK_LEXICAL_ERROR("9223372036854775809"),
        CHECK_LEXICAL_ERROR("0x8000000000000001"),
        CHECK_LEXICAL_ERROR("001000000000000000000001"),
        // TODO(dutor) It's too tedious to paste an overflowed double number here,
        // thus we rely on `folly::to<double>' to cover those cases for us.

        CHECK_SEMANTIC_VALUE("127.0.0.1", TokenType::IPV4, "127.0.0.1"),

        CHECK_SEMANTIC_VALUE("\"Hello\"", TokenType::STRING, "Hello"),
        CHECK_SEMANTIC_VALUE("\"Hello\\\\\"", TokenType::STRING, "Hello\\"),
        CHECK_SEMANTIC_VALUE("\"He\\nllo\"", TokenType::STRING, "He\nllo"),
        CHECK_SEMANTIC_VALUE("\"He\\\nllo\"", TokenType::STRING, "He\nllo"),
        CHECK_SEMANTIC_VALUE("\"\\\"Hello\\\"\"", TokenType::STRING, "\"Hello\""),

        CHECK_SEMANTIC_VALUE("'Hello'", TokenType::STRING, "Hello"),
        CHECK_SEMANTIC_VALUE("'\"Hello\"'", TokenType::STRING, "\"Hello\""),
        CHECK_SEMANTIC_VALUE("'\\'Hello\\''", TokenType::STRING, "'Hello'"),

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

        CHECK_SEMANTIC_VALUE("\"己所不欲，勿施于人\"", TokenType::STRING, "己所不欲，勿施于人"),
    };
#undef CHECK_SEMANTIC_TYPE
#undef CHECK_SEMANTIC_VALUE

    auto input = [&] (char *buf, int maxSize) {
        static int copied = 0;
        int left = stream.size() - copied;
        if (left == 0) {
            return 0;
        }
        int n = left < maxSize ? left : maxSize;
        ::memcpy(buf, &stream[copied], n);
        copied += n;
        return n;
    };
    scanner.setReadBuffer(input);

    for (auto &item : validators) {
        ASSERT_TRUE(item());
    }
}

}   // namespace nebula
