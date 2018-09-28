/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <sstream>
#include <vector>
#include <utility>
#include "parser/VGraphParser.hpp"
#include "parser/VGraphScanner.h"

// TODO(dutor) Check on the sematic value of tokens

namespace vesoft {

TEST(Scanner, Basic) {
    using TokenType = vesoft::VGraphParser::token::yytokentype;
    VGraphScanner scanner;
    vesoft::VGraphParser::semantic_type yylval;
    vesoft::VGraphParser::location_type yyloc;
    std::vector<std::pair<std::string, TokenType>> token_mappings = {
        {" . ",             TokenType::DOT},
        {" , ",             TokenType::COMMA},
        {" : ",             TokenType::COLON},
        {" ; ",             TokenType::SEMICOLON},
        {" + ",             TokenType::ADD},
        {" - ",             TokenType::SUB},
        {" * ",             TokenType::MUL},
        {" / ",             TokenType::DIV},
        {" % ",             TokenType::MOD},
        {" @ ",             TokenType::AT},

        {" < ",             TokenType::LT},
        {" <= ",            TokenType::LE},
        {" > ",             TokenType::GT},
        {" >= ",            TokenType::GE},
        {" == ",            TokenType::EQ},
        {" != ",            TokenType::NE},

        {" || ",            TokenType::OR},
        {" && ",            TokenType::AND},
        {" | ",             TokenType::PIPE},

        {" = ",             TokenType::ASSIGN},

        {" ( ",             TokenType::L_PAREN},
        {" ) ",             TokenType::R_PAREN},
        {" [ ",             TokenType::L_BRACKET},
        {" ] ",             TokenType::R_BRACKET},
        {" { ",             TokenType::L_BRACE},
        {" } ",             TokenType::R_BRACE},

        {" <- ",             TokenType::L_ARROW},
        {" -> ",             TokenType::R_ARROW},

        {" GO ",            TokenType::KW_GO},
        {" go ",            TokenType::KW_GO},
        {" AS ",            TokenType::KW_AS},
        {" as ",            TokenType::KW_AS},
        {" TO ",            TokenType::KW_TO},
        {" to ",            TokenType::KW_TO},
        {" OR ",            TokenType::KW_OR},
        {" or ",            TokenType::KW_OR},
        {" USE ",           TokenType::KW_USE},
        {" use ",           TokenType::KW_USE},
        {" SET ",           TokenType::KW_SET},
        {" set ",           TokenType::KW_SET},
        {" FROM ",          TokenType::KW_FROM},
        {" from ",          TokenType::KW_FROM},
        {" WHERE ",         TokenType::KW_WHERE},
        {" where ",         TokenType::KW_WHERE},
        {" MATCH ",         TokenType::KW_MATCH},
        {" match ",         TokenType::KW_MATCH},
        {" INSERT ",        TokenType::KW_INSERT},
        {" insert ",        TokenType::KW_INSERT},
        {" VALUES ",        TokenType::KW_VALUES},
        {" values ",        TokenType::KW_VALUES},
        {" RETURN ",        TokenType::KW_RETURN},
        {" return ",        TokenType::KW_RETURN},
        {" DEFINE ",        TokenType::KW_DEFINE},
        {" define ",        TokenType::KW_DEFINE},
        {" VERTEX ",        TokenType::KW_VERTEX},
        {" vertex ",        TokenType::KW_VERTEX},
        {" EDGE ",          TokenType::KW_EDGE},
        {" edge ",          TokenType::KW_EDGE},
        {" UPDATE ",        TokenType::KW_UPDATE},
        {" update ",        TokenType::KW_UPDATE},
        {" ALTER ",         TokenType::KW_ALTER},
        {" alter ",         TokenType::KW_ALTER},
        {" STEPS ",         TokenType::KW_STEPS},
        {" steps ",         TokenType::KW_STEPS},
        {" OVER ",          TokenType::KW_OVER},
        {" over ",          TokenType::KW_OVER},
        {" UPTO ",          TokenType::KW_UPTO},
        {" upto ",          TokenType::KW_UPTO},
        {" REVERSELY ",     TokenType::KW_REVERSELY},
        {" reversely ",     TokenType::KW_REVERSELY},
        {" NAMESPACE ",     TokenType::KW_NAMESPACE},
        {" namespace ",     TokenType::KW_NAMESPACE},
        {" TTL ",           TokenType::KW_TTL},
        {" ttl ",           TokenType::KW_TTL},
        {" INT8 ",          TokenType::KW_INT8},
        {" int8 ",          TokenType::KW_INT8},
        {" INT16 ",         TokenType::KW_INT16},
        {" int16 ",         TokenType::KW_INT16},
        {" INT32 ",         TokenType::KW_INT32},
        {" int32 ",         TokenType::KW_INT32},
        {" INT64 ",         TokenType::KW_INT64},
        {" int64 ",         TokenType::KW_INT64},
        {" UINT8 ",         TokenType::KW_UINT8},
        {" uint8 ",         TokenType::KW_UINT8},
        {" UINT16 ",        TokenType::KW_UINT16},
        {" uint16 ",        TokenType::KW_UINT16},
        {" UINT32 ",        TokenType::KW_UINT32},
        {" uint32 ",        TokenType::KW_UINT32},
        {" UINT64 ",        TokenType::KW_UINT64},
        {" uint64 ",        TokenType::KW_UINT64},
        {" BIGINT ",        TokenType::KW_BIGINT},
        {" bigint ",        TokenType::KW_BIGINT},
        {" DOUBLE ",        TokenType::KW_DOUBLE},
        {" double ",        TokenType::KW_DOUBLE},
        {" STRING ",        TokenType::KW_STRING},
        {" string ",        TokenType::KW_STRING},
        {" BOOL ",          TokenType::KW_BOOL},
        {" bool ",          TokenType::KW_BOOL},
        {" TAG ",           TokenType::KW_TAG},
        {" tag ",           TokenType::KW_TAG},
        {" UNION ",         TokenType::KW_UNION},
        {" union ",         TokenType::KW_UNION},
        {" INTERSECT ",     TokenType::KW_INTERSECT},
        {" intersect ",     TokenType::KW_INTERSECT},
        {" MINUS ",         TokenType::KW_MINUS},
        {" minus ",         TokenType::KW_MINUS},

        {" v ",             TokenType::SYMBOL},
        {" v1 ",            TokenType::SYMBOL},
        {" var ",           TokenType::SYMBOL},
        {" _var ",          TokenType::SYMBOL},
        {" var123 ",        TokenType::SYMBOL},
        {" _var123 ",       TokenType::SYMBOL},

        {" 123 ",           TokenType::INTEGER},
        {" 0x123 ",         TokenType::INTEGER},
        {" 0Xdeadbeef ",    TokenType::INTEGER},
        {" 0123 ",          TokenType::INTEGER},
        {" 123u ",          TokenType::UINTEGER},
        {" 123UL ",         TokenType::UINTEGER},

        {" .456 ",          TokenType::DOUBLE},
        {" 123.",           TokenType::DOUBLE},
        {" 123.456 ",       TokenType::DOUBLE},

        {" $1 ",            TokenType::COL_REF_ID},
        {" $123 ",            TokenType::COL_REF_ID},

        {" $_ ",            TokenType::VARIABLE},
        {" $var ",            TokenType::VARIABLE},

        {"\"Hello\"",       TokenType::STRING},     // "Hello"   ==> Hello
        {"\"He\\nllo\"",    TokenType::STRING},     // "He\nllo" ==> He
                                                    //               llo
        {"\"He\\\nllo\"",    TokenType::STRING},    // "He\nllo" ==> He
                                                    //               llo
        {"\"Hell\\o\"",     TokenType::STRING},     // "Hello"   ==> Hello
        {"\"Hello\\\\\"",   TokenType::STRING},     // "Hello\\" ==> Hello\     //
        {"\"\\110ello\"",   TokenType::STRING},     // "Hello"   ==> Hello
        {"\"\\\"Hello\\\"\"",   TokenType::STRING}, // "\"Hello\""   ==> "Hello"
    };

    std::string token_stream;
    for (auto &pair : token_mappings) {
        token_stream += pair.first;
    }

    std::istringstream is(token_stream);

    scanner.switch_streams(&is, nullptr);

    for (auto &pair : token_mappings) {
        auto &token = pair.first;
        auto expected_type = static_cast<int>(pair.second);
        auto actual_type = scanner.yylex(&yylval, &yyloc);

        ASSERT_EQ(expected_type, actual_type) << "Lex error for `" << token <<"'";
    }
}

}   // namespace vesoft
