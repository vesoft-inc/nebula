%option c++
%option yyclass="GraphScanner"
%option nodefault noyywrap
%option never-interactive
%option yylineno

%{
#include "parser/GQLParser.h"
#include "parser/GraphScanner.h"
#include "GraphParser.hpp"

#define YY_USER_ACTION  yylloc->columns(yyleng);

using TokenType = nebula::GraphParser::token;

static constexpr size_t MAX_STRING = 4096;


%}

%x STR

GO                          ([Gg][Oo])
AS                          ([Aa][Ss])
TO                          ([Tt][Oo])
OR                          ([Oo][Rr])
USE                         ([Uu][Ss][Ee])
SET                         ([Ss][Ee][Tt])
FROM                        ([Ff][Rr][Oo][Mm])
WHERE                       ([Ww][Hh][Ee][Rr][Ee])
MATCH                       ([Mm][Aa][Tt][Cc][Hh])
INSERT                      ([Ii][Nn][Ss][Ee][Rr][Tt])
VALUES                      ([Vv][Aa][Ll][Uu][Ee][Ss])
YIELD                       ([Yy][Ii][Ee][Ll][Dd])
RETURN                      ([Rr][Ee][Tt][Uu][Rr][Nn])
DEFINE                      ([Dd][Ee][Ff][Ii][Nn][Ee])
DESCRIBE                    ([Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee])
VERTEX                      ([Vv][Ee][Rr][Tt][Ee][Xx])
EDGE                        ([Ee][Dd][Gg][Ee])
UPDATE                      ([Uu][Pp][Dd][Aa][Tt][Ee])
ALTER                       ([Aa][Ll][Tt][Ee][Rr])
STEPS                       ([Ss][Tt][Ee][Pp][Ss])
OVER                        ([Oo][Vv][Ee][Rr])
UPTO                        ([Uu][Pp][Tt][Oo])
REVERSELY                   ([Rr][Ee][Vv][Ee][Rr][Ss][Ee][Ll][Yy])
SPACE                       ([Ss][Pp][Aa][Cc][Ee])
TTL                         ([Tt][Tt][Ll])
INT                         ([Ii][Nn][Tt])
INT8                        ({INT}8)
INT16                       ({INT}16)
INT32                       ({INT}32)
INT64                       ({INT}64)
UINT                        ([Uu][Ii][Nn][Tt])
UINT8                       ({UINT}8)
UINT16                      ({UINT}16)
UINT32                      ({UINT}32)
UINT64                      ({UINT}64)
BIGINT                      ([Bb][Ii][Gg][Ii][Nn][Tt])
DOUBLE                      ([Dd][Oo][Uu][Bb][Ll][Ee])
STRING                      ([Ss][Tt][Rr][Ii][Nn][Gg])
BOOL                        ([Bb][Oo][Oo][Ll])
TAG                         ([Tt][Aa][Gg])
UNION                       ([Uu][Nn][Ii][Oo][Nn])
INTERSECT                   ([Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt])
MINUS                       ([Mm][Ii][Nn][Uu][Ss])
NO                          ([Nn][Oo])
OVERWRITE                   ([Oo][Vv][Ee][Rr][Ww][Rr][Ii][Tt][Ee])
TRUE                        ([Tt][Rr][Uu][Ee])
FALSE                       ([Ff][Aa][Ll][Ss][Ee])

LABEL                       ([a-zA-Z][_a-zA-Z0-9]*)
DEC                         ([0-9])
HEX                         ([0-9a-fA-F])
OCT                         ([0-7])


%%

                            thread_local static char sbuf[MAX_STRING];
                            size_t pos = 0;

{GO}                        { return TokenType::KW_GO; }
{AS}                        { return TokenType::KW_AS; }
{TO}                        { return TokenType::KW_TO; }
{OR}                        { return TokenType::KW_OR; }
{USE}                       { return TokenType::KW_USE; }
{SET}                       { return TokenType::KW_SET; }
{FROM}                      { return TokenType::KW_FROM; }
{WHERE}                     { return TokenType::KW_WHERE; }
{MATCH}                     { return TokenType::KW_MATCH; }
{INSERT}                    { return TokenType::KW_INSERT; }
{VALUES}                    { return TokenType::KW_VALUES; }
{YIELD}                     { return TokenType::KW_YIELD; }
{RETURN}                    { return TokenType::KW_RETURN; }
{DEFINE}                    { return TokenType::KW_DEFINE; }
{DESCRIBE}                  { return TokenType::KW_DESCRIBE; }
{VERTEX}                    { return TokenType::KW_VERTEX; }
{EDGE}                      { return TokenType::KW_EDGE; }
{UPDATE}                    { return TokenType::KW_UPDATE; }
{ALTER}                     { return TokenType::KW_ALTER; }
{STEPS}                     { return TokenType::KW_STEPS; }
{OVER}                      { return TokenType::KW_OVER; }
{UPTO}                      { return TokenType::KW_UPTO; }
{REVERSELY}                 { return TokenType::KW_REVERSELY; }
{SPACE}                     { return TokenType::KW_SPACE; }
{TTL}                       { return TokenType::KW_TTL; }
{INT8}                      { return TokenType::KW_INT8; }
{INT16}                     { return TokenType::KW_INT16; }
{INT32}                     { return TokenType::KW_INT32; }
{INT64}                     { return TokenType::KW_INT64; }
{UINT8}                     { return TokenType::KW_UINT8; }
{UINT16}                    { return TokenType::KW_UINT16; }
{UINT32}                    { return TokenType::KW_UINT32; }
{UINT64}                    { return TokenType::KW_UINT64; }
{BIGINT}                    { return TokenType::KW_BIGINT; }
{DOUBLE}                    { return TokenType::KW_DOUBLE; }
{STRING}                    { return TokenType::KW_STRING; }
{BOOL}                      { return TokenType::KW_BOOL; }
{TAG}                       { return TokenType::KW_TAG; }
{UNION}                     { return TokenType::KW_UNION; }
{INTERSECT}                 { return TokenType::KW_INTERSECT; }
{MINUS}                     { return TokenType::KW_MINUS; }
{NO}                        { return TokenType::KW_NO; }
{OVERWRITE}                 { return TokenType::KW_OVERWRITE; }
{TRUE}                      { yylval->boolval = true; return TokenType::BOOL; }
{FALSE}                     { yylval->boolval = false; return TokenType::BOOL; }

"."                         { return TokenType::DOT; }
","                         { return TokenType::COMMA; }
":"                         { return TokenType::COLON; }
";"                         { return TokenType::SEMICOLON; }
"@"                         { return TokenType::AT; }

"+"                         { return TokenType::ADD; }
"-"                         { return TokenType::SUB; }
"*"                         { return TokenType::MUL; }
"/"                         { return TokenType::DIV; }
"%"                         { return TokenType::MOD; }

"<"                         { return TokenType::LT; }
"<="                        { return TokenType::LE; }
">"                         { return TokenType::GT; }
">="                        { return TokenType::GE; }
"=="                        { return TokenType::EQ; }
"!="                        { return TokenType::NE; }

"||"                        { return TokenType::OR; }
"&&"                        { return TokenType::AND; }
"|"                         { return TokenType::PIPE; }

"="                         { return TokenType::ASSIGN; }

"("                         { return TokenType::L_PAREN; }
")"                         { return TokenType::R_PAREN; }
"["                         { return TokenType::L_BRACKET; }
"]"                         { return TokenType::R_BRACKET; }
"{"                         { return TokenType::L_BRACE; }
"}"                         { return TokenType::R_BRACE; }

"<-"                        { return TokenType::L_ARROW; }
"->"                        { return TokenType::R_ARROW; }
"_id"                       { return TokenType::ID_PROP; }
"_type"                     { return TokenType::TYPE_PROP; }
"_src"                      { return TokenType::SRC_ID_PROP; }
"_dst"                      { return TokenType::DST_ID_PROP; }
"_rank"                     { return TokenType::RANK_PROP; }
"$_"                        { return TokenType::INPUT_REF; }
"$$"                        { return TokenType::DST_REF; }

{LABEL}                     {
                                yylval->strval = new std::string(yytext, yyleng);
                                return TokenType::LABEL;
                            }
0[Xx]{HEX}+                 {
                                int64_t val = 0;
                                sscanf(yytext, "%lx", &val);
                                yylval->intval = val;
                                return TokenType::INTEGER;
                            }
0{OCT}+                     {
                                int64_t val = 0;
                                sscanf(yytext, "%lo", &val);
                                yylval->intval = val;
                                return TokenType::INTEGER;
                            }
{DEC}+                      { yylval->intval = ::atoll(yytext); return TokenType::INTEGER; }
{DEC}+[Uu][Ll]?             { yylval->intval = ::atoll(yytext); return TokenType::UINTEGER; }
{DEC}+\.{DEC}*              { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }
{DEC}*\.{DEC}+              { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }

\${DEC}+                    { yylval->intval = ::atoll(yytext + 1); return TokenType::COL_REF_ID; }
\${LABEL}                   { yylval->strval = new std::string(yytext + 1, yyleng - 1); return TokenType::VARIABLE; }


\"                          { BEGIN(STR); pos = 0; }
<STR>\"                     {
                                yylval->strval = new std::string(sbuf, pos);
                                BEGIN(INITIAL);
                                return TokenType::STRING;
                            }
<STR>\n                     { yyterminate(); }
<STR>[^\\\n\"]+             {
                                ::strncpy(sbuf + pos, yytext, yyleng);
                                pos += yyleng;
                            }
<STR>\\{OCT}{1,3}           {
                                int val = 0;
                                sscanf(yytext + 1, "%o", &val);
                                if (val > 0xFF) {
                                    yyterminate();
                                }
                                sbuf[pos] = val;
                                pos++;
                            }
<STR>\\{DEC}+               { yyterminate(); }
<STR>\\n                    { sbuf[pos] = '\n'; pos++; }
<STR>\\t                    { sbuf[pos] = '\t'; pos++; }
<STR>\\r                    { sbuf[pos] = '\r'; pos++; }
<STR>\\b                    { sbuf[pos] = '\b'; pos++; }
<STR>\\f                    { sbuf[pos] = '\f'; pos++; }
<STR>\\(.|\n)               { sbuf[pos] = yytext[1]; pos++; }
<STR>\\                     {
                                // This rule should have never been matched,
                                // but without this, it somehow triggers the `nodefault' warning of flex.
                                yyterminate();
                            }

[ \r\t]                     { yylloc->step(); }
\n                          {
                                yylineno++;
                                yylloc->lines(yyleng);
                                yylloc->step();
                            }
.                           { printf("error %c\n", *yytext); yyterminate(); }

%%
