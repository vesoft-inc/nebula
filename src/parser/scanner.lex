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
/* These tokens are kept sorted for human lookup */
ALL                         ([Aa][Ll][Ll])
ALTER                       ([Aa][Ll][Tt][Ee][Rr])
AS                          ([Aa][Ss])
BIGINT                      ([Bb][Ii][Gg][Ii][Nn][Tt])
BOOL                        ([Bb][Oo][Oo][Ll])
DEFINE                      ([Dd][Ee][Ff][Ii][Nn][Ee])
DESCRIBE                    ([Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee])
DOUBLE                      ([Dd][Oo][Uu][Bb][Ll][Ee])
EDGE                        ([Ee][Dd][Gg][Ee])
FALSE                       ([Ff][Aa][Ll][Ss][Ee])
FROM                        ([Ff][Rr][Oo][Mm])
GO                          ([Gg][Oo])
HOST                        ([Hh][Oo][Ss][Tt])
INSERT                      ([Ii][Nn][Ss][Ee][Rr][Tt])
INT                         ([Ii][Nn][Tt])
INTERSECT                   ([Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt])
MATCH                       ([Mm][Aa][Tt][Cc][Hh])
MINUS                       ([Mm][Ii][Nn][Uu][Ss])
NO                          ([Nn][Oo])
OR                          ([Oo][Rr])
OVER                        ([Oo][Vv][Ee][Rr])
OVERWRITE                   ([Oo][Vv][Ee][Rr][Ww][Rr][Ii][Tt][Ee])
RETURN                      ([Rr][Ee][Tt][Uu][Rr][Nn])
REVERSELY                   ([Rr][Ee][Vv][Ee][Rr][Ss][Ee][Ll][Yy])
SET                         ([Ss][Ee][Tt])
SHOW                        ([Ss][Hh][Oo][Ww])
SPACE                       ([Ss][Pp][Aa][Cc][Ee])
STEPS                       ([Ss][Tt][Ee][Pp][Ss])
STRING                      ([Ss][Tt][Rr][Ii][Nn][Gg])
TAG                         ([Tt][Aa][Gg])
TO                          ([Tt][Oo])
TRUE                        ([Tt][Rr][Uu][Ee])
TTL                         ([Tt][Tt][Ll])
UNION                       ([Uu][Nn][Ii][Oo][Nn])
UPDATE                      ([Uu][Pp][Dd][Aa][Tt][Ee])
UPTO                        ([Uu][Pp][Tt][Oo])
USE                         ([Uu][Ss][Ee])
VALUES                      ([Vv][Aa][Ll][Uu][Ee][Ss])
VERTEX                      ([Vv][Ee][Rr][Tt][Ee][Xx])
WHERE                       ([Ww][Hh][Ee][Rr][Ee])
YIELD                       ([Yy][Ii][Ee][Ll][Dd])

DEC                         ([0-9])
HEX                         ([0-9a-fA-F])
LABEL                       ([a-zA-Z][_a-zA-Z0-9]*)
OCT                         ([0-7])

%%

                            thread_local static char sbuf[MAX_STRING];
                            size_t pos = 0;

 /* keep same order as above  */
{ALL}                       { return TokenType::KW_ALL; }
{ALTER}                     { return TokenType::KW_ALTER; }
{AS}                        { return TokenType::KW_AS; }
{BIGINT}                    { return TokenType::KW_BIGINT; }
{BOOL}                      { return TokenType::KW_BOOL; }
{DEFINE}                    { return TokenType::KW_DEFINE; }
{DESCRIBE}                  { return TokenType::KW_DESCRIBE; }
{DOUBLE}                    { return TokenType::KW_DOUBLE; }
{EDGE}                      { return TokenType::KW_EDGE; }
{FALSE}                     { yylval->boolval = false; return TokenType::BOOL; }
{FROM}                      { return TokenType::KW_FROM; }
{GO}                        { return TokenType::KW_GO; }
{HOST}                      { return TokenType::KW_HOST; }
{INSERT}                    { return TokenType::KW_INSERT; }
{INT}                       { return TokenType::KW_INT; }
{INTERSECT}                 { return TokenType::KW_INTERSECT; }
{MATCH}                     { return TokenType::KW_MATCH; }
{MINUS}                     { return TokenType::KW_MINUS; }
{NO}                        { return TokenType::KW_NO; }
{OR}                        { return TokenType::KW_OR; }
{OVER}                      { return TokenType::KW_OVER; }
{OVERWRITE}                 { return TokenType::KW_OVERWRITE; }
{RETURN}                    { return TokenType::KW_RETURN; }
{REVERSELY}                 { return TokenType::KW_REVERSELY; }
{SET}                       { return TokenType::KW_SET; }
{SHOW}                      { return TokenType::KW_SHOW; }
{SPACE}                     { return TokenType::KW_SPACE; }
{STEPS}                     { return TokenType::KW_STEPS; }
{STRING}                    { return TokenType::KW_STRING; }
{TAG}                       { return TokenType::KW_TAG; }
{TO}                        { return TokenType::KW_TO; }
{TRUE}                      { yylval->boolval = true; return TokenType::BOOL; }
{TTL}                       { return TokenType::KW_TTL; }
{UNION}                     { return TokenType::KW_UNION; }
{UPDATE}                    { return TokenType::KW_UPDATE; }
{UPTO}                      { return TokenType::KW_UPTO; }
{USE}                       { return TokenType::KW_USE; }
{VALUES}                    { return TokenType::KW_VALUES; }
{VERTEX}                    { return TokenType::KW_VERTEX; }
{WHERE}                     { return TokenType::KW_WHERE; }
{YIELD}                     { return TokenType::KW_YIELD; }

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
"!"                         { return TokenType::NOT; }

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
                                if (yylval->strval->size() > MAX_STRING) {
                                    yyterminate();
                                }
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
{DEC}+\.{DEC}*              { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }
{DEC}*\.{DEC}+              { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }

\${LABEL}                   { yylval->strval = new std::string(yytext + 1, yyleng - 1); return TokenType::VARIABLE; }


\"                          { BEGIN(STR); pos = 0; }
<STR>\"                     {
                                yylval->strval = new std::string(sbuf, pos);
                                BEGIN(INITIAL);
                                if (yylval->strval->size() > MAX_STRING) {
                                    yyterminate();
                                }
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
