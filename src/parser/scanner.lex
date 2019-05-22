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
CREATE                      ([Cc][Rr][Ee][Aa][Tt][Ee])
DESCRIBE                    ([Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee])
VERTEX                      ([Vv][Ee][Rr][Tt][Ee][Xx])
EDGE                        ([Ee][Dd][Gg][Ee])
EDGES                       ([Ee][Dd][Gg][Ee][Ss])
UPDATE                      ([Uu][Pp][Dd][Aa][Tt][Ee])
DELETE                      ([Dd][Ee][Ll][Ee][Tt][Ee])
FIND                        ([Ff][Ii][Nn][Dd])
ALTER                       ([Aa][Ll][Tt][Ee][Rr])
STEPS                       ([Ss][Tt][Ee][Pp][Ss])
OVER                        ([Oo][Vv][Ee][Rr])
UPTO                        ([Uu][Pp][Tt][Oo])
REVERSELY                   ([Rr][Ee][Vv][Ee][Rr][Ss][Ee][Ll][Yy])
SPACE                       ([Ss][Pp][Aa][Cc][Ee])
SPACES                      ([Ss][Pp][Aa][Cc][Ee][Ss])
INDEX                       ([Ii][Nn][Dd][Ee][Xx])
INDEXS                      ([Ii][Nn][Dd][Ee][Xx][Ss])
TTL                         ([Tt][Tt][Ll])
INT                         ([Ii][Nn][Tt])
BIGINT                      ([Bb][Ii][Gg][Ii][Nn][Tt])
DOUBLE                      ([Dd][Oo][Uu][Bb][Ll][Ee])
STRING                      ([Ss][Tt][Rr][Ii][Nn][Gg])
BOOL                        ([Bb][Oo][Oo][Ll])
TAG                         ([Tt][Aa][Gg])
TAGS                        ([Tt][Aa][Gg][Ss])
UNION                       ([Uu][Nn][Ii][Oo][Nn])
INTERSECT                   ([Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt])
MINUS                       ([Mm][Ii][Nn][Uu][Ss])
NO                          ([Nn][Oo])
OVERWRITE                   ([Oo][Vv][Ee][Rr][Ww][Rr][Ii][Tt][Ee])
TRUE                        ([Tt][Rr][Uu][Ee])
FALSE                       ([Ff][Aa][Ll][Ss][Ee])
SHOW                        ([Ss][Hh][Oo][Ww])
ADD                         ([Aa][Dd][Dd])
HOSTS                       ([Hh][Oo][Ss][Tt][Ss])
TIMESTAMP                   ([Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp])
PARTITION_NUM               ([Pp][Aa][Rr][Tt][Ii][Tt][Ii][[Oo][Nn][_][Nn][Uu][Mm])
REPLICA_FACTOR              ([Rr][Ee][Pp][Ll][Ii][Cc][Aa][_][Ff][Aa][Cc][Tt][Oo][Rr])
DROP                        ([Dd][Rr][Oo][Pp])
REMOVE                      ([Rr][Ee][Mm][Oo][Vv][Ee])
IF                          ([Ii][Ff])
NOT                         ([Nn][Oo][Tt])
EXISTS                      ([Ee][Xx][Ii][Ss][Tt][Ss])
WITH                        ([Ww][Ii][Tt][Hh])
FIRSTNAME                   ([Ff][Ii][Rr][Ss][Tt][Nn][Aa][Mm][Ee])
LASTNAME                    ([Ll][Aa][Ss][Tt][Nn][Aa][Mm][Ee])
EMAIL                       ([Ee][Mm][Aa][Ii][Ll])
PHONE                       ([Pp][Hh][Oo][Nn][Ee])
USER                        ([Uu][Ss][Ee][Rr])
USERS                       ([Uu][Ss][Ee][Rr][Ss])
PASSWORD                    ([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd])
CHANGE                      ([Cc][Hh][Aa][Nn][Gg][Ee])
ROLE                        ([Rr][Oo][Ll][Ee])
GOD                         ([Gg][Oo][Dd])
ADMIN                       ([Aa][Dd][Mm][Ii][Nn])
GUEST                       ([Gg][Uu][Ee][Ss][Tt])
GRANT                       ([Gg][Rr][Aa][Nn][Tt])
REVOKE                      ([Rr][Ee][Vv][Oo][Kk][Ee])
ON                          ([Oo][Nn])
ROLES                       ([Rr][Oo][Ll][Ee][Ss])
BY                          ([Bb][Yy])
IN                          ([Ii][Nn])

LABEL                       ([a-zA-Z][_a-zA-Z0-9]*)
DEC                         ([0-9])
HEX                         ([0-9a-fA-F])
OCT                         ([0-7])
IP_OCTET                    ([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])


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
{DESCRIBE}                  { return TokenType::KW_DESCRIBE; }
{VERTEX}                    { return TokenType::KW_VERTEX; }
{EDGE}                      { return TokenType::KW_EDGE; }
{EDGES}                     { return TokenType::KW_EDGES; }
{UPDATE}                    { return TokenType::KW_UPDATE; }
{DELETE}                    { return TokenType::KW_DELETE; }
{FIND}                      { return TokenType::KW_FIND; }
{ALTER}                     { return TokenType::KW_ALTER; }
{STEPS}                     { return TokenType::KW_STEPS; }
{OVER}                      { return TokenType::KW_OVER; }
{UPTO}                      { return TokenType::KW_UPTO; }
{REVERSELY}                 { return TokenType::KW_REVERSELY; }
{SPACE}                     { return TokenType::KW_SPACE; }
{SPACES}                    { return TokenType::KW_SPACES; }
{INDEX}                     { return TokenType::KW_INDEX; }
{INDEXS}                    { return TokenType::KW_INDEXS; }
{TTL}                       { return TokenType::KW_TTL; }
{INT}                       { return TokenType::KW_INT; }
{BIGINT}                    { return TokenType::KW_BIGINT; }
{DOUBLE}                    { return TokenType::KW_DOUBLE; }
{STRING}                    { return TokenType::KW_STRING; }
{BOOL}                      { return TokenType::KW_BOOL; }
{TAG}                       { return TokenType::KW_TAG; }
{TAGS}                      { return TokenType::KW_TAGS; }
{UNION}                     { return TokenType::KW_UNION; }
{INTERSECT}                 { return TokenType::KW_INTERSECT; }
{MINUS}                     { return TokenType::KW_MINUS; }
{NO}                        { return TokenType::KW_NO; }
{OVERWRITE}                 { return TokenType::KW_OVERWRITE; }
{TRUE}                      { yylval->boolval = true; return TokenType::BOOL; }
{FALSE}                     { yylval->boolval = false; return TokenType::BOOL; }
{SHOW}                      { return TokenType::KW_SHOW; }
{ADD}                       { return TokenType::KW_ADD; }
{HOSTS}                     { return TokenType::KW_HOSTS; }
{TIMESTAMP}                 { return TokenType::KW_TIMESTAMP; }
{CREATE}                    { return TokenType::KW_CREATE;}
{PARTITION_NUM}             { return TokenType::KW_PARTITION_NUM; }
{REPLICA_FACTOR}            { return TokenType::KW_REPLICA_FACTOR; }
{DROP}                      { return TokenType::KW_DROP; }
{REMOVE}                    { return TokenType::KW_REMOVE; }
{IF}                        { return TokenType::KW_IF; }
{NOT}                       { return TokenType::KW_NOT; }
{EXISTS}                    { return TokenType::KW_EXISTS; }
{WITH}                      { return TokenType::KW_WITH; }
{FIRSTNAME}                 { return TokenType::KW_FIRSTNAME; }
{LASTNAME}                  { return TokenType::KW_LASTNAME; }
{EMAIL}                     { return TokenType::KW_EMAIL; }
{PHONE}                     { return TokenType::KW_PHONE; }
{USER}                      { return TokenType::KW_USER; }
{USERS}                     { return TokenType::KW_USERS; }
{PASSWORD}                  { return TokenType::KW_PASSWORD; }
{CHANGE}                    { return TokenType::KW_CHANGE; }
{ROLE}                      { return TokenType::KW_ROLE; }
{GOD}                       { return TokenType::KW_GOD; }
{ADMIN}                     { return TokenType::KW_ADMIN; }
{GUEST}                     { return TokenType::KW_GUEST; }
{GRANT}                     { return TokenType::KW_GRANT; }
{REVOKE}                    { return TokenType::KW_REVOKE; }
{ON}                        { return TokenType::KW_ON; }
{ROLES}                     { return TokenType::KW_ROLES; }
{BY}                        { return TokenType::KW_BY; }
{IN}                        { return TokenType::KW_IN; }

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
"$$"                        { return TokenType::DST_REF; }
"$^"                        { return TokenType::SRC_REF; }
"$-"                        { return TokenType::INPUT_REF; }

{LABEL}                     {
                                yylval->strval = new std::string(yytext, yyleng);
                                if (yylval->strval->size() > MAX_STRING) {
                                    yyterminate();
                                }
                                return TokenType::LABEL;
                            }
{IP_OCTET}(\.{IP_OCTET}){3} {
                                uint32_t octets[4] = {0};
                                sscanf(yytext, "%i.%i.%i.%i", &octets[3], &octets[2], &octets[1], &octets[0]);
                                // The bytes order conforms to the one used in NetworkUtils
                                uint32_t ipv4 = (octets[3] << 24) | (octets[2] << 16) | (octets[1] << 8) | octets[0];
                                yylval->intval = ipv4;
                                return TokenType::IPV4;
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
[+-]?{DEC}+                 { yylval->intval = ::atoll(yytext); return TokenType::INTEGER; }
[+-]?{DEC}+\.{DEC}*         { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }
[+-]?{DEC}*\.{DEC}+         { yylval->doubleval = ::atof(yytext); return TokenType::DOUBLE; }

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
