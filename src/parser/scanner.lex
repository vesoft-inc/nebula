%option c++
%option yyclass="GraphScanner"
%option nodefault noyywrap
%option never-interactive
%option yylineno

%{
#include "parser/GQLParser.h"
#include "parser/GraphScanner.h"
#include "GraphParser.hpp"

#define YY_USER_ACTION                  \
    yylloc->step();                     \
    yylloc->columns(yyleng);

using TokenType = nebula::GraphParser::token;

static constexpr size_t MAX_STRING = 4096;

%}

%x DQ_STR
%x SQ_STR
%x COMMENT

GO                          ([Gg][Oo])
AS                          ([Aa][Ss])
TO                          ([Tt][Oo])
OR                          ([Oo][Rr])
AND                         ([Aa][Nn][Dd])
XOR                         ([Xx][Oo][Rr])
USE                         ([Uu][Ss][Ee])
SET                         ([Ss][Ee][Tt])
FROM                        ([Ff][Rr][Oo][Mm])
WHERE                       ([Ww][Hh][Ee][Rr][Ee])
MATCH                       ([Mm][Aa][Tt][Cc][Hh])
INSERT                      ([Ii][Nn][Ss][Ee][Rr][Tt])
VALUES                      ([Vv][Aa][Ll][Uu][Ee][Ss]?)
YIELD                       ([Yy][Ii][Ee][Ll][Dd])
RETURN                      ([Rr][Ee][Tt][Uu][Rr][Nn])
CREATE                      ([Cc][Rr][Ee][Aa][Tt][Ee])
DESCRIBE                    ([Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee])
DESC                        ([Dd][Ee][Ss][Cc])
VERTEX                      ([Vv][Ee][Rr][Tt][Ee][Xx])
EDGE                        ([Ee][Dd][Gg][Ee])
EDGES                       ([Ee][Dd][Gg][Ee][Ss])
UPDATE                      ([Uu][Pp][Dd][Aa][Tt][Ee])
UPSERT                      ([Uu][Pp][Ss][Ee][Rr][Tt])
WHEN                        ([Ww][Hh][Ee][Nn])
DELETE                      ([Dd][Ee][Ll][Ee][Tt][Ee])
FIND                        ([Ff][Ii][Nn][Dd])
LOOKUP                      ([Ll][Oo][Oo][Kk][Uu][Pp])
ALTER                       ([Aa][Ll][Tt][Ee][Rr])
STEPS                       ([Ss][Tt][Ee][Pp][Ss]?)
OVER                        ([Oo][Vv][Ee][Rr])
UPTO                        ([Uu][Pp][Tt][Oo])
REVERSELY                   ([Rr][Ee][Vv][Ee][Rr][Ss][Ee][Ll][Yy])
SPACE                       ([Ss][Pp][Aa][Cc][Ee])
SPACES                      ([Ss][Pp][Aa][Cc][Ee][Ss])
INDEX                       ([Ii][Nn][Dd][Ee][Xx])
INDEXES                     ([Ii][Nn][Dd][Ee][Xx][Ee][Ss])
REBUILD                     ([Rr][Ee][Bb][Uu][Ii][Ll][Dd])
STATUS                      ([Ss][Tt][Aa][Tt][Uu][Ss])
INT                         ([Ii][Nn][Tt])
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
PART                        ([Pp][Aa][Rr][Tt])
PARTS                       ([Pp][Aa][Rr][Tt][Ss])
TIMESTAMP                   ([Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp])
PARTITION_NUM               ([Pp][Aa][Rr][Tt][Ii][Tt][Ii][[Oo][Nn][_][Nn][Uu][Mm])
REPLICA_FACTOR              ([Rr][Ee][Pp][Ll][Ii][Cc][Aa][_][Ff][Aa][Cc][Tt][Oo][Rr])
CHARSET                     ([Cc][Hh][Aa][Rr][Ss][Ee][Tt])
COLLATE                     ([Cc][Oo][Ll][Ll][Aa][Tt][Ee])
COLLATION                   ([Cc][Oo][Ll][Ll][Aa][Tt][Ii][Oo][Nn])
DROP                        ([Dd][Rr][Oo][Pp])
REMOVE                      ([Rr][Ee][Mm][Oo][Vv][Ee])
IF                          ([Ii][Ff])
NOT                         ([Nn][Oo][Tt])
EXISTS                      ([Ee][Xx][Ii][Ss][Tt][Ss])
WITH                        ([Ww][Ii][Tt][Hh])
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
TTL_DURATION                ([Tt][Tt][Ll][_][Dd][Uu][Rr][Aa][Tt][Ii][Oo][Nn])
TTL_COL                     ([Tt][Tt][Ll][_][Cc][Oo][Ll])
DOWNLOAD                    ([Dd][Oo][Ww][Nn][Ll][Oo][Aa][Dd])
HDFS                        ([Hh][Dd][Ff][Ss])
ORDER                       ([Oo][Rr][Dd][Ee][Rr])
INGEST                      ([Ii][Nn][Gg][Ee][Ss][Tt])
SUBMIT                      ([Ss][Uu][Bb][Mm][Ii][Tt])
COMPACT                     ([Cc][Oo][Mm][Pp][Aa][Cc][Tt])
FLUSH                       ([Ff][Ll][Uu][Ss][Hh])
ASC                         ([Aa][Ss][Cc])
DISTINCT                    ([Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt])
DEFAULT                     ([Dd][Ee][Ff][Aa][Uu][Ll][Tt])
CONFIGS                     ([Cc][Oo][Nn][Ff][Ii][Gg][Ss])
GET                         ([Gg][Ee][Tt])
GRAPH                       ([Gg][Rr][Aa][Pp][Hh])
META                        ([Mm][Ee][Tt][Aa])
STORAGE                     ([Ss][Tt][Oo][Rr][Aa][Gg][Ee])
FETCH                       ([Ff][Ee][Tt][Cc][Hh])
PROP                        ([Pp][Rr][Oo][Pp])
ALL                         ([Aa][Ll][Ll])
BALANCE                     ([Bb][Aa][Ll][Aa][Nn][Cc][Ee])
LEADER                      ([Ll][Ee][Aa][Dd][Ee][Rr])
UUID                        ([Uu][Uu][Ii][Dd])
OF                          ([Oo][Ff])
DATA                        ([Dd][Aa][Tt][Aa])
STOP                        ([Ss][Tt][Oo][Pp])
SHORTEST                    ([Ss][Hh][Oo][Rr][Tt][Ee][Ss][Tt])
PATH                        ([Pp][Aa][Tt][Hh])
LIMIT                       ([Ll][Ii][Mm][Ii][Tt])
OFFSET                      ([Oo][Ff][Ff][Ss][Ee][Tt])
GROUP                       ([Gg][Rr][Oo][Uu][Pp])
COUNT                       ([Cc][Oo][Uu][Nn][Tt])
COUNT_DISTINCT              ([Cc][Oo][Uu][Nn][Tt][_][Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt])
SUM                         ([Ss][Uu][Mm])
AVG                         ([Aa][Vv][Gg])
MIN                         ([Mm][Ii][Nn])
MAX                         ([Mm][Aa][Xx])
STD                         ([Ss][Tt][Dd])
BIT_AND                     ([Bb][Ii][Tt][_][Aa][Nn][Dd])
BIT_OR                      ([Bb][Ii][Tt][_][Oo][Rr])
BIT_XOR                     ([Bb][Ii][Tt][_][Xx][Oo][Rr])
IS                          ([Ii][Ss])
NULL                        ([Nn][Uu][Ll][Ll])
SNAPSHOT                    ([Ss][Nn][Aa][Pp][Ss][Hh][Oo][Tt])
SNAPSHOTS                   ([Ss][Nn][Aa][Pp][Ss][Hh][Oo][Tt][Ss])
FORCE                       ([Ff][Oo][Rr][Cc][Ee])
OFFLINE                     ([Oo][Ff][Ff][Ll][Ii][Nn][Ee])
BIDIRECT                    ([Bb][Ii][Dd][Ii][Rr][Ee][Cc][Tt])
ACCOUNT                     ([Aa][Cc][Cc][Oo][Uu][Nn][Tt])
DBA                         ([Dd][Bb][Aa])
CONTAINS                    ([Cc][Oo][Nn][Tt][Aa][Ii][Nn][Ss])
GLOBAL                      ([Gg][Ll][Oo][Bb][Aa][Ll])

LABEL                       ([a-zA-Z][_a-zA-Z0-9]*)
DEC                         ([0-9])
EXP                         ([eE][-+]?[0-9]*)
HEX                         ([0-9a-fA-F])
OCT                         ([0-7])
IP_OCTET                    ([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])

JOBS                        ([Jj][Oo][Bb][Ss])
JOB                         ([Jj][Oo][Bb])
RECOVER                     ([Rr][Ee][Cc][Oo][Vv][Ee][Rr])

%%

 /* Reserved keyword */
{GO}                        { return TokenType::KW_GO; }
{AS}                        { return TokenType::KW_AS; }
{TO}                        { return TokenType::KW_TO; }
{OR}                        { return TokenType::KW_OR; }
{AND}                       { return TokenType::KW_AND; }
{XOR}                       { return TokenType::KW_XOR; }
{USE}                       { return TokenType::KW_USE; }
{SET}                       { return TokenType::KW_SET; }
{FROM}                      { return TokenType::KW_FROM; }
{WHERE}                     { return TokenType::KW_WHERE; }
{MATCH}                     { return TokenType::KW_MATCH; }
{INSERT}                    { return TokenType::KW_INSERT; }
{YIELD}                     { return TokenType::KW_YIELD; }
{RETURN}                    { return TokenType::KW_RETURN; }
{DESCRIBE}                  { return TokenType::KW_DESCRIBE; }
{DESC}                      { return TokenType::KW_DESC; }
{VERTEX}                    { return TokenType::KW_VERTEX; }
{EDGE}                      { return TokenType::KW_EDGE; }
{EDGES}                     { return TokenType::KW_EDGES; }
{UPDATE}                    { return TokenType::KW_UPDATE; }
{UPSERT}                    { return TokenType::KW_UPSERT; }
{WHEN}                      { return TokenType::KW_WHEN; }
{DELETE}                    { return TokenType::KW_DELETE; }
{FIND}                      { return TokenType::KW_FIND; }
{LOOKUP}                    { return TokenType::KW_LOOKUP; }
{ALTER}                     { return TokenType::KW_ALTER; }
{STEPS}                     { return TokenType::KW_STEPS; }
{OVER}                      { return TokenType::KW_OVER; }
{UPTO}                      { return TokenType::KW_UPTO; }
{REVERSELY}                 { return TokenType::KW_REVERSELY; }
{INDEX}                     { return TokenType::KW_INDEX; }
{INDEXES}                   { return TokenType::KW_INDEXES; }
{REBUILD}                   { return TokenType::KW_REBUILD; }
{INT}                       { return TokenType::KW_INT; }
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
{SHOW}                      { return TokenType::KW_SHOW; }
{ADD}                       { return TokenType::KW_ADD; }
{TIMESTAMP}                 { return TokenType::KW_TIMESTAMP; }
{CREATE}                    { return TokenType::KW_CREATE;}
{DROP}                      { return TokenType::KW_DROP; }
{REMOVE}                    { return TokenType::KW_REMOVE; }
{IF}                        { return TokenType::KW_IF; }
{NOT}                       { return TokenType::KW_NOT; }
{EXISTS}                    { return TokenType::KW_EXISTS; }
{WITH}                      { return TokenType::KW_WITH; }
{CHANGE}                    { return TokenType::KW_CHANGE; }
{GRANT}                     { return TokenType::KW_GRANT; }
{REVOKE}                    { return TokenType::KW_REVOKE; }
{ON}                        { return TokenType::KW_ON; }
{BY}                        { return TokenType::KW_BY; }
{IN}                        { return TokenType::KW_IN; }
{DOWNLOAD}                  { return TokenType::KW_DOWNLOAD; }
{GET}                       { return TokenType::KW_GET; }
{OF}                        { return TokenType::KW_OF; }
{ORDER}                     { return TokenType::KW_ORDER; }
{INGEST}                    { return TokenType::KW_INGEST; }
{COMPACT}                   { return TokenType::KW_COMPACT; }
{FLUSH}                     { return TokenType::KW_FLUSH; }
{SUBMIT}                    { return TokenType::KW_SUBMIT; }
{ASC}                       { return TokenType::KW_ASC; }
{DISTINCT}                  { return TokenType::KW_DISTINCT; }
{FETCH}                     { return TokenType::KW_FETCH; }
{PROP}                      { return TokenType::KW_PROP; }
{BALANCE}                   { return TokenType::KW_BALANCE; }
{STOP}                      { return TokenType::KW_STOP; }
{LIMIT}                     { return TokenType::KW_LIMIT; }
{OFFSET}                    { return TokenType::KW_OFFSET; }
{IS}                        { return TokenType::KW_IS; }
{NULL}                      { return TokenType::KW_NULL; }
{RECOVER}                   { return TokenType::KW_RECOVER; }
{GLOBAL}                    { return TokenType::KW_GLOBAL; }


 /* Unreserved keyword */
{HOSTS}                     { return TokenType::KW_HOSTS; }
{SPACE}                     { return TokenType::KW_SPACE; }
{SPACES}                    { return TokenType::KW_SPACES; }
{VALUES}                    { return TokenType::KW_VALUES; }
{USER}                      { return TokenType::KW_USER; }
{USERS}                     { return TokenType::KW_USERS; }
{PASSWORD}                  { return TokenType::KW_PASSWORD; }
{ROLE}                      { return TokenType::KW_ROLE; }
{ROLES}                     { return TokenType::KW_ROLES; }
{GOD}                       { return TokenType::KW_GOD; }
{ADMIN}                     { return TokenType::KW_ADMIN; }
{DBA}                       { return TokenType::KW_DBA; }
{GUEST}                     { return TokenType::KW_GUEST; }
{GROUP}                     { return TokenType::KW_GROUP; }
{PARTITION_NUM}             { return TokenType::KW_PARTITION_NUM; }
{REPLICA_FACTOR}            { return TokenType::KW_REPLICA_FACTOR; }
{CHARSET}                   { return TokenType::KW_CHARSET; }
{COLLATE}                   { return TokenType::KW_COLLATE; }
{COLLATION}                 { return TokenType::KW_COLLATION; }
{ALL}                       { return TokenType::KW_ALL; }
{LEADER}                    { return TokenType::KW_LEADER; }
{UUID}                      { return TokenType::KW_UUID; }
{DATA}                      { return TokenType::KW_DATA; }
{SNAPSHOT}                  { return TokenType::KW_SNAPSHOT; }
{SNAPSHOTS}                 { return TokenType::KW_SNAPSHOTS; }
{OFFLINE}                   { return TokenType::KW_OFFLINE; }
{ACCOUNT}                   { return TokenType::KW_ACCOUNT; }
{JOBS}                      { return TokenType::KW_JOBS; }
{JOB}                       { return TokenType::KW_JOB; }
{COUNT}                     { return TokenType::KW_COUNT; }
{COUNT_DISTINCT}            { return TokenType::KW_COUNT_DISTINCT; }
{SUM}                       { return TokenType::KW_SUM; }
{AVG}                       { return TokenType::KW_AVG; }
{MAX}                       { return TokenType::KW_MAX; }
{MIN}                       { return TokenType::KW_MIN; }
{STD}                       { return TokenType::KW_STD; }
{BIT_AND}                   { return TokenType::KW_BIT_AND; }
{BIT_OR}                    { return TokenType::KW_BIT_OR; }
{BIT_XOR}                   { return TokenType::KW_BIT_XOR; }
{PATH}                      { return TokenType::KW_PATH; }
{BIDIRECT}                  { return TokenType::KW_BIDIRECT; }
{STATUS}                    { return TokenType::KW_STATUS; }
{FORCE}                     { return TokenType::KW_FORCE; }
{PART}                      { return TokenType::KW_PART; }
{PARTS}                     { return TokenType::KW_PARTS; }
{DEFAULT}                   { return TokenType::KW_DEFAULT; }
{HDFS}                      { return TokenType::KW_HDFS; }
{CONFIGS}                   { return TokenType::KW_CONFIGS; }
{TTL_DURATION}              { return TokenType::KW_TTL_DURATION; }
{TTL_COL}                   { return TokenType::KW_TTL_COL; }
{GRAPH}                     { return TokenType::KW_GRAPH; }
{META}                      { return TokenType::KW_META; }
{STORAGE}                   { return TokenType::KW_STORAGE; }
{SHORTEST}                  { return TokenType::KW_SHORTEST; }
{CONTAINS}                  { return TokenType::KW_CONTAINS; }


{TRUE}                      { yylval->boolval = true; return TokenType::BOOL; }
{FALSE}                     { yylval->boolval = false; return TokenType::BOOL; }

"."                         { return TokenType::DOT; }
","                         { return TokenType::COMMA; }
":"                         { return TokenType::COLON; }
";"                         { return TokenType::SEMICOLON; }
"@"                         { return TokenType::AT; }

"+"                         { return TokenType::PLUS; }
"-"                         { return TokenType::MINUS; }
"*"                         { return TokenType::MUL; }
"/"                         { return TokenType::DIV; }
"%"                         { return TokenType::MOD; }
"!"                         { return TokenType::NOT; }
"^"                         { return TokenType::XOR; }

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
                                    auto error = "Out of range of the LABEL length, "
                                                  "the  max length of LABEL is " +
                                                  std::to_string(MAX_STRING) + ":";
                                    delete yylval->strval;
                                    throw GraphParser::syntax_error(*yylloc, error);
                                }
                                return TokenType::LABEL;
                            }
\`{LABEL}\`                 {
                                yylval->strval = new std::string(yytext + 1, yyleng - 2);
                                if (yylval->strval->size() > MAX_STRING) {
                                    auto error = "Out of range of the LABEL length, "
                                                  "the  max length of LABEL is " +
                                                  std::to_string(MAX_STRING) + ":";
                                    delete yylval->strval;
                                    throw GraphParser::syntax_error(*yylloc, error);
                                }
                                return TokenType::LABEL;
                            }
{IP_OCTET}(\.{IP_OCTET}){3} {
                                uint32_t octets[4] = {0};
                                sscanf(yytext, "%u.%u.%u.%u", &octets[3], &octets[2], &octets[1], &octets[0]);
                                // The bytes order conforms to the one used in NetworkUtils
                                uint32_t ipv4 = (octets[3] << 24) | (octets[2] << 16) | (octets[1] << 8) | octets[0];
                                yylval->intval = ipv4;
                                return TokenType::IPV4;
                            }
0[Xx]{HEX}+                 {
                                if (yyleng > 18) {
                                    auto i = 2;
                                    while (i < yyleng && yytext[i] == '0') {
                                        i++;
                                    }
                                    if (yyleng - i > 16) {
                                        throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                    }
                                }
                                uint64_t val = 0;
                                sscanf(yytext, "%lx", &val);
                                if (val > MAX_ABS_INTEGER) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                yylval->intval = static_cast<int64_t>(val);
                                return TokenType::INTEGER;
                            }
0{OCT}+                     {
                                if (yyleng > 22) {
                                    auto i = 1;
                                    while (i < yyleng && yytext[i] == '0') {
                                        i++;
                                    }
                                    if (yyleng - i > 22 ||
                                            (yyleng - i == 22 && yytext[i] != '1')) {
                                        throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                    }
                                }
                                uint64_t val = 0;
                                sscanf(yytext, "%lo", &val);
                                if (val > MAX_ABS_INTEGER) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                yylval->intval = static_cast<int64_t>(val);
                                return TokenType::INTEGER;
                            }
{DEC}+                      {
                                try {
                                    folly::StringPiece text(yytext, yyleng);
                                    uint64_t val = folly::to<uint64_t>(text);
                                    if (val > MAX_ABS_INTEGER) {
                                        throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                    }
                                    yylval->intval = val;
                                } catch (...) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                return TokenType::INTEGER;
                            }
{DEC}+\.{DEC}*{EXP}         {
                                try {
                                    folly::StringPiece text(yytext, yyleng);
                                    yylval->doubleval = folly::to<double>(text);
                                } catch (...) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                return TokenType::DOUBLE;
                            }
{DEC}+\.{DEC}*              {
                                try {
                                    folly::StringPiece text(yytext, yyleng);
                                    yylval->doubleval = folly::to<double>(text);
                                } catch (...) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                return TokenType::DOUBLE;
                            }
{DEC}*\.{DEC}+              {
                                try {
                                    folly::StringPiece text(yytext, yyleng);
                                    yylval->doubleval = folly::to<double>(text);
                                } catch (...) {
                                    throw GraphParser::syntax_error(*yylloc, "Out of range:");
                                }
                                return TokenType::DOUBLE;
                            }

\${LABEL}                   { yylval->strval = new std::string(yytext + 1, yyleng - 1); return TokenType::VARIABLE; }


\"                          { BEGIN(DQ_STR); sbufPos_ = 0; }
\'                          { BEGIN(SQ_STR); sbufPos_ = 0; }
<DQ_STR>\"                  {
                                yylval->strval = new std::string(sbuf(), sbufPos_);
                                BEGIN(INITIAL);
                                return TokenType::STRING;
                            }
<SQ_STR>\'                  {
                                yylval->strval = new std::string(sbuf(), sbufPos_);
                                BEGIN(INITIAL);
                                return TokenType::STRING;
                            }
<DQ_STR,SQ_STR><<EOF>>      {
                                // Must match '' or ""
                                throw GraphParser::syntax_error(*yylloc, "Unterminated string: ");
                            }
<DQ_STR,SQ_STR>\n           { yyterminate(); }
<DQ_STR>[^\\\n\"]+          {
                                makeSpaceForString(yyleng);
                                ::strncpy(sbuf() + sbufPos_, yytext, yyleng);
                                sbufPos_ += yyleng;
                            }
<SQ_STR>[^\\\n\']+          {
                                makeSpaceForString(yyleng);
                                ::strncpy(sbuf() + sbufPos_, yytext, yyleng);
                                sbufPos_ += yyleng;
                            }
<DQ_STR,SQ_STR>\\{OCT}{1,3} {
                                makeSpaceForString(1);
                                uint32_t val = 0;
                                sscanf(yytext + 1, "%o", &val);
                                if (val > 0xFF) {
                                    yyterminate();
                                }
                                sbuf()[sbufPos_++] = val;
                            }
<DQ_STR,SQ_STR>\\{DEC}+     { yyterminate(); }
<DQ_STR,SQ_STR>\\n          {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = '\n';
                            }
<DQ_STR,SQ_STR>\\t          {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = '\t';
                            }
<DQ_STR,SQ_STR>\\r          {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = '\r';
                            }
<DQ_STR,SQ_STR>\\b          {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = '\b';
                            }
<DQ_STR,SQ_STR>\\f          {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = '\f';
                            }
<DQ_STR,SQ_STR>\\(.|\n)     {
                                makeSpaceForString(1);
                                sbuf()[sbufPos_++] = yytext[1];
                            }
<DQ_STR,SQ_STR>\\           {
                                // This rule should have never been matched,
                                // but without this, it somehow triggers the `nodefault' warning of flex.
                                yyterminate();
                            }

[ \r\t]                     { }
\n                          {
                                yylineno++;
                                yylloc->lines(yyleng);
                            }
"#".*                       // Skip the annotation
"//".*                      // Skip the annotation
"-- ".*                     // Skip the annotation
"/*"                        { BEGIN(COMMENT); }
<COMMENT>"*/"               { BEGIN(INITIAL); }
<COMMENT>([^*]|\n)+|.
<COMMENT><<EOF>>            {
                                // Must match /* */
                                throw GraphParser::syntax_error(*yylloc, "unterminated comment");
                            }
.                           {
                                /**
                                 * Any other unmatched byte sequences will get us here,
                                 * including the non-ascii ones, which are negative
                                 * in terms of type of `signed char'. At the same time, because
                                 * Bison translates all negative tokens to EOF(i.e. YY_NULL),
                                 * so we have to cast illegal characters to type of `unsinged char'
                                 * This will make Bison receive an unknown token, which leads to
                                 * a syntax error.
                                 *
                                 * Please note that it is not Flex but Bison to regard illegal
                                 * characters as errors, in such case.
                                 */
                                return static_cast<unsigned char>(yytext[0]);

                                /**
                                 * Alternatively, we could report illegal characters by
                                 * throwing a `syntax_error' exception.
                                 * In such a way, we could distinguish illegal characters
                                 * from normal syntax errors, but at cost of poor performance
                                 * incurred by the expensive exception handling.
                                 */
                                // throw GraphParser::syntax_error(*yylloc, "char illegal");
                            }

%%
