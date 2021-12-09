%option c++
%option yyclass="GraphScanner"
%option nodefault noyywrap
%option never-interactive
%option yylineno
%option case-insensitive

%{
#include "parser/GQLParser.h"
#include "parser/GraphScanner.h"
#include "GraphParser.hpp"
#include "graph/service/GraphFlags.h"

#define YY_USER_ACTION                  \
    yylloc->step();                     \
    yylloc->columns(yyleng);

static constexpr size_t MAX_STRING = 4096;

%}

%x DQ_STR
%x SQ_STR
%x COMMENT

blanks                      ([ \t\n]+)

NOT_IN                      (NOT{blanks}IN)
NOT_CONTAINS                (NOT{blanks}CONTAINS)
STARTS_WITH                 (STARTS{blanks}WITH)
NOT_STARTS_WITH             (NOT{blanks}STARTS{blanks}WITH)
ENDS_WITH                   (ENDS{blanks}WITH)
NOT_ENDS_WITH               (NOT{blanks}ENDS{blanks}WITH)
IS_NULL                     (IS{blanks}NULL)
IS_NOT_NULL                 (IS{blanks}NOT{blanks}NULL)
IS_EMPTY                    (IS{blanks}EMPTY)
IS_NOT_EMPTY                (IS{blanks}NOT{blanks}EMPTY)

LABEL                       ([a-zA-Z][_a-zA-Z0-9]*)
DEC                         ([0-9])
EXP                         ([eE][-+]?[0-9]+)
HEX                         ([0-9a-fA-F])
OCT                         ([0-7])
IP_OCTET                    ([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])

U                           [\x80-\xbf]
U2                          [\xc2-\xdf]
U3                          [\xe0-\xef]
U4                          [\xf0-\xf4]
CHINESE                     {U2}{U}|{U3}{U}{U}|{U4}{U}{U}{U}
CN_EN                       {CHINESE}|[a-zA-Z]
CN_EN_NUM                   {CHINESE}|[_a-zA-Z0-9]
UTF8_LABEL                  {CN_EN}{CN_EN_NUM}+

%%

 /* Reserved keyword */
"GO"                        { return TokenType::KW_GO; }
"AS"                        { return TokenType::KW_AS; }
"TO"                        { return TokenType::KW_TO; }
"OR"                        { return TokenType::KW_OR; }
"AND"                       { return TokenType::KW_AND; }
"XOR"                       { return TokenType::KW_XOR; }
"USE"                       { return TokenType::KW_USE; }
"SET"                       { return TokenType::KW_SET; }
"LIST"                      { return TokenType::KW_LIST; }
"MAP"                       { return TokenType::KW_MAP; }
"FROM"                      { return TokenType::KW_FROM; }
"WHERE"                     { return TokenType::KW_WHERE; }
"MATCH"                     { return TokenType::KW_MATCH; }
"INSERT"                    { return TokenType::KW_INSERT; }
"YIELD"                     { return TokenType::KW_YIELD; }
"RETURN"                    { return TokenType::KW_RETURN; }
"DESCRIBE"                  { return TokenType::KW_DESCRIBE; }
"DESC"                      { return TokenType::KW_DESC; }
"VERTEX"                    { return TokenType::KW_VERTEX; }
"VERTICES"                  { return TokenType::KW_VERTICES; }
"EDGE"                      { return TokenType::KW_EDGE; }
"EDGES"                     { return TokenType::KW_EDGES; }
"UPDATE"                    { return TokenType::KW_UPDATE; }
"UPSERT"                    { return TokenType::KW_UPSERT; }
"WHEN"                      { return TokenType::KW_WHEN; }
"DELETE"                    { return TokenType::KW_DELETE; }
"FIND"                      { return TokenType::KW_FIND; }
"PATH"                      { return TokenType::KW_PATH; }
"LOOKUP"                    { return TokenType::KW_LOOKUP; }
"ALTER"                     { return TokenType::KW_ALTER; }
"STEPS"                     { return TokenType::KW_STEPS; }
"STEP"                      { return TokenType::KW_STEPS; }
"OVER"                      { return TokenType::KW_OVER; }
"UPTO"                      { return TokenType::KW_UPTO; }
"REVERSELY"                 { return TokenType::KW_REVERSELY; }
"INDEX"                     { return TokenType::KW_INDEX; }
"INDEXES"                   { return TokenType::KW_INDEXES; }
"REBUILD"                   { return TokenType::KW_REBUILD; }
"BOOL"                      { return TokenType::KW_BOOL; }
"INT8"                      { return TokenType::KW_INT8; }
"INT16"                     { return TokenType::KW_INT16; }
"INT32"                     { return TokenType::KW_INT32; }
"INT64"                     { return TokenType::KW_INT64; }
"INT"                       { return TokenType::KW_INT; }
"FLOAT"                     { return TokenType::KW_FLOAT; }
"DOUBLE"                    { return TokenType::KW_DOUBLE; }
"STRING"                    { return TokenType::KW_STRING; }
"FIXED_STRING"              { return TokenType::KW_FIXED_STRING; }
"TIMESTAMP"                 { return TokenType::KW_TIMESTAMP; }
"DATE"                      { return TokenType::KW_DATE; }
"TIME"                      { return TokenType::KW_TIME; }
"DATETIME"                  { return TokenType::KW_DATETIME; }
"TAG"                       { return TokenType::KW_TAG; }
"TAGS"                      { return TokenType::KW_TAGS; }
"UNION"                     { return TokenType::KW_UNION; }
"INTERSECT"                 { return TokenType::KW_INTERSECT; }
"MINUS"                     { return TokenType::KW_MINUS; }
"NO"                        { return TokenType::KW_NO; }
"OVERWRITE"                 { return TokenType::KW_OVERWRITE; }
"SHOW"                      { return TokenType::KW_SHOW; }
"ADD"                       { return TokenType::KW_ADD; }
"CREATE"                    { return TokenType::KW_CREATE;}
"DROP"                      { return TokenType::KW_DROP; }
"REMOVE"                    { return TokenType::KW_REMOVE; }
"IF"                        { return TokenType::KW_IF; }
"NOT"                       { return TokenType::KW_NOT; }
"EXISTS"                    { return TokenType::KW_EXISTS; }
"WITH"                      { return TokenType::KW_WITH; }
"CHANGE"                    { return TokenType::KW_CHANGE; }
"GRANT"                     { return TokenType::KW_GRANT; }
"REVOKE"                    { return TokenType::KW_REVOKE; }
"ON"                        { return TokenType::KW_ON; }
"BY"                        { return TokenType::KW_BY; }
"IN"                        { return TokenType::KW_IN; }
{NOT_IN}                    { return TokenType::KW_NOT_IN; }
"DOWNLOAD"                  { return TokenType::KW_DOWNLOAD; }
"GET"                       { return TokenType::KW_GET; }
"OF"                        { return TokenType::KW_OF; }
"ORDER"                     { return TokenType::KW_ORDER; }
"INGEST"                    { return TokenType::KW_INGEST; }
"COMPACT"                   { return TokenType::KW_COMPACT; }
"FLUSH"                     { return TokenType::KW_FLUSH; }
"SUBMIT"                    { return TokenType::KW_SUBMIT; }
"ASC"                       { return TokenType::KW_ASC; }
"ASCENDING"                 { return TokenType::KW_ASCENDING; }
"DESCENDING"                { return TokenType::KW_DESCENDING; }
"DISTINCT"                  { return TokenType::KW_DISTINCT; }
"FETCH"                     { return TokenType::KW_FETCH; }
"PROP"                      { return TokenType::KW_PROP; }
"BALANCE"                   { return TokenType::KW_BALANCE; }
"STOP"                      { return TokenType::KW_STOP; }
"LIMIT"                     { return TokenType::KW_LIMIT; }
"OFFSET"                    { return TokenType::KW_OFFSET; }
"IS"                        { return TokenType::KW_IS; }
"NULL"                      { return TokenType::KW_NULL; }
"RECOVER"                   { return TokenType::KW_RECOVER; }
"EXPLAIN"                   { return TokenType::KW_EXPLAIN; }
"PROFILE"                   { return TokenType::KW_PROFILE; }
"FORMAT"                    { return TokenType::KW_FORMAT; }
"CASE"                      { return TokenType::KW_CASE; }

 /**
  * TODO(dutor) Manage the dynamic allocated objects with an object pool,
  *     so that we ease the operations such as expression rewriting, associating
  *     the original text for an unreserved keywords, etc.
  *
  */
 /* Unreserved keyword */
"HOST"                      { return TokenType::KW_HOST; }
"HOSTS"                     { return TokenType::KW_HOSTS; }
"SPACE"                     { return TokenType::KW_SPACE; }
"SPACES"                    { return TokenType::KW_SPACES; }
"VALUE"                     { return TokenType::KW_VALUE; }
"VALUES"                    { return TokenType::KW_VALUES; }
"USER"                      { return TokenType::KW_USER; }
"USERS"                     { return TokenType::KW_USERS; }
"PASSWORD"                  { return TokenType::KW_PASSWORD; }
"ROLE"                      { return TokenType::KW_ROLE; }
"ROLES"                     { return TokenType::KW_ROLES; }
"GOD"                       { return TokenType::KW_GOD; }
"ADMIN"                     { return TokenType::KW_ADMIN; }
"DBA"                       { return TokenType::KW_DBA; }
"GUEST"                     { return TokenType::KW_GUEST; }
"GROUP"                     { return TokenType::KW_GROUP; }
"PARTITION_NUM"             { return TokenType::KW_PARTITION_NUM; }
"REPLICA_FACTOR"            { return TokenType::KW_REPLICA_FACTOR; }
"VID_TYPE"                  { return TokenType::KW_VID_TYPE; }
"CHARSET"                   { return TokenType::KW_CHARSET; }
"COLLATE"                   { return TokenType::KW_COLLATE; }
"COLLATION"                 { return TokenType::KW_COLLATION; }
"ATOMIC_EDGE"               { return TokenType::KW_ATOMIC_EDGE; }
"ALL"                       { return TokenType::KW_ALL; }
"ANY"                       { return TokenType::KW_ANY; }
"SINGLE"                    { return TokenType::KW_SINGLE; }
"NONE"                      { return TokenType::KW_NONE; }
"REDUCE"                    { return TokenType::KW_REDUCE; }
"LEADER"                    { return TokenType::KW_LEADER; }
"UUID"                      { return TokenType::KW_UUID; }
"DATA"                      { return TokenType::KW_DATA; }
"SNAPSHOT"                  { return TokenType::KW_SNAPSHOT; }
"SNAPSHOTS"                 { return TokenType::KW_SNAPSHOTS; }
"ACCOUNT"                   { return TokenType::KW_ACCOUNT; }
"JOBS"                      { return TokenType::KW_JOBS; }
"JOB"                       { return TokenType::KW_JOB; }
"BIDIRECT"                  { return TokenType::KW_BIDIRECT; }
"STATS"                     { return TokenType::KW_STATS; }
"STATUS"                    { return TokenType::KW_STATUS; }
"FORCE"                     { return TokenType::KW_FORCE; }
"PART"                      { return TokenType::KW_PART; }
"PARTS"                     { return TokenType::KW_PARTS; }
"DEFAULT"                   { return TokenType::KW_DEFAULT; }
"HDFS"                      { return TokenType::KW_HDFS; }
"CONFIGS"                   { return TokenType::KW_CONFIGS; }
"TTL_DURATION"              { return TokenType::KW_TTL_DURATION; }
"TTL_COL"                   { return TokenType::KW_TTL_COL; }
"GRAPH"                     { return TokenType::KW_GRAPH; }
"META"                      { return TokenType::KW_META; }
"STORAGE"                   { return TokenType::KW_STORAGE; }
"SHORTEST"                  { return TokenType::KW_SHORTEST; }
"NOLOOP"                    { return TokenType::KW_NOLOOP; }
"OUT"                       { return TokenType::KW_OUT; }
"BOTH"                      { return TokenType::KW_BOTH; }
"SUBGRAPH"                  { return TokenType::KW_SUBGRAPH; }
"CONTAINS"                  { return TokenType::KW_CONTAINS; }
{NOT_CONTAINS}              { return TokenType::KW_NOT_CONTAINS; }
"STARTS"                    { return TokenType::KW_STARTS;}
{STARTS_WITH}               { return TokenType::KW_STARTS_WITH;}
{NOT_STARTS_WITH}           { return TokenType::KW_NOT_STARTS_WITH;}
"ENDS"                      { return TokenType::KW_ENDS;}
{ENDS_WITH}                 { return TokenType::KW_ENDS_WITH;}
{NOT_ENDS_WITH}             { return TokenType::KW_NOT_ENDS_WITH;}
{IS_NULL}                   { return TokenType::KW_IS_NULL;}
{IS_NOT_NULL}               { return TokenType::KW_IS_NOT_NULL;}
{IS_EMPTY}                  { return TokenType::KW_IS_EMPTY;}
{IS_NOT_EMPTY}              { return TokenType::KW_IS_NOT_EMPTY;}
"UNWIND"                    { return TokenType::KW_UNWIND;}
"SKIP"                      { return TokenType::KW_SKIP;}
"OPTIONAL"                  { return TokenType::KW_OPTIONAL;}
"THEN"                      { return TokenType::KW_THEN; }
"ELSE"                      { return TokenType::KW_ELSE; }
"END"                       { return TokenType::KW_END; }
"GROUPS"                    { return TokenType::KW_GROUPS; }
"ZONE"                      { return TokenType::KW_ZONE; }
"ZONES"                     { return TokenType::KW_ZONES; }
"INTO"                      { return TokenType::KW_INTO; }
"LISTENER"                  { return TokenType::KW_LISTENER; }
"ELASTICSEARCH"             { return TokenType::KW_ELASTICSEARCH; }
"HTTP"                      { return TokenType::KW_HTTP; }
"HTTPS"                      { return TokenType::KW_HTTPS; }
"FULLTEXT"                  { return TokenType::KW_FULLTEXT; }
"AUTO"                      { return TokenType::KW_AUTO; }
"FUZZY"                     { return TokenType::KW_FUZZY; }
"PREFIX"                    { return TokenType::KW_PREFIX; }
"REGEXP"                    { return TokenType::KW_REGEXP; }
"WILDCARD"                  { return TokenType::KW_WILDCARD; }
"TEXT"                      { return TokenType::KW_TEXT; }
"SEARCH"                    { return TokenType::KW_SEARCH; }
"CLIENTS"                   { return TokenType::KW_CLIENTS; }
"SIGN"                      { return TokenType::KW_SIGN; }
"SERVICE"                   { return TokenType::KW_SERVICE; }
"TEXT_SEARCH"               { return TokenType::KW_TEXT_SEARCH; }
"RESET"                     { return TokenType::KW_RESET; }
"PLAN"                      { return TokenType::KW_PLAN; }
"COMMENT"                   { return TokenType::KW_COMMENT; }
"SESSIONS"                  { return TokenType::KW_SESSIONS; }
"SESSION"                   { return TokenType::KW_SESSION; }
"SAMPLE"                    { return TokenType::KW_SAMPLE; }
"QUERIES"                   { return TokenType::KW_QUERIES; }
"QUERY"                     { return TokenType::KW_QUERY; }
"KILL"                      { return TokenType::KW_KILL; }
"TOP"                       { return TokenType::KW_TOP; }
"GEOGRAPHY"                 { return TokenType::KW_GEOGRAPHY; }
"POINT"                     { return TokenType::KW_POINT; }
"LINESTRING"                { return TokenType::KW_LINESTRING; }
"POLYGON"                   { return TokenType::KW_POLYGON; }
"TRUE"                      { yylval->boolval = true; return TokenType::BOOL; }
"FALSE"                     { yylval->boolval = false; return TokenType::BOOL; }

"."                         { return TokenType::DOT; }
".."                        { return TokenType::DOT_DOT; }
","                         { return TokenType::COMMA; }
":"                         { return TokenType::COLON; }
";"                         { return TokenType::SEMICOLON; }
"@"                         { return TokenType::AT; }
"?"                         { return TokenType::QM; }

"+"                         { return TokenType::PLUS; }
"-"                         { return TokenType::MINUS; }
"*"                         { return TokenType::STAR; }
"/"                         { return TokenType::DIV; }
"%"                         { return TokenType::MOD; }
"!"                         { return TokenType::NOT; }

"<"                         { return TokenType::LT; }
"<="                        { return TokenType::LE; }
">"                         { return TokenType::GT; }
">="                        { return TokenType::GE; }
"=="                        { return TokenType::EQ; }
"!="                        { return TokenType::NE; }
"<>"                        { return TokenType::NE; }
"=~"                        { return TokenType::REG; }

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
                                yylval->strval = new std::string(yytext, yyleng);
                                return TokenType::IPV4;
                            }
0[Xx]{HEX}+                 {
                                return parseHex();
                            }
0{OCT}+                     {
                                return parseOct();
                            }

{DEC}+\.\.                  {
                                yyless(yyleng - 2);
                                return parseDecimal();
                            }
{DEC}+                      {
                                return parseDecimal();
                            }

{DEC}*\.{DEC}+              |
{DEC}+\.{DEC}*              |
{DEC}*\.{DEC}*{EXP}         |
{DEC}+{EXP}                 {
                                return parseDouble();
                            }

\${LABEL}                   {
                                yylval->strval = new std::string(yytext + 1, yyleng - 1);
                                return TokenType::VARIABLE;
                            }


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
                                if (FLAGS_disable_octal_escape_char) {
                                    makeSpaceForString(yyleng);
                                    ::strncpy(sbuf() + sbufPos_, yytext, yyleng);
                                    sbufPos_ += yyleng;
                                } else {
                                    makeSpaceForString(1);
                                    uint32_t val = 0;
                                    sscanf(yytext + 1, "%o", &val);
                                    if (val > 0xFF) {
                                        yyterminate();
                                    }
                                    sbuf()[sbufPos_++] = val;
                                }
                            }
<DQ_STR,SQ_STR>\\{DEC}+ {
                                if (FLAGS_disable_octal_escape_char) {
                                    makeSpaceForString(yyleng);
                                    ::strncpy(sbuf() + sbufPos_, yytext, yyleng);
                                    sbufPos_ += yyleng;
                                } else {
                                    yyterminate();
                                }
                            }
<DQ_STR,SQ_STR>\\[uUxX]{HEX}{4} {
                                auto encoded = folly::codePointToUtf8(std::strtoul(yytext+2, nullptr, 16));
                                makeSpaceForString(encoded.size());
                                ::strncpy(sbuf() + sbufPos_, encoded.data(), encoded.size());
                                sbufPos_ += encoded.size();
                            }
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
"/*"                        { BEGIN(COMMENT); }
<COMMENT>"*/"               { BEGIN(INITIAL); }
<COMMENT>([^*]|\n)+|.
<COMMENT><<EOF>>            {
                                // Must match /* */
                                throw GraphParser::syntax_error(*yylloc, "unterminated comment");
                            }
\`{UTF8_LABEL}\`            {
                                yylval->strval = new std::string(yytext + 1, yyleng - 2);
                                if (yylval->strval->size() > MAX_STRING) {
                                    auto error = "Out of range of the LABEL length, "
                                                  "the  max length of LABEL is " +
                                                  std::to_string(MAX_STRING) + ":";
                                    delete yylval->strval;
                                    throw GraphParser::syntax_error(*yylloc, error);
                                }
                                return TokenType::UTF8_LABEL;
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
