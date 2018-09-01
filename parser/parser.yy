%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { vesoft }
%define parser_class_name { VGraphParser }
%lex-param { vesoft::VGraphScanner& scanner }
%parse-param { vesoft::VGraphScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { vesoft::Statement** statement }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include "AstTypes.h"

namespace vesoft {

class VGraphScanner;

}

}

%code {
    static int yylex(vesoft::VGraphParser::semantic_type* yylval,
            vesoft::VGraphParser::location_type *yylloc,
            vesoft::VGraphScanner& scanner);
}

%union {
    bool                                    boolval;
    uint64_t                                intval;
    double                                  doubleval;
    std::string                            *strval;
    vesoft::Expression                     *expr;
    vesoft::Sentence                       *sentence;
    vesoft::Statement                      *statement;
    vesoft::ColumnSpecification            *colspec;
    vesoft::ColumnSpecificationList        *colspeclist;
    vesoft::ColumnType                      type;
    vesoft::StepClause                     *step_clause;
    vesoft::FromClause                     *from_clause;
    vesoft::SourceNodeList                 *src_node_list;
    vesoft::OverClause                     *over_clause;
    vesoft::WhereClause                    *where_clause;
    vesoft::ReturnClause                   *return_clause;
    vesoft::ColumnList                     *column_list;
    vesoft::PropertyList                   *prop_list;
    vesoft::ValueList                      *value_list;
}
/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_RETURN KW_DEFINE KW_VERTEX KW_TTL
%token KW_EDGE KW_UPDATE KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_NAMESPACE
%token KW_INT8 KW_INT16 KW_INT32 KW_INT64 KW_UINT8 KW_UINT16 KW_UINT32 KW_UINT64
%token KW_BIGINT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_UNION KW_INTERSECT KW_MINUS
/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND LT LE GT GE EQ NE ADD SUB MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER UINTEGER COL_REF_ID
%token <doubleval> DOUBLE
%token <strval> STRING SYMBOL VARIABLE

%type <expr> expression logic_or_expression logic_and_expression
%type <expr> relational_expression multiplicative_expression additive_expression
%type <expr> unary_expression primary_expression equality_expression
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <src_node_list> id_list ref_list
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <return_clause> return_clause
%type <column_list> column_list
%type <prop_list> prop_list
%type <value_list> value_list

%type <intval> ttl_spec

%type <colspec> column_spec
%type <colspeclist> column_spec_list

%type <sentence> go_sentence match_sentence use_sentence
%type <sentence> define_tag_sentence define_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> query_sentence set_sentence piped_sentence assignment_sentence
%type <sentence> maintainance_sentence insert_vertex_sentence
%type <statement> statement

%start statement

%%

primary_expression
    : INTEGER {
        $$ = new PrimaryExpression((int64_t)$1);
    }
    | UINTEGER {
        $$ = new PrimaryExpression((uint64_t)$1);
    }
    | DOUBLE {
        $$ = new PrimaryExpression($1);
    }
    | STRING {
        $$ = new PrimaryExpression(*$1);
    }
    | BOOL {
        $$ = new PrimaryExpression($1);
    }
    | SYMBOL {
        // TODO(dutor) detect semantic type of symbol
        $$ = new PropertyExpression($1);
    }
    | SYMBOL DOT SYMBOL {
        $$ = new PropertyExpression($1, $3);
    }
    | SYMBOL L_BRACKET SYMBOL R_BRACKET DOT SYMBOL {
        $$ = new PropertyExpression($1, $6, $3);
    }
    | L_PAREN expression R_PAREN {
        $$ = $2;
    }
    ;

unary_expression
    : primary_expression {}
    | ADD primary_expression {
        $$ = new UnaryExpression(UnaryExpression::PLUS, $2);
    }
    | SUB primary_expression {
        $$ = new UnaryExpression(UnaryExpression::MINUS, $2);
    }
    | L_PAREN type_spec R_PAREN primary_expression {
        $$ = new TypeCastingExpression($2, $4);
    }
    ;

type_spec
    : KW_INT8 { $$ = ColumnType::INT8; }
    | KW_INT16 { $$ = ColumnType::INT16; }
    | KW_INT32 { $$ = ColumnType::INT32; }
    | KW_INT64 { $$ = ColumnType::INT64; }
    | KW_UINT8 { $$ = ColumnType::UINT8; }
    | KW_UINT16 { $$ = ColumnType::UINT16; }
    | KW_UINT32 { $$ = ColumnType::UINT32; }
    | KW_UINT64 { $$ = ColumnType::UINT64; }
    | KW_DOUBLE { $$ = ColumnType::DOUBLE; }
    | KW_STRING { $$ = ColumnType::STRING; }
    | KW_BOOL { $$ = ColumnType::BOOL; }
    | KW_BIGINT { $$ = ColumnType::BIGINT; }
    ;

multiplicative_expression
    : unary_expression {}
    | multiplicative_expression MUL unary_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MUL, $3);
    }
    | multiplicative_expression DIV unary_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::DIV, $3);
    }
    | multiplicative_expression MOD unary_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::MOD, $3);
    }
    ;

additive_expression
    : multiplicative_expression {}
    | additive_expression ADD multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::ADD, $3);
    }
    | additive_expression SUB multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::SUB, $3);
    }
    ;

relational_expression
    : additive_expression {}
    | relational_expression LT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LT, $3);
    }
    | relational_expression GT additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GT, $3);
    }
    | relational_expression LE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::LE, $3);
    }
    | relational_expression GE additive_expression {
        $$ = new RelationalExpression($1, RelationalExpression::GE, $3);
    }
    ;

equality_expression
    : relational_expression {}
    | equality_expression EQ relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::EQ, $3);
    }
    | equality_expression NE relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::NE, $3);
    }
    ;

logic_and_expression
    : equality_expression {}
    | logic_and_expression AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    ;

logic_or_expression
    : logic_and_expression {}
    | logic_or_expression OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    ;

expression
    : logic_or_expression { }
    ;

go_sentence
    : KW_GO step_clause from_clause over_clause where_clause return_clause {
        //fprintf(stderr, "primary: %s\n", $5->toString().c_str());
        //fprintf(stderr, "result: ");
        //Expression::print($5->eval());
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        go->setReturnClause($6);
        $$ = go;
    }
    ;

step_clause
    : %empty { $$ = new StepClause(); }
    | INTEGER KW_STEPS { $$ = new StepClause($1); }
    | KW_UPTO INTEGER KW_STEPS { $$ = new StepClause($2, true); }
    ;

from_clause
    : KW_FROM id_list KW_AS SYMBOL {
        auto from = new FromClause($2, $4);
        $$ = from;
    }
    | KW_FROM L_BRACKET ref_list R_BRACKET KW_AS SYMBOL {
        auto from = new FromClause($3, $6, true/* is ref id*/);
        $$ = from;
    }
    ;

id_list
    : INTEGER {
        auto list = new SourceNodeList();
        list->addNodeId($1);
        $$ = list;
    }
    | id_list COMMA INTEGER {
        $$ = $1;
        $$->addNodeId($3);
    }
    ;

ref_list
    : COL_REF_ID {
        auto list = new SourceNodeList();
        list->addNodeId($1);
        $$ = list;
    }
    | ref_list COMMA COL_REF_ID {
        $$ = $1;
        $$->addNodeId($3);
    }
    | ref_list COMMA {
        $$ = $1;
    }
    ;

over_clause
    : %empty { $$ = nullptr; }
    | KW_OVER SYMBOL {
        $$ = new OverClause($2);
    }
    | KW_OVER SYMBOL KW_REVERSELY { $$ = new OverClause($2, true); }
    ;

where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

return_clause
    : %empty { $$ = nullptr; }
    | KW_RETURN column_list { $$ = new ReturnClause($2); }
    ;

column_list
    : expression {
        auto list = new ColumnList();
        list->addColumn($1);
        $$ = list;
    }
    | column_list COMMA expression {
        $1->addColumn($3);
        $$ = $1;
    }
    ;

match_sentence
    : KW_MATCH { $$ = new MatchSentence; }
    ;

use_sentence
    : KW_USE KW_NAMESPACE SYMBOL { $$ = new UseSentence($3); }
    ;

define_tag_sentence
    : KW_DEFINE KW_TAG SYMBOL L_PAREN column_spec_list R_PAREN {
        $$ = new DefineTagSentence($3, $5);
    }
    | KW_DEFINE KW_TAG SYMBOL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new DefineTagSentence($3, $5);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG SYMBOL L_PAREN column_spec_list R_PAREN {
        $$ = new AlterTagSentence($3, $5);
    }
    | KW_ALTER KW_TAG SYMBOL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new AlterTagSentence($3, $5);
    }
    ;

define_edge_sentence
    : KW_DEFINE KW_EDGE SYMBOL L_PAREN column_spec_list R_PAREN {
        $$ = new DefineEdgeSentence($3, $5);
    }
    | KW_DEFINE KW_EDGE SYMBOL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new DefineEdgeSentence($3, $5);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE SYMBOL L_PAREN column_spec_list R_PAREN {
        $$ = new AlterEdgeSentence($3, $5);
    }
    | KW_ALTER KW_EDGE SYMBOL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new AlterEdgeSentence($3, $5);
    }
    ;

column_spec_list
    : column_spec {
        $$ = new ColumnSpecificationList();
        $$->addColumn($1);
    }
    | column_spec_list COMMA column_spec {
        $$ = $1;
        $$->addColumn($3);
    }
    ;

column_spec
    : SYMBOL type_spec { $$ = new ColumnSpecification($2, $1); }
    | SYMBOL type_spec ttl_spec { $$ = new ColumnSpecification($2, $1, $3); }
    ;

ttl_spec
    : KW_TTL ASSIGN INTEGER { $$ = $3; }
    ;

query_sentence
    : go_sentence {}
    | match_sentence {}
    ;

set_sentence
    : query_sentence {}
    | set_sentence KW_UNION query_sentence { $$ = new SetSentence($1, SetSentence::UNION, $3); }
    | set_sentence KW_INTERSECT query_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS query_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    | L_PAREN piped_sentence R_PAREN { $$ = $2; }
    ;

piped_sentence
    : set_sentence {}
    | piped_sentence PIPE set_sentence { $$ = new PipedSentence($1, $3); }
    ;

assignment_sentence
    : VARIABLE ASSIGN piped_sentence {
        $$ = new AssignmentSentence($1, $3);
    }
    ;

insert_vertex_sentence
    : KW_INSERT KW_VERTEX SYMBOL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN INTEGER COLON value_list R_PAREN {
        $$ = new InsertVertexSentence($9, $3, $5, $11);
    }
    | KW_INSERT KW_TAG SYMBOL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN INTEGER COLON value_list R_PAREN {
        $$ = new InsertVertexSentence($9, $3, $5, $11);
    }
    ;

prop_list
    : SYMBOL {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA SYMBOL {
        $$ = $1;
        $$->addProp($3);
    }
    | prop_list COMMA {
        $$ = $1;
    }
    ;

value_list
    : expression {
        $$ = new ValueList();
        $$->addValue($1);
    }
    | value_list COMMA expression {
        $$ = $1;
        $$->addValue($3);
    }
    | value_list COMMA {
        $$ = $1;
    }
    ;

maintainance_sentence
    : define_tag_sentence {}
    | define_edge_sentence {}
    | alter_tag_sentence {}
    | alter_edge_sentence {}
    ;

statement
    : maintainance_sentence {
        $$ = new Statement($1);
        *statement = $$;
    }
    | use_sentence {
        $$ = new Statement($1);
        *statement = $$;
    }
    | piped_sentence {
        $$ = new Statement($1);
        *statement = $$;
    }
    | assignment_sentence {
        $$ = new Statement($1);
        *statement = $$;
    }
    | insert_vertex_sentence {
        $$ = new Statement($1);
        *statement = $$;
    }
    | statement SEMICOLON maintainance_sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | statement SEMICOLON use_sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | statement SEMICOLON piped_sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | statement SEMICOLON assignment_sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | statement SEMICOLON insert_vertex_sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | statement SEMICOLON {
        $$ = $1;
    }
    ;


%%

void vesoft::VGraphParser::error(const vesoft::VGraphParser::location_type& loc,
                                     const std::string &msg) {
    std::ostringstream os;
    os << msg << " at " << loc;
    errmsg = os.str();
}


#include "VGraphScanner.h"
static int yylex(vesoft::VGraphParser::semantic_type* yylval,
        vesoft::VGraphParser::location_type *yylloc,
        vesoft::VGraphScanner& scanner) {
    return scanner.yylex(yylval, yylloc);
}
