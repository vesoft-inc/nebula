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
#include "parser/Statement.h"

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
    vesoft::ReturnFields                   *return_fields;
    vesoft::PropertyList                   *prop_list;
    vesoft::ValueList                      *value_list;
    vesoft::UpdateList                     *update_list;
    vesoft::UpdateItem                     *update_item;
}
/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_RETURN KW_DEFINE KW_VERTEX KW_TTL
%token KW_EDGE KW_UPDATE KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_NAMESPACE
%token KW_INT8 KW_INT16 KW_INT32 KW_INT64 KW_UINT8 KW_UINT16 KW_UINT32 KW_UINT64
%token KW_BIGINT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN
/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND LT LE GT GE EQ NE ADD SUB MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON L_ARROW R_ARROW AT

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
%type <return_fields> return_fields
%type <prop_list> prop_list
%type <value_list> value_list
%type <update_list> update_list
%type <update_item> update_item

%type <intval> ttl_spec

%type <colspec> column_spec
%type <colspeclist> column_spec_list

%type <sentence> go_sentence match_sentence use_sentence
%type <sentence> define_tag_sentence define_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> traverse_sentence set_sentence piped_sentence assignment_sentence
%type <sentence> maintainance_sentence insert_vertex_sentence insert_edge_sentence
%type <sentence> mutate_sentence update_vertex_sentence update_edge_sentence
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
    | KW_RETURN return_fields { $$ = new ReturnClause($2); }
    ;

return_fields
    : expression {
        auto fields = new ReturnFields();
        fields->addColumn($1);
        $$ = fields;
    }
    | return_fields COMMA expression {
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

traverse_sentence
    : go_sentence {}
    | match_sentence {}
    ;

set_sentence
    : traverse_sentence {}
    | set_sentence KW_UNION traverse_sentence { $$ = new SetSentence($1, SetSentence::UNION, $3); }
    | set_sentence KW_INTERSECT traverse_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS traverse_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
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

insert_edge_sentence
    : KW_INSERT KW_EDGE SYMBOL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
      INTEGER R_ARROW INTEGER COLON value_list R_PAREN {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setSrcId($9);
        sentence->setDstId($11);
        sentence->setValues($13);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE SYMBOL L_PAREN prop_list R_PAREN
      KW_VALUES L_PAREN INTEGER R_ARROW INTEGER COLON value_list R_PAREN {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setSrcId($11);
        sentence->setDstId($13);
        sentence->setValues($15);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE SYMBOL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
      INTEGER R_ARROW INTEGER AT INTEGER COLON value_list R_PAREN {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setSrcId($9);
        sentence->setDstId($11);
        sentence->setRank($13);
        sentence->setValues($15);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE SYMBOL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
      INTEGER R_ARROW INTEGER AT INTEGER COLON value_list R_PAREN {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setSrcId($11);
        sentence->setDstId($13);
        sentence->setRank($15);
        sentence->setValues($17);
        $$ = sentence;
    }
    ;

update_vertex_sentence
    : KW_UPDATE KW_VERTEX INTEGER KW_SET update_list where_clause return_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhereClause($6);
        sentence->setReturnClause($7);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_VERTEX INTEGER KW_SET update_list where_clause return_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setInsertable(true);
        sentence->setVid($5);
        sentence->setUpdateList($7);
        sentence->setWhereClause($8);
        sentence->setReturnClause($9);
        $$ = sentence;
    }
    ;

update_list
    : update_item {
        $$ = new UpdateList();
        $$->addItem($1);
    }
    | update_list COMMA update_item {
        $$ = $1;
        $$->addItem($3);
    }
    ;

update_item
    : SYMBOL ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    ;

update_edge_sentence
    : KW_UPDATE KW_EDGE INTEGER R_ARROW INTEGER
      KW_SET update_list where_clause return_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setUpdateList($7);
        sentence->setWhereClause($8);
        sentence->setReturnClause($9);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE INTEGER R_ARROW INTEGER
      KW_SET update_list where_clause return_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($5);
        sentence->setDstId($7);
        sentence->setUpdateList($9);
        sentence->setWhereClause($10);
        sentence->setReturnClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE INTEGER R_ARROW INTEGER AT INTEGER
      KW_SET update_list where_clause return_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setUpdateList($9);
        sentence->setWhereClause($10);
        sentence->setReturnClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE INTEGER R_ARROW INTEGER AT INTEGER KW_SET
      update_list where_clause return_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($5);
        sentence->setDstId($7);
        sentence->setRank($9);
        sentence->setUpdateList($11);
        sentence->setWhereClause($12);
        sentence->setReturnClause($13);
        $$ = sentence;
    }
    ;

mutate_sentence
    : insert_vertex_sentence {}
    | insert_edge_sentence {}
    | update_vertex_sentence {}
    | update_edge_sentence {}
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
    | mutate_sentence {
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
    | statement SEMICOLON mutate_sentence {
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
