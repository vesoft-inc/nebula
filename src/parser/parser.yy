%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { GraphParser }
%lex-param { nebula::GraphScanner& scanner }
%parse-param { nebula::GraphScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::SequentialSentences** sentences }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include "parser/SequentialSentences.h"

namespace nebula {

class GraphScanner;

}

}

%code {
    static int yylex(nebula::GraphParser::semantic_type* yylval,
                     nebula::GraphParser::location_type *yylloc,
                     nebula::GraphScanner& scanner);
}

%union {
    bool                                    boolval;
    int64_t                                 intval;
    double                                  doubleval;
    std::string                            *strval;
    nebula::Expression                     *expr;
    nebula::Sentence                       *sentence;
    nebula::SequentialSentences            *sentences;
    nebula::ColumnSpecification            *colspec;
    nebula::ColumnSpecificationList        *colspeclist;
    nebula::ColumnType                      type;
    nebula::StepClause                     *step_clause;
    nebula::FromClause                     *from_clause;
    nebula::SourceNodeList                 *src_node_list;
    nebula::OverClause                     *over_clause;
    nebula::WhereClause                    *where_clause;
    nebula::YieldClause                    *yield_clause;
    nebula::YieldColumns                   *yield_columns;
    nebula::YieldColumn                    *yield_column;
    nebula::PropertyList                   *prop_list;
    nebula::ValueList                      *value_list;
    nebula::UpdateList                     *update_list;
    nebula::UpdateItem                     *update_item;
}
/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_YIELD KW_RETURN KW_DEFINE KW_VERTEX KW_TTL
%token KW_EDGE KW_UPDATE KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_SPACE
%token KW_INT KW_BIGINT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN KW_DESCRIBE KW_SHOW KW_HOSTS KW_TIMESTAMP
/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND LT LE GT GE EQ NE ADD SUB MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON L_ARROW R_ARROW AT
%token ID_PROP TYPE_PROP SRC_ID_PROP DST_ID_PROP RANK_PROP INPUT_REF DST_REF

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER
%token <doubleval> DOUBLE
%token <strval> STRING VARIABLE LABEL

%type <expr> expression logic_or_expression logic_and_expression
%type <expr> relational_expression multiplicative_expression additive_expression
%type <expr> unary_expression primary_expression equality_expression
%type <expr> ref_expression input_ref_expression dst_ref_expression var_ref_expression
%type <expr> alias_ref_expression
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <src_node_list> id_list
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <yield_clause> yield_clause
%type <yield_columns> yield_columns
%type <yield_column> yield_column
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
%type <sentence> describe_tag_sentence describe_edge_sentence
%type <sentence> traverse_sentence set_sentence piped_sentence assignment_sentence
%type <sentence> maintainance_sentence insert_vertex_sentence insert_edge_sentence
%type <sentence> mutate_sentence update_vertex_sentence update_edge_sentence
%type <sentence> show_sentence
%type <sentence> sentence
%type <sentences> sentences

%start sentences

%%

primary_expression
    : INTEGER {
        $$ = new PrimaryExpression($1);
    }
    | DOUBLE {
        $$ = new PrimaryExpression($1);
    }
    | STRING {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | BOOL {
        $$ = new PrimaryExpression($1);
    }
    | LABEL {
        $$ = new PrimaryExpression(*$1);
        delete $1;
    }
    | input_ref_expression {
        $$ = $1;
    }
    | dst_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    | alias_ref_expression {
        $$ = $1;
    }
    | L_PAREN expression R_PAREN {
        $$ = $2;
    }
    ;

input_ref_expression
    : INPUT_REF DOT LABEL {
        $$ = new InputPropertyExpression($3);
    }
    ;

dst_ref_expression
    : DST_REF L_BRACKET LABEL R_BRACKET DOT ID_PROP {
        $$ = new DestIdExpression($3);
    }
    | DST_REF L_BRACKET LABEL R_BRACKET DOT LABEL {
        $$ = new DestPropertyExpression($3, $6);
    }
    ;

var_ref_expression
    : VARIABLE DOT LABEL {
        $$ = new VariablePropertyExpression($1, $3);
    }
    ;

alias_ref_expression
    : LABEL DOT LABEL {
        $$ = new EdgePropertyExpression($1, $3);
    }
    | LABEL DOT TYPE_PROP {
        $$ = new EdgeTypeExpression($1);
    }
    | LABEL DOT SRC_ID_PROP {
        $$ = new EdgeSrcIdExpression($1);
    }
    | LABEL DOT DST_ID_PROP {
        $$ = new EdgeDstIdExpression($1);
    }
    | LABEL DOT RANK_PROP {
        $$ = new EdgeRankExpression($1);
    }
    | LABEL L_BRACKET LABEL R_BRACKET DOT LABEL {
        $$ = new SourcePropertyExpression($1, $3, $6);
    }
    | LABEL L_BRACKET LABEL R_BRACKET DOT ID_PROP {
        $$ = new SourceIdExpression($1, $3);
    }
    ;

unary_expression
    : primary_expression {}
    | ADD primary_expression {
        $$ = new UnaryExpression(UnaryExpression::PLUS, $2);
    }
    | SUB primary_expression {
        $$ = new UnaryExpression(UnaryExpression::NEGATE, $2);
    }
    | NOT primary_expression {
        $$ = new UnaryExpression(UnaryExpression::NOT, $2);
    }
    | L_PAREN type_spec R_PAREN primary_expression {
        $$ = new TypeCastingExpression($2, $4);
    }
    ;

type_spec
    : KW_INT { $$ = ColumnType::INT; }
    | KW_DOUBLE { $$ = ColumnType::DOUBLE; }
    | KW_STRING { $$ = ColumnType::STRING; }
    | KW_BOOL { $$ = ColumnType::BOOL; }
    | KW_BIGINT { $$ = ColumnType::BIGINT; }
    | KW_TIMESTAMP { $$ = ColumnType::TIMESTAMP; }
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
    : KW_GO step_clause from_clause over_clause where_clause yield_clause {
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        go->setYieldClause($6);
        $$ = go;
    }
    ;

step_clause
    : %empty { $$ = new StepClause(); }
    | INTEGER KW_STEPS { $$ = new StepClause($1); }
    | KW_UPTO INTEGER KW_STEPS { $$ = new StepClause($2, true); }
    ;

from_clause
    : KW_FROM id_list {
        auto from = new FromClause($2);
        $$ = from;
    }
    | KW_FROM id_list KW_AS LABEL {
        auto from = new FromClause($2, $4);
        $$ = from;
    }
    | KW_FROM ref_expression {
        auto from = new FromClause($2);
        $$ = from;
    }
    | KW_FROM ref_expression KW_AS LABEL {
        auto from = new FromClause($2, $4);
        $$ = from;
    }
    ;

ref_expression
    : input_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }

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

over_clause
    : KW_OVER LABEL {
        $$ = new OverClause($2);
    }
    | KW_OVER LABEL KW_REVERSELY {
        $$ = new OverClause($2, nullptr, true);
    }
    | KW_OVER LABEL KW_AS LABEL {
        $$ = new OverClause($2, $4);
    }
    | KW_OVER LABEL KW_AS LABEL KW_REVERSELY {
        $$ = new OverClause($2, $4, true);
    }
    ;

where_clause
    : %empty { $$ = nullptr; }
    | KW_WHERE expression { $$ = new WhereClause($2); }
    ;

yield_clause
    : %empty { $$ = nullptr; }
    | KW_YIELD yield_columns { $$ = new YieldClause($2); }
    ;

yield_columns
    : yield_column {
        auto fields = new YieldColumns();
        fields->addColumn($1);
        $$ = fields;
    }
    | yield_columns COMMA yield_column {
        $1->addColumn($3);
        $$ = $1;
    }
    ;

yield_column
    : expression {
        $$ = new YieldColumn($1);
    }
    | expression KW_AS LABEL {
        $$ = new YieldColumn($1, $3);
    }
    ;

match_sentence
    : KW_MATCH { $$ = new MatchSentence; }
    ;

use_sentence
    : KW_USE KW_SPACE LABEL { $$ = new UseSentence($3); }
    ;

define_tag_sentence
    : KW_DEFINE KW_TAG LABEL L_PAREN R_PAREN {
        $$ = new DefineTagSentence($3, new ColumnSpecificationList());
    }
    | KW_DEFINE KW_TAG LABEL L_PAREN column_spec_list R_PAREN {
        $$ = new DefineTagSentence($3, $5);
    }
    | KW_DEFINE KW_TAG LABEL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new DefineTagSentence($3, $5);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG LABEL L_PAREN R_PAREN {
        $$ = new AlterTagSentence($3, new ColumnSpecificationList());
    }
    | KW_ALTER KW_TAG LABEL L_PAREN column_spec_list R_PAREN {
        $$ = new AlterTagSentence($3, $5);
    }
    | KW_ALTER KW_TAG LABEL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new AlterTagSentence($3, $5);
    }
    ;

define_edge_sentence
    : KW_DEFINE KW_EDGE LABEL LABEL R_ARROW LABEL L_PAREN R_PAREN {
        $$ = new DefineEdgeSentence($3, $4, $6, new ColumnSpecificationList());
    }
    | KW_DEFINE KW_EDGE LABEL LABEL R_ARROW LABEL L_PAREN column_spec_list R_PAREN {
        $$ = new DefineEdgeSentence($3, $4, $6, $8);
    }
    | KW_DEFINE KW_EDGE LABEL LABEL R_ARROW LABEL L_PAREN column_spec_list COMMA R_PAREN {
        $$ = new DefineEdgeSentence($3, $4, $6, $8);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE LABEL L_PAREN R_PAREN {
        $$ = new AlterEdgeSentence($3, new ColumnSpecificationList());
    }
    | KW_ALTER KW_EDGE LABEL L_PAREN column_spec_list R_PAREN {
        $$ = new AlterEdgeSentence($3, $5);
    }
    | KW_ALTER KW_EDGE LABEL L_PAREN column_spec_list COMMA R_PAREN {
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
    : LABEL type_spec { $$ = new ColumnSpecification($2, $1); }
    | LABEL type_spec ttl_spec { $$ = new ColumnSpecification($2, $1, $3); }
    ;

ttl_spec
    : KW_TTL ASSIGN INTEGER { $$ = $3; }
    ;

describe_tag_sentence
    : KW_DESCRIBE KW_TAG LABEL {
        $$ = new DescribeTagSentence($3);
    }
    ;

describe_edge_sentence
    : KW_DESCRIBE KW_EDGE LABEL {
        $$ = new DescribeEdgeSentence($3);
    }
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
    : KW_INSERT KW_VERTEX LABEL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN INTEGER COLON value_list R_PAREN {
        $$ = new InsertVertexSentence($9, $3, $5, $11);
    }
    | KW_INSERT KW_TAG LABEL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN INTEGER COLON value_list R_PAREN {
        $$ = new InsertVertexSentence($9, $3, $5, $11);
    }
    ;

prop_list
    : LABEL {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA LABEL {
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
    : KW_INSERT KW_EDGE LABEL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
      INTEGER R_ARROW INTEGER COLON value_list R_PAREN {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setSrcId($9);
        sentence->setDstId($11);
        sentence->setValues($13);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE LABEL L_PAREN prop_list R_PAREN
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
    | KW_INSERT KW_EDGE LABEL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
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
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE LABEL L_PAREN prop_list R_PAREN KW_VALUES L_PAREN
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
    : KW_UPDATE KW_VERTEX INTEGER KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhereClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_VERTEX INTEGER KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setInsertable(true);
        sentence->setVid($5);
        sentence->setUpdateList($7);
        sentence->setWhereClause($8);
        sentence->setYieldClause($9);
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
    : LABEL ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    ;

update_edge_sentence
    : KW_UPDATE KW_EDGE INTEGER R_ARROW INTEGER
      KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setUpdateList($7);
        sentence->setWhereClause($8);
        sentence->setYieldClause($9);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE INTEGER R_ARROW INTEGER
      KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($5);
        sentence->setDstId($7);
        sentence->setUpdateList($9);
        sentence->setWhereClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_EDGE INTEGER R_ARROW INTEGER AT INTEGER
      KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setRank($7);
        sentence->setUpdateList($9);
        sentence->setWhereClause($10);
        sentence->setYieldClause($11);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE INTEGER R_ARROW INTEGER AT INTEGER KW_SET
      update_list where_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setInsertable(true);
        sentence->setSrcId($5);
        sentence->setDstId($7);
        sentence->setRank($9);
        sentence->setUpdateList($11);
        sentence->setWhereClause($12);
        sentence->setYieldClause($13);
        $$ = sentence;
    }
    ;

show_sentence
    : KW_SHOW KW_HOSTS {
        auto sentence = new ShowSentence(ShowKind::kShowHosts);
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
    | describe_tag_sentence {}
    | describe_edge_sentence {}
    | show_sentence {}
    ;

sentence
    : maintainance_sentence {}
    | use_sentence {}
    | piped_sentence {}
    | assignment_sentence {}
    | mutate_sentence {}
    ;

sentences
    : sentence {
        $$ = new SequentialSentences($1);
        *sentences = $$;
    }
    | sentences SEMICOLON sentence {
        $$ = $1;
        $1->addSentence($3);
    }
    | sentences SEMICOLON {
        $$ = $1;
    }
    ;


%%

void nebula::GraphParser::error(const nebula::GraphParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    os << msg << " at " << loc;
    errmsg = os.str();
}


#include "GraphScanner.h"
static int yylex(nebula::GraphParser::semantic_type* yylval,
                 nebula::GraphParser::location_type *yylloc,
                 nebula::GraphScanner& scanner) {
    return scanner.yylex(yylval, yylloc);
}
