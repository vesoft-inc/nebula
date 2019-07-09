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
    nebula::VertexIDList                   *vid_list;
    nebula::OverClause                     *over_clause;
    nebula::WhereClause                    *where_clause;
    nebula::YieldClause                    *yield_clause;
    nebula::YieldColumns                   *yield_columns;
    nebula::YieldColumn                    *yield_column;
    nebula::VertexTagList                  *vertex_tag_list;
    nebula::VertexTagItem                  *vertex_tag_item;
    nebula::PropertyList                   *prop_list;
    nebula::ValueList                      *value_list;
    nebula::VertexRowList                  *vertex_row_list;
    nebula::VertexRowItem                  *vertex_row_item;
    nebula::EdgeRowList                    *edge_row_list;
    nebula::EdgeRowItem                    *edge_row_item;
    nebula::UpdateList                     *update_list;
    nebula::UpdateItem                     *update_item;
    nebula::EdgeList                       *edge_list;
    nebula::ArgumentList                   *argument_list;
    nebula::HostList                       *host_list;
    nebula::HostAddr                       *host_item;
    nebula::SpaceOptList                   *space_opt_list;
    nebula::SpaceOptItem                   *space_opt_item;
    nebula::AlterSchemaOptList             *alter_schema_opt_list;
    nebula::AlterSchemaOptItem             *alter_schema_opt_item;
    nebula::WithUserOptList                *with_user_opt_list;
    nebula::WithUserOptItem                *with_user_opt_item;
    nebula::RoleTypeClause                 *role_type_clause;
    nebula::AclItemClause                  *acl_item_clause;
    nebula::SchemaPropList                 *create_schema_prop_list;
    nebula::SchemaPropItem                 *create_schema_prop_item;
    nebula::SchemaPropList                 *alter_schema_prop_list;
    nebula::SchemaPropItem                 *alter_schema_prop_item;
    nebula::OrderFactor                    *order_factor;
    nebula::OrderFactors                   *order_factors;
}

/* destructors */
%destructor {} <sentences>
%destructor {} <boolval> <intval> <doubleval> <type>
%destructor { delete $$; } <*>

/* keywords */
%token KW_GO KW_AS KW_TO KW_OR KW_USE KW_SET KW_FROM KW_WHERE KW_ALTER
%token KW_MATCH KW_INSERT KW_VALUES KW_YIELD KW_RETURN KW_CREATE KW_VERTEX
%token KW_EDGE KW_EDGES KW_UPDATE KW_STEPS KW_OVER KW_UPTO KW_REVERSELY KW_SPACE KW_DELETE KW_FIND
%token KW_INT KW_BIGINT KW_DOUBLE KW_STRING KW_BOOL KW_TAG KW_TAGS KW_UNION KW_INTERSECT KW_MINUS
%token KW_NO KW_OVERWRITE KW_IN KW_DESCRIBE KW_DESC KW_SHOW KW_HOSTS KW_TIMESTAMP KW_ADD
%token KW_PARTITION_NUM KW_REPLICA_FACTOR KW_DROP KW_REMOVE KW_SPACES
%token KW_IF KW_NOT KW_EXISTS KW_WITH KW_FIRSTNAME KW_LASTNAME KW_EMAIL KW_PHONE KW_USER KW_USERS
%token KW_PASSWORD KW_CHANGE KW_ROLE KW_GOD KW_ADMIN KW_GUEST KW_GRANT KW_REVOKE KW_ON
%token KW_ROLES KW_BY
%token KW_TTL_DURATION KW_TTL_COL
%token KW_ORDER KW_ASC
/* symbols */
%token L_PAREN R_PAREN L_BRACKET R_BRACKET L_BRACE R_BRACE COMMA
%token PIPE OR AND LT LE GT GE EQ NE PLUS MINUS MUL DIV MOD NOT NEG ASSIGN
%token DOT COLON SEMICOLON L_ARROW R_ARROW AT
%token ID_PROP TYPE_PROP SRC_ID_PROP DST_ID_PROP RANK_PROP INPUT_REF DST_REF SRC_REF

/* token type specification */
%token <boolval> BOOL
%token <intval> INTEGER IPV4
%token <doubleval> DOUBLE
%token <strval> STRING VARIABLE LABEL

%type <strval> name_label unreserved_keyword
%type <expr> expression logic_or_expression logic_and_expression
%type <expr> relational_expression multiplicative_expression additive_expression
%type <expr> unary_expression primary_expression equality_expression
%type <expr> src_ref_expression
%type <expr> dst_ref_expression
%type <expr> input_ref_expression
%type <expr> var_ref_expression
%type <expr> alias_ref_expression
%type <expr> vid_ref_expression
%type <expr> vid
%type <expr> function_call_expression
%type <argument_list> argument_list
%type <type> type_spec
%type <step_clause> step_clause
%type <from_clause> from_clause
%type <vid_list> vid_list
%type <over_clause> over_clause
%type <where_clause> where_clause
%type <yield_clause> yield_clause
%type <yield_columns> yield_columns
%type <yield_column> yield_column
%type <vertex_tag_list> vertex_tag_list
%type <vertex_tag_item> vertex_tag_item
%type <prop_list> prop_list
%type <value_list> value_list
%type <vertex_row_list> vertex_row_list
%type <vertex_row_item> vertex_row_item
%type <edge_row_list> edge_row_list
%type <edge_row_item> edge_row_item
%type <update_list> update_list
%type <update_item> update_item
%type <edge_list> edge_list
%type <host_list> host_list
%type <host_item> host_item
%type <space_opt_list> space_opt_list
%type <space_opt_item> space_opt_item
%type <alter_schema_opt_list> alter_schema_opt_list
%type <alter_schema_opt_item> alter_schema_opt_item
%type <create_schema_prop_list> create_schema_prop_list
%type <create_schema_prop_item> create_schema_prop_item
%type <alter_schema_prop_list> alter_schema_prop_list
%type <alter_schema_prop_item> alter_schema_prop_item
%type <order_factor> order_factor
%type <order_factors> order_factors

%type <intval> port unary_integer rank

%type <colspec> column_spec
%type <colspeclist> column_spec_list

%type <with_user_opt_list> with_user_opt_list
%type <with_user_opt_item> with_user_opt_item
%type <role_type_clause> role_type_clause
%type <acl_item_clause> acl_item_clause

%type <sentence> go_sentence match_sentence use_sentence find_sentence
%type <sentence> create_tag_sentence create_edge_sentence
%type <sentence> alter_tag_sentence alter_edge_sentence
%type <sentence> describe_tag_sentence describe_edge_sentence
%type <sentence> drop_tag_sentence drop_edge_sentence
%type <sentence> traverse_sentence set_sentence piped_sentence assignment_sentence
%type <sentence> maintain_sentence insert_vertex_sentence insert_edge_sentence
%type <sentence> mutate_sentence update_vertex_sentence update_edge_sentence delete_vertex_sentence delete_edge_sentence
%type <sentence> show_sentence add_hosts_sentence remove_hosts_sentence create_space_sentence describe_space_sentence
%type <sentence> drop_space_sentence
%type <sentence> yield_sentence
%type <sentence> order_by_sentence
%type <sentence> create_user_sentence alter_user_sentence drop_user_sentence change_password_sentence
%type <sentence> grant_sentence revoke_sentence
%type <sentence> sentence
%type <sentences> sentences

%start sentences

%%

name_label
     : LABEL { $$ = $1; }
     | unreserved_keyword { $$ = $1; }
     ;

unreserved_keyword
     : KW_SPACE              { $$ = new std::string("space"); }
     | KW_HOSTS              { $$ = new std::string("hosts"); }
     | KW_SPACES             { $$ = new std::string("spaces"); }
     | KW_FIRSTNAME          { $$ = new std::string("firstname"); }
     | KW_LASTNAME           { $$ = new std::string("lastname"); }
     | KW_EMAIL              { $$ = new std::string("email"); }
     | KW_PHONE              { $$ = new std::string("phone"); }
     | KW_USER               { $$ = new std::string("user"); }
     | KW_USERS              { $$ = new std::string("users"); }
     | KW_PASSWORD           { $$ = new std::string("password"); }
     | KW_ROLE               { $$ = new std::string("role"); }
     | KW_ROLES              { $$ = new std::string("roles"); }
     | KW_GOD                { $$ = new std::string("god"); }
     | KW_ADMIN              { $$ = new std::string("admin"); }
     | KW_GUEST              { $$ = new std::string("guest"); }
     ;

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
    | src_ref_expression {
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
    | function_call_expression {
        $$ = $1;
    }
    ;

input_ref_expression
    : INPUT_REF DOT LABEL {
        $$ = new InputPropertyExpression($3);
    }
    | INPUT_REF {
        // To reference the `id' column implicitly
        $$ = new InputPropertyExpression(new std::string("id"));
    }
    ;

src_ref_expression
    : SRC_REF DOT LABEL DOT LABEL {
        $$ = new SourcePropertyExpression($3, $5);
    }
    ;

dst_ref_expression
    : DST_REF DOT LABEL DOT LABEL {
        $$ = new DestPropertyExpression($3, $5);
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
    ;

function_call_expression
    : name_label L_PAREN argument_list R_PAREN {
        $$ = new FunctionCallExpression($1, $3);
    }
    ;

argument_list
    : %empty {
        $$ = nullptr;
    }
    | expression {
        $$ = new ArgumentList();
        $$->addArgument($1);
    }
    | argument_list COMMA expression {
        $$ = $1;
        $$->addArgument($3);
    }
    ;

unary_expression
    : primary_expression { $$ = $1; }
    | PLUS primary_expression {
        $$ = new UnaryExpression(UnaryExpression::PLUS, $2);
    }
    | MINUS primary_expression {
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
    : unary_expression { $$ = $1; }
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
    : multiplicative_expression { $$ = $1; }
    | additive_expression PLUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::ADD, $3);
    }
    | additive_expression MINUS multiplicative_expression {
        $$ = new ArithmeticExpression($1, ArithmeticExpression::SUB, $3);
    }
    ;

relational_expression
    : additive_expression { $$ = $1; }
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
    : relational_expression { $$ = $1; }
    | equality_expression EQ relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::EQ, $3);
    }
    | equality_expression NE relational_expression {
        $$ = new RelationalExpression($1, RelationalExpression::NE, $3);
    }
    ;

logic_and_expression
    : equality_expression { $$ = $1; }
    | logic_and_expression AND equality_expression {
        $$ = new LogicalExpression($1, LogicalExpression::AND, $3);
    }
    ;

logic_or_expression
    : logic_and_expression { $$ = $1; }
    | logic_or_expression OR logic_and_expression {
        $$ = new LogicalExpression($1, LogicalExpression::OR, $3);
    }
    ;

expression
    : logic_or_expression { $$ = $1; }
    ;

go_sentence
    : KW_GO step_clause from_clause over_clause where_clause yield_clause {
        auto go = new GoSentence();
        go->setStepClause($2);
        go->setFromClause($3);
        go->setOverClause($4);
        go->setWhereClause($5);
        if ($6 == nullptr) {
            auto *edge = new std::string(*$4->edge());
            auto *expr = new EdgeDstIdExpression(edge);
            auto *alias = new std::string("id");
            auto *col = new YieldColumn(expr, alias);
            auto *cols = new YieldColumns();
            cols->addColumn(col);
            $6 = new YieldClause(cols);
        }
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
    : KW_FROM vid_list {
        $$ = new FromClause($2);
    }
    | KW_FROM vid_ref_expression {
        $$ = new FromClause($2);
    }
    ;

vid_list
    : vid {
        $$ = new VertexIDList();
        $$->add($1);
    }
    | vid_list COMMA vid {
        $$ = $1;
        $$->add($3);
    }
    ;

vid
    : unary_integer {
        $$ = new PrimaryExpression($1);
    }
    | function_call_expression {
        $$ = $1;
    }
    ;

unary_integer
    : PLUS INTEGER {
        $$ = $2;
    }
    | MINUS INTEGER {
        $$ = -$2;
    }
    | INTEGER {
        $$ = $1;
    }
    ;

vid_ref_expression
    : input_ref_expression {
        $$ = $1;
    }
    | var_ref_expression {
        $$ = $1;
    }
    ;

over_clause
    : KW_OVER name_label {
        $$ = new OverClause($2);
    }
    | KW_OVER name_label KW_REVERSELY {
        $$ = new OverClause($2, nullptr, true);
    }
    | KW_OVER name_label KW_AS name_label {
        $$ = new OverClause($2, $4);
    }
    | KW_OVER name_label KW_AS name_label KW_REVERSELY {
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
    | expression KW_AS name_label {
        $$ = new YieldColumn($1, $3);
    }
    ;

yield_sentence
    : KW_YIELD yield_columns {
        $$ = new YieldSentence($2);
    }
    ;

match_sentence
    : KW_MATCH { $$ = new MatchSentence; }
    ;

find_sentence
    : KW_FIND prop_list KW_FROM name_label where_clause {
        auto sentence = new FindSentence($4, $2);
        sentence->setWhereClause($5);
        $$ = sentence;
    }
    ;

use_sentence
    : KW_USE name_label { $$ = new UseSentence($2); }
    ;

create_schema_prop_list
    : %empty {
        $$ = nullptr;
    }
    | create_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | create_schema_prop_list COMMA create_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

 create_schema_prop_item
    : KW_TTL_DURATION ASSIGN unary_integer {
        // Less than or equal to 0 means infinity, so less than 0 is equivalent to 0
        if ($3 < 0) {
            $3 = 0;
        }
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN name_label {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_tag_sentence
    : KW_CREATE KW_TAG name_label L_PAREN R_PAREN create_schema_prop_list {
        if ($6 == nullptr) {
            $6 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, new ColumnSpecificationList(), $6);
    }
    | KW_CREATE KW_TAG name_label L_PAREN column_spec_list R_PAREN create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, $5, $7);
    }
    | KW_CREATE KW_TAG name_label L_PAREN column_spec_list COMMA R_PAREN create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateTagSentence($3, $5, $8);
    }
    ;

alter_tag_sentence
    : KW_ALTER KW_TAG name_label alter_schema_opt_list {
        $$ = new AlterTagSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_TAG name_label alter_schema_prop_list {
        $$ = new AlterTagSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_TAG name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterTagSentence($3, $4, $5);
    }
    ;

alter_schema_opt_list
    : alter_schema_opt_item {
        $$ = new AlterSchemaOptList();
        $$->addOpt($1);
    }
    | alter_schema_opt_list COMMA alter_schema_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_opt_item
    : KW_ADD L_PAREN column_spec_list R_PAREN {
        $$ = new AlterSchemaOptItem(AlterSchemaOptItem::ADD, $3);
    }
    | KW_CHANGE L_PAREN column_spec_list R_PAREN {
      $$ = new AlterSchemaOptItem(AlterSchemaOptItem::CHANGE, $3);
    }
    | KW_DROP L_PAREN column_spec_list R_PAREN {
      $$ = new AlterSchemaOptItem(AlterSchemaOptItem::DROP, $3);
    }
    ;

alter_schema_prop_list
    : alter_schema_prop_item {
        $$ = new SchemaPropList();
        $$->addOpt($1);
    }
    | alter_schema_prop_list COMMA alter_schema_prop_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

alter_schema_prop_item
    : KW_TTL_DURATION ASSIGN unary_integer {
        // Less than or equal to 0 means infinity, so less than 0 is equivalent to 0
        if ($3 < 0) {
            $3 = 0;
        }
        $$ = new SchemaPropItem(SchemaPropItem::TTL_DURATION, $3);
    }
    | KW_TTL_COL ASSIGN name_label {
        $$ = new SchemaPropItem(SchemaPropItem::TTL_COL, *$3);
        delete $3;
    }
    ;

create_edge_sentence
    : KW_CREATE KW_EDGE name_label L_PAREN R_PAREN create_schema_prop_list {
        if ($6 == nullptr) {
            $6 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3,  new ColumnSpecificationList(), $6);
    }
    | KW_CREATE KW_EDGE name_label L_PAREN column_spec_list R_PAREN create_schema_prop_list {
        if ($7 == nullptr) {
            $7 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3, $5, $7);
    }
    | KW_CREATE KW_EDGE name_label L_PAREN column_spec_list COMMA R_PAREN create_schema_prop_list {
        if ($8 == nullptr) {
            $8 = new SchemaPropList();
        }
        $$ = new CreateEdgeSentence($3, $5, $8);
    }
    ;

alter_edge_sentence
    : KW_ALTER KW_EDGE name_label alter_schema_opt_list {
        $$ = new AlterEdgeSentence($3, $4, new SchemaPropList());
    }
    | KW_ALTER KW_EDGE name_label alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, new AlterSchemaOptList(), $4);
    }
    | KW_ALTER KW_EDGE name_label alter_schema_opt_list alter_schema_prop_list {
        $$ = new AlterEdgeSentence($3, $4, $5);
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
    : name_label { $$ = new ColumnSpecification($1); }
    | name_label type_spec { $$ = new ColumnSpecification($2, $1); }
    ;

describe_tag_sentence
    : KW_DESCRIBE KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    | KW_DESC KW_TAG name_label {
        $$ = new DescribeTagSentence($3);
    }
    ;

describe_edge_sentence
    : KW_DESCRIBE KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    | KW_DESC KW_EDGE name_label {
        $$ = new DescribeEdgeSentence($3);
    }
    ;

drop_tag_sentence
    : KW_DROP KW_TAG name_label {
        $$ = new DropTagSentence($3);
    }
    ;

drop_edge_sentence
    : KW_DROP KW_EDGE name_label {
        $$ = new DropEdgeSentence($3);
    }
    ;

order_factor
    : input_ref_expression {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_ASC {
        $$ = new OrderFactor($1, OrderFactor::ASCEND);
    }
    | input_ref_expression KW_DESC {
        $$ = new OrderFactor($1, OrderFactor::DESCEND);
    }
    | LABEL {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_ASC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::ASCEND);
    }
    | LABEL KW_DESC {
        auto inputRef = new InputPropertyExpression($1);
        $$ = new OrderFactor(inputRef, OrderFactor::DESCEND);
    }
    ;

order_factors
    : order_factor {
        auto factors = new OrderFactors();
        factors->addFactor($1);
        $$ = factors;
    }
    | order_factors COMMA order_factor {
        $1->addFactor($3);
        $$ = $1;
    }
    ;

order_by_sentence
    : KW_ORDER KW_BY order_factors {
        $$ = new OrderBySentence($3);
    }
    ;

traverse_sentence
    : go_sentence { $$ = $1; }
    | match_sentence { $$ = $1; }
    | find_sentence { $$ = $1; }
    | order_by_sentence { $$ = $1; }
    ;

set_sentence
    : traverse_sentence { $$ = $1; }
    | set_sentence KW_UNION traverse_sentence { $$ = new SetSentence($1, SetSentence::UNION, $3); }
    | set_sentence KW_INTERSECT traverse_sentence { $$ = new SetSentence($1, SetSentence::INTERSECT, $3); }
    | set_sentence KW_MINUS traverse_sentence { $$ = new SetSentence($1, SetSentence::MINUS, $3); }
    | L_PAREN piped_sentence R_PAREN { $$ = $2; }
    ;

piped_sentence
    : set_sentence { $$ = $1; }
    | piped_sentence PIPE set_sentence { $$ = new PipedSentence($1, $3); }
    ;

assignment_sentence
    : VARIABLE ASSIGN piped_sentence {
        $$ = new AssignmentSentence($1, $3);
    }
    ;

insert_vertex_sentence
    : KW_INSERT KW_VERTEX vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($3, $5);
    }
    | KW_INSERT KW_VERTEX KW_NO KW_OVERWRITE vertex_tag_list KW_VALUES vertex_row_list {
        $$ = new InsertVertexSentence($5, $7, false /* not overwritable */);
    }
    ;

vertex_tag_list
    : vertex_tag_item {
        $$ = new VertexTagList();
        $$->addTagItem($1);
    }
    | vertex_tag_list COMMA vertex_tag_item {
        $$ = $1;
        $$->addTagItem($3);
    }
    ;

vertex_tag_item
    : name_label L_PAREN R_PAREN{
        $$ = new VertexTagItem($1);
    }
    | name_label L_PAREN prop_list R_PAREN {
        $$ = new VertexTagItem($1, $3);
    }
    ;

prop_list
    : name_label {
        $$ = new PropertyList();
        $$->addProp($1);
    }
    | prop_list COMMA name_label {
        $$ = $1;
        $$->addProp($3);
    }
    | prop_list COMMA {
        $$ = $1;
    }
    ;

vertex_row_list
    : vertex_row_item {
        $$ = new VertexRowList();
        $$->addRow($1);
    }
    | vertex_row_list COMMA vertex_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

vertex_row_item
    : vid COLON L_PAREN R_PAREN {
        $$ = new VertexRowItem($1, new ValueList());
    }
    | vid COLON L_PAREN value_list R_PAREN {
        $$ = new VertexRowItem($1, $4);
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
    : KW_INSERT KW_EDGE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps(new PropertyList());
        sentence->setRows($7);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setEdge($3);
        sentence->setProps($5);
        sentence->setRows($8);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps(new PropertyList());
        sentence->setRows($9);
        $$ = sentence;
    }
    | KW_INSERT KW_EDGE KW_NO KW_OVERWRITE name_label L_PAREN prop_list R_PAREN KW_VALUES edge_row_list {
        auto sentence = new InsertEdgeSentence();
        sentence->setOverwrite(false);
        sentence->setEdge($5);
        sentence->setProps($7);
        sentence->setRows($10);
        $$ = sentence;
    }
    ;

edge_row_list
    : edge_row_item {
        $$ = new EdgeRowList();
        $$->addRow($1);
    }
    | edge_row_list COMMA edge_row_item {
        $1->addRow($3);
        $$ = $1;
    }
    ;

edge_row_item
    : vid R_ARROW vid COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, new ValueList());
    }
    | vid R_ARROW vid COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $6);
    }
    | vid R_ARROW vid AT rank COLON L_PAREN R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, new ValueList());
    }
    | vid R_ARROW vid AT rank COLON L_PAREN value_list R_PAREN {
        $$ = new EdgeRowItem($1, $3, $5, $8);
    }
    ;

rank: unary_integer { $$ = $1; };

update_vertex_sentence
    : KW_UPDATE KW_VERTEX vid KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateVertexSentence();
        sentence->setVid($3);
        sentence->setUpdateList($5);
        sentence->setWhereClause($6);
        sentence->setYieldClause($7);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_VERTEX vid KW_SET update_list where_clause yield_clause {
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
    : name_label ASSIGN expression {
        $$ = new UpdateItem($1, $3);
    }
    ;

update_edge_sentence
    : KW_UPDATE KW_EDGE vid R_ARROW vid
      KW_SET update_list where_clause yield_clause {
        auto sentence = new UpdateEdgeSentence();
        sentence->setSrcId($3);
        sentence->setDstId($5);
        sentence->setUpdateList($7);
        sentence->setWhereClause($8);
        sentence->setYieldClause($9);
        $$ = sentence;
    }
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE vid R_ARROW vid
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
    | KW_UPDATE KW_EDGE vid R_ARROW vid AT rank
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
    | KW_UPDATE KW_OR KW_INSERT KW_EDGE vid R_ARROW vid AT rank KW_SET
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

delete_vertex_sentence
    : KW_DELETE KW_VERTEX vid_list where_clause {
        auto sentence = new DeleteVertexSentence($3);
        sentence->setWhereClause($4);
        $$ = sentence;
    }
    ;

edge_list
    : vid R_ARROW vid {
        $$ = new EdgeList();
        $$->addEdge($1, $3);
    }
    | edge_list COMMA vid R_ARROW vid {
        $$ = $1;
        $$->addEdge($3, $5);
    }
    ;

delete_edge_sentence
    : KW_DELETE KW_EDGE edge_list where_clause {
        auto sentence = new DeleteEdgeSentence($3);
        sentence->setWhereClause($4);
        $$ = sentence;
    }
    ;

show_sentence
    : KW_SHOW KW_HOSTS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowHosts);
    }
    | KW_SHOW KW_SPACES {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowSpaces);
    }
    | KW_SHOW KW_TAGS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowTags);
    }
    | KW_SHOW KW_EDGES {
         $$ = new ShowSentence(ShowSentence::ShowType::kShowEdges);
    }
    | KW_SHOW KW_USERS {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowUsers);
    }
    | KW_SHOW KW_USER name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowUser, $3);
    }
    | KW_SHOW KW_ROLES KW_IN name_label {
        $$ = new ShowSentence(ShowSentence::ShowType::kShowRoles, $4);
    }
    ;

add_hosts_sentence
    : KW_ADD KW_HOSTS host_list {
        auto sentence = new AddHostsSentence();
        sentence->setHosts($3);
        $$ = sentence;
    }
    ;

remove_hosts_sentence
    : KW_REMOVE KW_HOSTS host_list {
        auto sentence = new RemoveHostsSentence();
        sentence->setHosts($3);
        $$ = sentence;
    }
    ;

host_list
    : host_item {
        $$ = new HostList();
        $$->addHost($1);
    }
    | host_list COMMA host_item {
        $$ = $1;
        $$->addHost($3);
    }
    | host_list COMMA {
        $$ = $1;
    }
    ;

host_item
    : IPV4 COLON port {
        $$ = new nebula::HostAddr();
        $$->first = $1;
        $$->second = $3;
    }
    /* TODO(dutor) Support hostname and IPv6 */
    ;

port : INTEGER { $$ = $1; }

create_space_sentence
    : KW_CREATE KW_SPACE name_label {
        auto sentence = new CreateSpaceSentence($3);
        $$ = sentence;
    }
    | KW_CREATE KW_SPACE name_label L_PAREN space_opt_list R_PAREN {
        auto sentence = new CreateSpaceSentence($3);
        sentence->setOpts($5);
        $$ = sentence;
    }
    ;

describe_space_sentence
    : KW_DESCRIBE KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    | KW_DESC KW_SPACE name_label {
        $$ = new DescribeSpaceSentence($3);
    }
    ;

space_opt_list
    : space_opt_item {
        $$ = new SpaceOptList();
        $$->addOpt($1);
    }
    | space_opt_list COMMA space_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    | space_opt_list COMMA {
        $$ = $1;
    }
    ;

 space_opt_item
    : KW_PARTITION_NUM ASSIGN INTEGER {
        $$ = new SpaceOptItem(SpaceOptItem::PARTITION_NUM, $3);
    }
    | KW_REPLICA_FACTOR ASSIGN INTEGER {
        $$ = new SpaceOptItem(SpaceOptItem::REPLICA_FACTOR, $3);
    }
    // TODO(YT) Create Spaces for different engines
    // KW_ENGINE_TYPE ASSIGN name_label
    ;

drop_space_sentence
    : KW_DROP KW_SPACE name_label {
        $$ = new DropSpaceSentence($3);
    }
    ;

//  User manager sentences.

with_user_opt_item
    : KW_FIRSTNAME STRING {
        $$ = new WithUserOptItem(WithUserOptItem::FIRST, $2);
    }
    | KW_LASTNAME STRING {
        $$ = new WithUserOptItem(WithUserOptItem::LAST, $2);
    }
    | KW_EMAIL STRING {
        $$ = new WithUserOptItem(WithUserOptItem::EMAIL, $2);
    }
    | KW_PHONE STRING {
        $$ = new WithUserOptItem(WithUserOptItem::PHONE, $2);
    }
    ;

with_user_opt_list
    : with_user_opt_item {
        $$ = new WithUserOptList();
        $$->addOpt($1);
    }
    | with_user_opt_list COMMA with_user_opt_item {
        $$ = $1;
        $$->addOpt($3);
    }
    ;

create_user_sentence
    : KW_CREATE KW_USER name_label KW_WITH KW_PASSWORD STRING {
        $$ = new CreateUserSentence($3, $6);
    }
    | KW_CREATE KW_USER KW_IF KW_NOT KW_EXISTS name_label KW_WITH KW_PASSWORD STRING {
        auto sentence = new CreateUserSentence($6, $9);
        sentence->setMissingOk(true);
        $$ = sentence;
    }
    | KW_CREATE KW_USER name_label KW_WITH KW_PASSWORD STRING COMMA with_user_opt_list {
        auto sentence = new CreateUserSentence($3, $6);
        sentence->setOpts($8);
        $$ = sentence;
    }
    | KW_CREATE KW_USER KW_IF KW_NOT KW_EXISTS name_label KW_WITH KW_PASSWORD STRING COMMA with_user_opt_list {
        auto sentence = new CreateUserSentence($6, $9);
        sentence->setMissingOk(true);
        sentence->setOpts($11);
        $$ = sentence;
    }

alter_user_sentence
    : KW_ALTER KW_USER name_label KW_WITH with_user_opt_list {
        auto sentence = new AlterUserSentence($3);
        sentence->setOpts($5);
        $$ = sentence;
    }
    ;

drop_user_sentence
    : KW_DROP KW_USER name_label {
        $$ = new DropUserSentence($3);
    }
    | KW_DROP KW_USER KW_IF KW_EXISTS name_label {
        auto sentence = new DropUserSentence($5);
        sentence->setMissingOk(true);
        $$ = sentence;
    }
    ;

change_password_sentence
    : KW_CHANGE KW_PASSWORD name_label KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5);
        $$ = sentence;
    }
    | KW_CHANGE KW_PASSWORD name_label KW_FROM STRING KW_TO STRING {
        auto sentence = new ChangePasswordSentence($3, $5, $7);
        $$ = sentence;
    }
    ;

role_type_clause
    : KW_GOD { $$ = new RoleTypeClause(RoleTypeClause::GOD); }
    | KW_ADMIN { $$ = new RoleTypeClause(RoleTypeClause::ADMIN); }
    | KW_USER { $$ = new RoleTypeClause(RoleTypeClause::USER); }
    | KW_GUEST { $$ = new RoleTypeClause(RoleTypeClause::GUEST); }
    ;

acl_item_clause
    : KW_ROLE role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(true, $4);
        sentence->setRoleTypeClause($2);
        $$ = sentence;
    }
    | role_type_clause KW_ON name_label {
        auto sentence = new AclItemClause(false, $3);
        sentence->setRoleTypeClause($1);
        $$ = sentence;
    }
    ;

grant_sentence
    : KW_GRANT acl_item_clause KW_TO name_label {
        auto sentence = new GrantSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

revoke_sentence
    : KW_REVOKE acl_item_clause KW_FROM name_label {
        auto sentence = new RevokeSentence($4);
        sentence->setAclItemClause($2);
        $$ = sentence;
    }
    ;

mutate_sentence
    : insert_vertex_sentence { $$ = $1; }
    | insert_edge_sentence { $$ = $1; }
    | update_vertex_sentence { $$ = $1; }
    | update_edge_sentence { $$ = $1; }
    | delete_vertex_sentence { $$ = $1; }
    | delete_edge_sentence { $$ = $1; }
    ;

maintain_sentence
    : create_tag_sentence { $$ = $1; }
    | create_edge_sentence { $$ = $1; }
    | alter_tag_sentence { $$ = $1; }
    | alter_edge_sentence { $$ = $1; }
    | describe_tag_sentence { $$ = $1; }
    | describe_edge_sentence { $$ = $1; }
    | drop_tag_sentence { $$ = $1; }
    | drop_edge_sentence { $$ = $1; }
    | show_sentence { $$ = $1; }
    | add_hosts_sentence { $$ = $1; }
    | remove_hosts_sentence { $$ = $1; }
    | create_space_sentence { $$ = $1; }
    | describe_space_sentence { $$ = $1; }
    | drop_space_sentence { $$ = $1; }
    | yield_sentence {
        // Now we take YIELD as a normal maintenance sentence.
        // In the future, we might make it able to be used in pipe.
        $$ = $1;
    }
    ;
    | create_user_sentence { $$ = $1; }
    | alter_user_sentence { $$ = $1; }
    | drop_user_sentence { $$ = $1; }
    | change_password_sentence { $$ = $1; }
    | grant_sentence { $$ = $1; }
    | revoke_sentence { $$ = $1; }
    ;

sentence
    : maintain_sentence { $$ = $1; }
    | use_sentence { $$ = $1; }
    | piped_sentence { $$ = $1; }
    | assignment_sentence { $$ = $1; }
    | mutate_sentence { $$ = $1; }
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
